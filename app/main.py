"""
Employee Attendance & Analytics API (FastAPI + MongoDB, PyMongo async driver)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (required; a local .env is loaded, real environment variables win)
      LOG_LEVEL (optional, default INFO)

Contract: openapi.yaml (paths, status codes, business rules R1-R10); storage: DATA_MODEL.md.

Section map
    1.  Imports
    2.  Configuration
    3.  Logging
    4.  Constants & enums
    5.  Domain exceptions
    6.  Schemas (validated types, request models, response models)
    7.  Pure helpers (time conversion, business rules R1-R8)
    8.  Serializers (MongoDB document -> API shape)
    9.  Query / pipeline builders (shared by the endpoints and /admin/explain)
    10. Services (business logic + database access)
    11. Database lifecycle (indexes, lifespan)
    12. App instance & middleware
    13. Exception handlers
    14. Dependencies
    15. Routers & endpoints
"""

# =========================================================================== #
# 1. Imports
# =========================================================================== #
import json
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from typing import Annotated, Any, AsyncIterator, Literal, Optional

import asyncio
from bson import Decimal128, json_util
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, FastAPI, Path, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StringConstraints,
    model_validator,
)
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, IndexModel, ReturnDocument
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import ConnectionFailure, DuplicateKeyError, OperationFailure, PyMongoError
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send


# =========================================================================== #
# 2. Configuration
# =========================================================================== #
# override=False: real environment variables always take priority over .env.
load_dotenv(override=False)


@dataclass(frozen=True)
class Settings:
    mongo_uri: str
    mongo_db: str
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        missing = [name for name in ("MONGO_URI", "MONGO_DB") if not os.environ.get(name)]
        if missing:
            raise RuntimeError(f"Missing required environment variable(s): {', '.join(missing)}")
        return cls(
            mongo_uri=os.environ["MONGO_URI"],
            mongo_db=os.environ["MONGO_DB"],
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        )


# =========================================================================== #
# 3. Logging
# =========================================================================== #
logger = logging.getLogger("attendance")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    logger.addHandler(_handler)
    logger.propagate = False
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())


# =========================================================================== #
# 4. Constants & enums
# =========================================================================== #
UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

MIN_EPOCH_MS = 100_000_000_000  # below this the value looks like seconds -> 422
MAX_EPOCH_MS = 4_102_444_800_000  # 2100-01-01T00:00:00Z

GRACE_SECONDS = 10 * 60  # R2
OVERTIME_MIN_MINUTES = 30  # R3
HALF_DAY_THRESHOLD = 4.50  # R5
MAX_PUNCH_SPAN = timedelta(hours=24)

DEFAULT_PAGE_SIZE = 20  # R10
MAX_PAGE_SIZE = 100  # R10
DEFAULT_LEADERBOARD_LIMIT = 10
MAX_LEADERBOARD_LIMIT = 50
MAX_TREND_DAYS = 92

EMPLOYEES = "employees"
ATTENDANCE = "attendance_logs"
DATE_FMT = "%Y-%m-%d"

DATE_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
HHMM_PATTERN = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"
EMP_CODE_PATTERN = r"^EMP[0-9]{4,6}$"
EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


class Status(str, Enum):
    PRESENT = "PRESENT"
    ABSENT = "ABSENT"
    LEAVE = "LEAVE"
    WFH = "WFH"
    ON_DUTY = "ON_DUTY"


class PresenceStatus(str, Enum):
    PRESENT = "PRESENT"
    WFH = "WFH"
    ON_DUTY = "ON_DUTY"


class ExplainTarget(str, Enum):
    ATTENDANCE_LIST = "attendance_list"
    EMPLOYEE_MONTHLY = "employee_monthly"
    DEPARTMENT_SUMMARY = "department_summary"
    LATE_LEADERBOARD = "late_leaderboard"
    DEPARTMENT_TREND = "department_trend"


PRESENCE_STATUSES = ("PRESENT", "WFH", "ON_DUTY")  # R6
ABSENCE_STATUSES = ("ABSENT", "LEAVE")
TRACKED_FIELDS = (
    "status",
    "punch_in",
    "punch_out",
    "work_hours",
    "late_minutes",
    "overtime_minutes",
    "half_day",
)
ATTENDANCE_LIST_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


# =========================================================================== #
# 5. Domain exceptions (translated to HTTP responses in section 13)
# =========================================================================== #
class AppError(Exception):
    """Base class for errors whose body is {"detail": "<message>"}."""

    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class NotFoundError(AppError):
    status_code = status.HTTP_404_NOT_FOUND


class ConflictError(AppError):
    status_code = status.HTTP_409_CONFLICT


class ServiceUnavailableError(AppError):
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE


class BusinessRuleError(Exception):
    """A semantic validation failure -> 422 with FastAPI's array-shaped `detail`."""

    def __init__(self, loc: tuple, msg: str, error_type: str = "value_error") -> None:
        super().__init__(msg)
        self.loc = loc
        self.msg = msg
        self.error_type = error_type


# =========================================================================== #
# 6. Schemas
# =========================================================================== #
# ---- 6a. Reusable validated types ------------------------------------------ #
def _validate_date_str(value: str) -> str:
    if not re.fullmatch(DATE_PATTERN, value):
        raise ValueError("must be a date in YYYY-MM-DD format")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("is not a valid calendar date") from exc
    return value


DateStr = Annotated[str, AfterValidator(_validate_date_str)]
MonthStr = Annotated[str, StringConstraints(pattern=MONTH_PATTERN)]
HHMM = Annotated[str, StringConstraints(pattern=HHMM_PATTERN)]
EmpCode = Annotated[str, StringConstraints(pattern=EMP_CODE_PATTERN)]
# StrictInt rejects strings, floats and booleans; the bounds reject seconds-looking values.
EpochMillis = Annotated[StrictInt, Field(ge=MIN_EPOCH_MS, le=MAX_EPOCH_MS)]


@dataclass(frozen=True)
class Pagination:
    page: int
    page_size: int

    @property
    def skip(self) -> int:
        return (self.page - 1) * self.page_size


# ---- 6b. Request models ---------------------------------------------------- #
class EmployeeCreate(BaseModel):
    model_config = ConfigDict(extra="ignore")

    emp_code: EmpCode
    name: Annotated[str, StringConstraints(min_length=1, max_length=100)]
    email: Annotated[str, StringConstraints(max_length=120, pattern=EMAIL_PATTERN)]
    department: Annotated[str, StringConstraints(min_length=1, max_length=50)]
    shift_start: HHMM = "09:30"
    shift_end: HHMM = "18:30"
    joined_on: DateStr

    @model_validator(mode="after")
    def _shift_times_must_differ(self) -> "EmployeeCreate":
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    emp_code: str
    punched_at: Optional[EpochMillis] = None
    status: PresenceStatus = PresenceStatus.PRESENT


class PunchOutRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    emp_code: str
    punched_at: Optional[EpochMillis] = None


class RegularizeRequest(BaseModel):
    # Derived fields (work_hours, late_minutes, ...) are silently ignored per the contract.
    model_config = ConfigDict(extra="ignore")

    status: Optional[Status] = None
    punch_in: Optional[EpochMillis] = None
    punch_out: Optional[EpochMillis] = None
    reason: Annotated[str, StringConstraints(min_length=5, max_length=200)]
    regularized_by: Annotated[str, StringConstraints(min_length=1, max_length=50)]


# ---- 6c. Response models --------------------------------------------------- #
class HealthOut(BaseModel):
    status: Literal["ok"]


class EmployeeOut(BaseModel):
    emp_code: str
    name: str
    email: str
    department: str
    shift_start: str
    shift_end: str
    joined_on: str
    created_at: Optional[int]


class EmployeePage(BaseModel):
    items: list[EmployeeOut]
    total: int
    page: int
    page_size: int


class HistoryEntryOut(BaseModel):
    at: Optional[int]
    by: str
    reason: str
    changes: dict[str, Any]


class AttendanceRecordOut(BaseModel):
    emp_code: str
    date: str
    status: str
    punch_in: Optional[int]
    punch_out: Optional[int]
    work_hours: Optional[float]
    late_minutes: int
    overtime_minutes: int
    half_day: bool
    history: list[HistoryEntryOut]


class AttendancePage(BaseModel):
    items: list[AttendanceRecordOut]
    total: int
    page: int
    page_size: int


class EmployeeMonthlyOut(BaseModel):
    emp_code: str
    month: str
    working_days: int
    present_days: float
    leave_days: int
    late_count: int
    total_late_minutes: int
    total_overtime_minutes: int
    attendance_pct: Optional[float]


class DepartmentSummaryItem(BaseModel):
    department: str
    headcount: int
    present_days: float
    avg_work_hours: Optional[float]
    late_count: int
    total_late_minutes: int
    leave_count: int
    on_duty_count: int


class DepartmentSummaryOut(BaseModel):
    month: str
    items: list[DepartmentSummaryItem]


class LeaderboardItem(BaseModel):
    rank: int
    emp_code: str
    name: str
    department: str
    total_late_minutes: int
    late_count: int


class LeaderboardOut(BaseModel):
    month: str
    items: list[LeaderboardItem]


class TrendItem(BaseModel):
    date: str
    is_working_day: bool
    headcount: int
    present_count: float
    late_count: int
    attendance_rate: Optional[float]
    moving_avg_7d: Optional[float]


class TrendOut(BaseModel):
    department: str
    items: list[TrendItem]


class ExplainOut(BaseModel):
    endpoint: str
    collection: str
    explain: dict[str, Any]


# =========================================================================== #
# 7. Pure helpers (no I/O)
# =========================================================================== #
# ---- 7a. Instants ---------------------------------------------------------- #
def ensure_utc(value: datetime) -> datetime:
    """PyMongo is configured tz-aware; this also guards against any naive datetime."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def stored_instant(value: Optional[datetime]) -> Optional[datetime]:
    """A stored BSON date as an aware UTC datetime truncated to whole seconds (R1)."""
    if value is None:
        return None
    return ensure_utc(value).replace(microsecond=0)


def ms_to_dt(millis: int) -> datetime:
    """Epoch milliseconds -> aware UTC datetime, truncated to whole seconds (R1)."""
    return datetime.fromtimestamp(millis // 1000, tz=UTC)


def dt_to_ms(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    return (ensure_utc(value) - EPOCH) // timedelta(milliseconds=1)


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


# ---- 7b. Shift & attendance date (R1) ------------------------------------- #
def parse_hhmm(value: str) -> dtime:
    hours, minutes = map(int, value.split(":"))
    return dtime(hours, minutes)


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return shift_end <= shift_start  # zero-padded HH:MM compares correctly as text


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> date:
    """R1: the IST calendar date of punch_in; an overnight shift's early-morning punch
    (IST time earlier than shift_end) belongs to the shift that started the day before."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if is_overnight(shift_start, shift_end) and local.time() < parse_hhmm(shift_end):
        day -= timedelta(days=1)
    return day


def shift_start_at(att_date: date, shift_start: str) -> datetime:
    return datetime.combine(att_date, parse_hhmm(shift_start), tzinfo=IST)


def shift_end_at(att_date: date, shift_start: str, shift_end: str) -> datetime:
    end = datetime.combine(att_date, parse_hhmm(shift_end), tzinfo=IST)
    if is_overnight(shift_start, shift_end):
        end += timedelta(days=1)
    return end


# ---- 7c. Derived fields (R2 - R5) ----------------------------------------- #
def compute_late_minutes(punch_in: datetime, att_date: date, shift_start: str) -> int:
    """R2: late only when strictly more than 10:00 after shift start; minutes counted from shift start."""
    elapsed = (punch_in - shift_start_at(att_date, shift_start)) // timedelta(seconds=1)
    return elapsed // 60 if elapsed > GRACE_SECONDS else 0


def compute_overtime_minutes(
    punch_out: datetime, att_date: date, shift_start: str, shift_end: str
) -> int:
    """R3: whole minutes after shift end, only when at least 30."""
    elapsed = (punch_out - shift_end_at(att_date, shift_start, shift_end)) // timedelta(seconds=1)
    minutes = elapsed // 60
    return minutes if minutes >= OVERTIME_MIN_MINUTES else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600 rounded half-up to 2 decimals (exact Decimal arithmetic)."""
    seconds = (punch_out - punch_in) // timedelta(seconds=1)
    hours = (Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(hours)


def is_half_day(work_hours: float) -> bool:
    """R5: uses the already rounded work_hours."""
    return work_hours < HALF_DAY_THRESHOLD


def derive_fields(
    status_value: str,
    punch_in: Optional[datetime],
    punch_out: Optional[datetime],
    att_date: date,
    employee: dict,
) -> dict:
    """Derived fields for a record, per the table in DATA_MODEL.md section 3."""
    if status_value not in PRESENCE_STATUSES or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = compute_late_minutes(punch_in, att_date, employee["shift_start"])
    if punch_out is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    hours = compute_work_hours(punch_in, punch_out)
    return {
        "work_hours": hours,
        "late_minutes": late,
        "overtime_minutes": compute_overtime_minutes(
            punch_out, att_date, employee["shift_start"], employee["shift_end"]
        ),
        "half_day": is_half_day(hours),
    }


def validate_punch_out(punch_in: datetime, punch_out: datetime, loc: tuple) -> None:
    if punch_out <= punch_in:
        raise BusinessRuleError(loc, "punch_out must be after punch_in")
    if punch_out - punch_in > MAX_PUNCH_SPAN:
        raise BusinessRuleError(loc, "punch_out must be within 24 hours of punch_in")


# ---- 7d. Reporting numbers & calendar (R7, R8) ---------------------------- #
def half_up(value: float, places: int) -> float:
    """R8 half-up rounding; str() avoids binary-float artefacts (2.675 -> 2.68)."""
    quantum = Decimal(1).scaleb(-places)
    return float(Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP))


def month_bounds(month: str) -> tuple[date, date]:
    year, mon = map(int, month.split("-"))
    first = date(year, mon, 1)
    next_first = date(year + 1, 1, 1) if mon == 12 else date(year, mon + 1, 1)
    return first, next_first - timedelta(days=1)


def count_working_days(start: date, end: date) -> int:
    """R7: Mon-Fri between start and end inclusive (calendar math, not document iteration)."""
    if start > end:
        return 0
    total_days = (end - start).days + 1
    full_weeks, remainder = divmod(total_days, 7)
    extra = sum(1 for i in range(remainder) if (start.weekday() + i) % 7 < 5)
    return full_weeks * 5 + extra


def validate_trend_range(date_from: str, date_to: str) -> None:
    start, end = date.fromisoformat(date_from), date.fromisoformat(date_to)
    if end < start:
        raise BusinessRuleError(("query", "to"), "'to' must be on or after 'from'")
    if (end - start).days + 1 > MAX_TREND_DAYS:
        raise BusinessRuleError(("query", "to"), f"range must not exceed {MAX_TREND_DAYS} days")


# =========================================================================== #
# 8. Serializers (MongoDB document -> API shape; `_id` never leaves the service)
# =========================================================================== #
def _instant_or_value(value: Any) -> Any:
    return dt_to_ms(value) if isinstance(value, datetime) else value


def serialize_employee(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start"),
        "shift_end": doc.get("shift_end"),
        "joined_on": doc.get("joined_on"),
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def serialize_history_entry(entry: dict) -> dict:
    changes = {
        field: {
            "from": _instant_or_value(change.get("from")),
            "to": _instant_or_value(change.get("to")),
        }
        for field, change in (entry.get("changes") or {}).items()
        if isinstance(change, dict)
    }
    return {
        "at": dt_to_ms(entry.get("at")),
        "by": entry.get("by") or "",
        "reason": entry.get("reason") or "",
        "changes": changes,
    }


def serialize_record(doc: dict) -> dict:
    """Missing history / half_day / minutes are treated as [] / false / 0 (DATA_MODEL.md)."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc.get("status"),
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day") or False),
        "history": [serialize_history_entry(h) for h in (doc.get("history") or [])],
    }


# =========================================================================== #
# 9. Query / pipeline builders
#    Each endpoint and /admin/explain call the SAME builder, so the explained
#    plan is exactly the plan the endpoint runs.
# =========================================================================== #
# ---- 9a. Aggregation expression helpers ----------------------------------- #
def _mongo_half_up(expr: Any, places: int) -> dict:
    """Half-up rounding inside MongoDB ($round is banker's rounding). Null-safe.
    $toDecimal converts doubles with 15 significant digits, removing float noise."""
    factor = 10**places
    scaled = {"$multiply": [{"$toDecimal": expr}, factor]}
    return {"$toDouble": {"$divide": [{"$floor": {"$add": [scaled, 0.5]}}, factor]}}


def _is_weekday_expr(date_str_expr: str) -> dict:
    day = {"$dateFromString": {"dateString": date_str_expr, "format": DATE_FMT, "onError": None, "onNull": None}}
    return {"$in": [{"$isoDayOfWeek": day}, [1, 2, 3, 4, 5]]}


def _is_present_expr(status_expr: str) -> dict:
    return {"$in": [status_expr, list(PRESENCE_STATUSES)]}


def _present_value_expr(status_expr: str, half_day_expr: str) -> dict:
    """R5/R6: a present record counts 1, a half day 0.5, anything else 0."""
    half = {"$eq": [{"$ifNull": [half_day_expr, False]}, True]}
    return {"$cond": [_is_present_expr(status_expr), {"$cond": [half, 0.5, 1]}, 0]}


def _is_late_expr(late_expr: str) -> dict:
    return {"$gt": [{"$ifNull": [late_expr, 0]}, 0]}


# ---- 9b. Builders ---------------------------------------------------------- #
def build_attendance_list_query(
    emp_code: Optional[str],
    date_from: Optional[str],
    date_to: Optional[str],
    status_filter: Optional[Status],
) -> dict:
    if date_from and date_to and date_from > date_to:
        raise BusinessRuleError(("query", "date_from"), "date_from must be on or before date_to")
    query: dict[str, Any] = {}
    if emp_code is not None:
        query["emp_code"] = emp_code
    if date_from or date_to:
        query["date"] = {}
        if date_from:
            query["date"]["$gte"] = date_from
        if date_to:
            query["date"]["$lte"] = date_to
    if status_filter is not None:
        query["status"] = status_filter.value
    return query


def build_employee_monthly_pipeline(emp_code: str, month: str) -> list[dict]:
    start, end = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start.isoformat(), "$lte": end.isoformat()}}},
        {
            "$group": {
                "_id": None,
                # R7: present_days counts Mon-Fri records only; half day = 0.5 (R5).
                "present_days": {
                    "$sum": {
                        "$cond": [
                            _is_weekday_expr("$date"),
                            _present_value_expr("$status", "$half_day"),
                            0,
                        ]
                    }
                },
                "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "late_count": {"$sum": {"$cond": [_is_late_expr("$late_minutes"), 1, 0]}},
                # Weekend records still contribute to late / overtime totals (R7).
                "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
                "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            }
        },
    ]


def build_department_summary_pipeline(month: str, department: Optional[str]) -> list[dict]:
    start, end = month_bounds(month)
    s, e = start.isoformat(), end.isoformat()
    # R9: rooted at employees so people with zero logs still count; future joiners excluded.
    employee_match: dict[str, Any] = {"joined_on": {"$lte": e}}
    employee_match["department"] = department if department is not None else {"$type": "string"}

    def over_logs(in_expr: Any) -> dict:
        return {"$sum": {"$map": {"input": "$logs", "as": "l", "in": in_expr}}}

    has_hours = {"$and": [_is_present_expr("$$l.status"), {"$isNumber": "$$l.work_hours"}]}
    is_late = _is_late_expr("$$l.late_minutes")
    return [
        {"$match": employee_match},
        {"$project": {"_id": 0, "emp_code": 1, "department": 1}},
        {
            "$lookup": {
                "from": ATTENDANCE,
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": s, "$lte": e}}},
                    {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1, "work_hours": 1}},
                ],
                "as": "logs",
            }
        },
        {
            "$project": {
                "department": 1,
                "present_days": over_logs(
                    {"$cond": [_is_weekday_expr("$$l.date"), _present_value_expr("$$l.status", "$$l.half_day"), 0]}
                ),
                "hours_sum": over_logs({"$cond": [has_hours, {"$toDecimal": "$$l.work_hours"}, Decimal128("0")]}),
                "hours_count": over_logs({"$cond": [has_hours, 1, 0]}),
                "late_count": over_logs({"$cond": [is_late, 1, 0]}),
                "total_late_minutes": over_logs({"$cond": [is_late, "$$l.late_minutes", 0]}),
                "leave_count": over_logs({"$cond": [{"$eq": ["$$l.status", "LEAVE"]}, 1, 0]}),
                "on_duty_count": over_logs({"$cond": [{"$eq": ["$$l.status", "ON_DUTY"]}, 1, 0]}),
            }
        },
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_days": {"$sum": "$present_days"},
                "hours_sum": {"$sum": "$hours_sum"},
                "hours_count": {"$sum": "$hours_count"},
                "late_count": {"$sum": "$late_count"},
                "total_late_minutes": {"$sum": "$total_late_minutes"},
                "leave_count": {"$sum": "$leave_count"},
                "on_duty_count": {"$sum": "$on_duty_count"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": 1,
                "present_days": _mongo_half_up("$present_days", 2),
                # Mean over records (not an average of per-employee averages).
                "avg_work_hours": {
                    "$cond": [
                        {"$gt": ["$hours_count", 0]},
                        _mongo_half_up({"$divide": ["$hours_sum", "$hours_count"]}, 2),
                        None,
                    ]
                },
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1,
            }
        },
        {"$sort": {"department": 1}},
    ]


def build_late_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> list[dict]:
    start, end = month_bounds(month)
    pipeline: list[dict] = [
        {"$match": {"date": {"$gte": start.isoformat(), "$lte": end.isoformat()}, "late_minutes": {"$gt": 0}}},
        {
            "$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": "$late_minutes"},
                "late_count": {"$sum": 1},
            }
        },
        # Inner-join semantics: logs whose employee does not exist are dropped.
        {"$lookup": {"from": EMPLOYEES, "localField": "_id", "foreignField": "emp_code", "as": "employee"}},
        {"$unwind": "$employee"},
    ]
    if department is not None:
        pipeline.append({"$match": {"employee.department": department}})
    pipeline += [
        # Standard competition ranking (1, 2, 2, 4); limit is applied AFTER ranking.
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$employee.name",
                "department": "$employee.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ]
    return pipeline


def build_department_trend_pipeline(department: str, date_from: str, date_to: str) -> list[dict]:
    range_start = datetime.fromisoformat(date_from).replace(tzinfo=UTC)
    range_end_exclusive = datetime.fromisoformat(date_to).replace(tzinfo=UTC) + timedelta(days=1)

    log_event = {
        "date": "$$l.date",
        "present": _present_value_expr("$$l.status", "$$l.half_day"),
        "late": {"$cond": [_is_late_expr("$$l.late_minutes"), 1, 0]},
        "joins": 0,
    }
    # Every employee emits one "join" event: +1 headcount from max(joined_on, from) onwards (R9).
    # Employees joining after `to` emit a neutral event so the pipeline is never empty.
    join_event = {
        "$cond": [
            {"$lte": ["$joined_on", date_to]},
            {"date": {"$max": ["$joined_on", date_from]}, "present": 0, "late": 0, "joins": 1},
            {"date": date_from, "present": 0, "late": 0, "joins": 0},
        ]
    }
    return [
        {"$match": {"department": department}},
        {"$project": {"_id": 0, "emp_code": 1, "joined_on": 1}},
        {
            "$lookup": {
                "from": ATTENDANCE,
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": date_from, "$lte": date_to}}},
                    {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1}},
                ],
                "as": "logs",
            }
        },
        {"$project": {"events": {"$concatArrays": [{"$map": {"input": "$logs", "as": "l", "in": log_event}}, [join_event]]}}},
        {"$unwind": "$events"},
        {
            "$group": {
                "_id": "$events.date",
                "present_count": {"$sum": "$events.present"},
                "late_count": {"$sum": "$events.late"},
                "joins": {"$sum": "$events.joins"},
            }
        },
        {"$set": {"day": {"$dateFromString": {"dateString": "$_id", "format": DATE_FMT, "timezone": "UTC"}}}},
        # Gap filling: exactly one document per calendar day in [from, to].
        {"$densify": {"field": "day", "range": {"step": 1, "unit": "day", "bounds": [range_start, range_end_exclusive]}}},
        {
            "$setWindowFields": {
                "sortBy": {"day": 1},
                "output": {"headcount": {"$sum": {"$ifNull": ["$joins", 0]}, "window": {"documents": ["unbounded", "current"]}}},
            }
        },
        {
            "$set": {
                "present_count": {"$ifNull": ["$present_count", 0]},
                "late_count": {"$ifNull": ["$late_count", 0]},
                "is_working_day": {"$in": [{"$isoDayOfWeek": "$day"}, [1, 2, 3, 4, 5]]},
            }
        },
        {
            "$set": {
                "attendance_rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        _mongo_half_up({"$divide": ["$present_count", "$headcount"]}, 4),
                        None,
                    ]
                }
            }
        },
        # $avg ignores nulls; the window never reaches before `from`, so early rows are shorter.
        {
            "$setWindowFields": {
                "sortBy": {"day": 1},
                "output": {"moving_avg_7d": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": {"$dateToString": {"date": "$day", "format": DATE_FMT, "timezone": "UTC"}},
                "is_working_day": 1,
                "headcount": 1,
                "present_count": 1,
                "late_count": 1,
                "attendance_rate": 1,
                "moving_avg_7d": _mongo_half_up("$moving_avg_7d", 4),
            }
        },
        {"$sort": {"date": 1}},
    ]


# =========================================================================== #
# 10. Services (business logic + database access)
# =========================================================================== #
async def run_aggregate(db: AsyncDatabase, collection: str, pipeline: list[dict]) -> list[dict]:
    cursor = await db[collection].aggregate(pipeline)
    return await cursor.to_list(None)


async def get_employee_or_404(db: AsyncDatabase, emp_code: str) -> dict:
    employee = await db[EMPLOYEES].find_one({"emp_code": emp_code}, {"_id": 0})
    if employee is None:
        raise NotFoundError(f"Employee {emp_code} not found")
    return employee


# ---- 10a. Employees -------------------------------------------------------- #
async def create_employee(db: AsyncDatabase, body: EmployeeCreate) -> dict:
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        # Uniqueness is enforced by the unique index, not by check-then-insert.
        await db[EMPLOYEES].insert_one(doc)
    except DuplicateKeyError as exc:
        raise ConflictError(f"Employee {body.emp_code} already exists") from exc
    return serialize_employee(doc)


async def list_employees(db: AsyncDatabase, department: Optional[str], paging: Pagination) -> dict:
    query = {"department": department} if department is not None else {}
    cursor = db[EMPLOYEES].find(query, {"_id": 0}).sort("emp_code", ASCENDING).skip(paging.skip).limit(paging.page_size)
    count = db[EMPLOYEES].count_documents(query)
    total, docs = await asyncio.gather(count, cursor.to_list(None))
    return {
        "items": [serialize_employee(d) for d in docs],
        "total": total,
        "page": paging.page,
        "page_size": paging.page_size,
    }


# ---- 10b. Attendance -------------------------------------------------------- #
async def punch_in(db: AsyncDatabase, body: PunchInRequest) -> dict:
    employee = await get_employee_or_404(db, body.emp_code)
    punched_at = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    att_date = attendance_date(punched_at, employee["shift_start"], employee["shift_end"])
    doc = {
        "emp_code": body.emp_code,
        "date": att_date.isoformat(),
        "status": body.status.value,
        "punch_in": punched_at,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": compute_late_minutes(punched_at, att_date, employee["shift_start"]),
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }
    try:
        # The unique (emp_code, date) index makes concurrent punch-ins race-safe.
        await db[ATTENDANCE].insert_one(doc)
    except DuplicateKeyError as exc:
        raise ConflictError(f"{body.emp_code} already has a record for {doc['date']}") from exc
    return serialize_record(doc)


async def punch_out(db: AsyncDatabase, body: PunchOutRequest) -> dict:
    employee = await get_employee_or_404(db, body.emp_code)
    punched_at = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    record = await db[ATTENDANCE].find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": punched_at}},
        sort=[("punch_in", DESCENDING)],
    )
    if record is None:
        raise NotFoundError(f"No punch-in found for {body.emp_code} at or before punched_at")
    if record.get("punch_out") is not None:
        raise ConflictError(f"Record {body.emp_code}/{record['date']} is already punched out")

    punch_in_at = stored_instant(record["punch_in"])
    validate_punch_out(punch_in_at, punched_at, ("body", "punched_at"))
    derived = derive_fields(record["status"], punch_in_at, punched_at, date.fromisoformat(record["date"]), employee)

    # Atomic conditional update: only one of several concurrent punch-outs can match.
    updated = await db[ATTENDANCE].find_one_and_update(
        {"_id": record["_id"], "punch_out": None},
        {
            "$set": {
                "punch_out": punched_at,
                "work_hours": derived["work_hours"],
                "overtime_minutes": derived["overtime_minutes"],
                "half_day": derived["half_day"],
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise ConflictError(f"Record {body.emp_code}/{record['date']} is already punched out")
    return serialize_record(updated)


async def list_attendance(db: AsyncDatabase, query: dict, paging: Pagination) -> dict:
    cursor = (
        db[ATTENDANCE]
        .find(query, {"_id": 0})
        .sort(ATTENDANCE_LIST_SORT)
        .skip(paging.skip)
        .limit(paging.page_size)
    )
    count = db[ATTENDANCE].count_documents(query)
    total, docs = await asyncio.gather(count, cursor.to_list(None))
    return {
        "items": [serialize_record(d) for d in docs],
        "total": total,
        "page": paging.page,
        "page_size": paging.page_size,
    }


def _tracked_state(record: dict) -> dict:
    return {
        "status": record.get("status"),
        "punch_in": stored_instant(record.get("punch_in")),
        "punch_out": stored_instant(record.get("punch_out")),
        "work_hours": record.get("work_hours"),
        "late_minutes": record.get("late_minutes") or 0,
        "overtime_minutes": record.get("overtime_minutes") or 0,
        "half_day": bool(record.get("half_day") or False),
    }


def _diff_states(old: dict, new: dict) -> dict:
    return {
        field: {"from": old[field], "to": new[field]}
        for field in TRACKED_FIELDS
        if old[field] != new[field]
    }


async def regularize_attendance(
    db: AsyncDatabase, emp_code: str, record_date: str, body: RegularizeRequest
) -> dict:
    employee = await get_employee_or_404(db, emp_code)
    record = await db[ATTENDANCE].find_one({"emp_code": emp_code, "date": record_date})
    if record is None:
        raise NotFoundError(f"No attendance record for {emp_code} on {record_date}")
    att_date = date.fromisoformat(record_date)

    new_status = body.status.value if body.status is not None else record.get("status")
    if new_status in ABSENCE_STATUSES:
        # ABSENT / LEAVE clear both punch times; sending one alongside is contradictory.
        for field in ("punch_in", "punch_out"):
            if getattr(body, field) is not None:
                raise BusinessRuleError(("body", field), f"{field} cannot be set when status is {new_status}")
        new_in = new_out = None
    else:
        new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else stored_instant(record.get("punch_in"))
        new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else stored_instant(record.get("punch_out"))
        if new_in is None:
            raise BusinessRuleError(("body", "punch_in"), f"status {new_status} requires a punch_in")
        if attendance_date(new_in, employee["shift_start"], employee["shift_end"]) != att_date:
            raise BusinessRuleError(("body", "punch_in"), f"punch_in must fall on the attendance date {record_date}")
        if new_out is not None:
            validate_punch_out(new_in, new_out, ("body", "punch_out"))

    new_state = {
        "status": new_status,
        "punch_in": new_in,
        "punch_out": new_out,
        **derive_fields(new_status, new_in, new_out, att_date, employee),
    }
    changes = _diff_states(_tracked_state(record), new_state)
    if not changes:
        raise BusinessRuleError(("body",), "request does not change the record")

    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
    # Optimistic concurrency: the update only applies if nobody changed the record since we read it.
    guard = {
        "_id": record["_id"],
        "status": record.get("status"),
        "punch_in": record.get("punch_in"),
        "punch_out": record.get("punch_out"),
        "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(record.get("history") or [])]},
    }
    updated = await db[ATTENDANCE].find_one_and_update(
        guard,
        {"$set": new_state, "$push": {"history": entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise ConflictError("Record was modified concurrently; reload and retry")
    return serialize_record(updated)


# ---- 10c. Analytics --------------------------------------------------------- #
async def employee_monthly(db: AsyncDatabase, emp_code: str, month: str) -> dict:
    employee = await get_employee_or_404(db, emp_code)
    month_start, month_end = month_bounds(month)
    joined_on = date.fromisoformat(employee["joined_on"])
    working_days = count_working_days(max(month_start, joined_on), month_end)

    rows = await run_aggregate(db, ATTENDANCE, build_employee_monthly_pipeline(emp_code, month))
    stats = rows[0] if rows else {}
    present_days = half_up(stats.get("present_days", 0), 2)
    attendance_pct = half_up(present_days / working_days * 100, 2) if working_days else None
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present_days,
        "leave_days": stats.get("leave_days", 0),
        "late_count": stats.get("late_count", 0),
        "total_late_minutes": stats.get("total_late_minutes", 0),
        "total_overtime_minutes": stats.get("total_overtime_minutes", 0),
        "attendance_pct": attendance_pct,
    }


async def department_summary(db: AsyncDatabase, month: str, department: Optional[str]) -> dict:
    items = await run_aggregate(db, EMPLOYEES, build_department_summary_pipeline(month, department))
    return {"month": month, "items": items}


async def late_leaderboard(db: AsyncDatabase, month: str, limit: int, department: Optional[str]) -> dict:
    items = await run_aggregate(db, ATTENDANCE, build_late_leaderboard_pipeline(month, limit, department))
    return {"month": month, "items": items}


async def department_trend(db: AsyncDatabase, department: str, date_from: str, date_to: str) -> dict:
    validate_trend_range(date_from, date_to)
    if await db[EMPLOYEES].find_one({"department": department}, {"_id": 1}) is None:
        raise NotFoundError(f"Department {department} not found")
    items = await run_aggregate(db, EMPLOYEES, build_department_trend_pipeline(department, date_from, date_to))
    return {"department": department, "items": items}


# ---- 10d. Explain ----------------------------------------------------------- #
def _require(value: Any, name: str) -> Any:
    if value is None:
        raise BusinessRuleError(("query", name), "Field required", error_type="missing")
    return value


async def _run_explain(db: AsyncDatabase, command: dict) -> dict:
    raw = await db.command({"explain": command, "verbosity": "executionStats"})
    return json.loads(json_util.dumps(raw))  # BSON types -> plain JSON


async def explain_endpoint(db: AsyncDatabase, target: ExplainTarget, params: dict, paging: Pagination) -> dict:
    if target is ExplainTarget.ATTENDANCE_LIST:
        query = build_attendance_list_query(params["emp_code"], params["date_from"], params["date_to"], params["status"])
        collection = ATTENDANCE
        command = {
            "find": ATTENDANCE,
            "filter": query,
            "projection": {"_id": 0},
            "sort": dict(ATTENDANCE_LIST_SORT),
            "skip": paging.skip,
            "limit": paging.page_size,
        }
    else:
        if target is ExplainTarget.EMPLOYEE_MONTHLY:
            collection = ATTENDANCE
            pipeline = build_employee_monthly_pipeline(_require(params["emp_code"], "emp_code"), _require(params["month"], "month"))
        elif target is ExplainTarget.DEPARTMENT_SUMMARY:
            collection = EMPLOYEES
            pipeline = build_department_summary_pipeline(_require(params["month"], "month"), params["department"])
        elif target is ExplainTarget.LATE_LEADERBOARD:
            collection = ATTENDANCE
            pipeline = build_late_leaderboard_pipeline(_require(params["month"], "month"), params["limit"], params["department"])
        else:  # DEPARTMENT_TREND
            department = _require(params["department"], "department")
            date_from, date_to = _require(params["from"], "from"), _require(params["to"], "to")
            validate_trend_range(date_from, date_to)
            collection = EMPLOYEES
            pipeline = build_department_trend_pipeline(department, date_from, date_to)
        command = {"aggregate": collection, "pipeline": pipeline, "cursor": {}}
    return {"endpoint": target.value, "collection": collection, "explain": await _run_explain(db, command)}


# =========================================================================== #
# 11. Database lifecycle
# =========================================================================== #
INDEX_SPECS: dict[str, list[IndexModel]] = {
    EMPLOYEES: [
        IndexModel([("emp_code", ASCENDING)], unique=True),  # identity + race-safe create
        IndexModel([("department", ASCENDING), ("emp_code", ASCENDING)]),  # list filter/sort, trend, summary
        IndexModel([("joined_on", ASCENDING)]),  # summary headcount cutoff (R9)
    ],
    ATTENDANCE: [
        IndexModel([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True),  # one record per day; $lookup
        IndexModel([("date", DESCENDING), ("emp_code", ASCENDING)]),  # list sort, month-range analytics
        IndexModel([("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)]),  # list by status
        IndexModel([("emp_code", ASCENDING), ("punch_in", DESCENDING)]),  # punch-out lookup
    ],
}


async def ensure_indexes(db: AsyncDatabase) -> None:
    """Idempotent. Each index is created on its own so one data problem cannot block the rest;
    connection errors propagate so the caller can retry later."""
    for collection, models in INDEX_SPECS.items():
        for model in models:
            try:
                await db[collection].create_indexes([model])
            except OperationFailure as exc:
                logger.error("Could not create index %s on %s: %s", model.document["key"], collection, exc)


async def ensure_indexes_once(app: FastAPI) -> None:
    if app.state.indexes_ready:
        return
    async with app.state.index_lock:
        if not app.state.indexes_ready:
            await ensure_indexes(app.state.db)
            app.state.indexes_ready = True
            logger.info("Indexes ensured")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = Settings.from_env()
    client: AsyncMongoClient = AsyncMongoClient(settings.mongo_uri, tz_aware=True, serverSelectionTimeoutMS=5000)
    app.state.db = client[settings.mongo_db]
    app.state.indexes_ready = False
    app.state.index_lock = asyncio.Lock()
    try:
        await ensure_indexes_once(app)
    except ConnectionFailure as exc:
        # Start anyway: /health reports 503 and retries index creation once MongoDB answers.
        logger.warning("MongoDB unreachable at startup (%s); will retry from /health", exc)
    try:
        yield
    finally:
        await client.close()


# =========================================================================== #
# 12. App instance & middleware
# =========================================================================== #
class RequestContextMiddleware:
    """Pure ASGI middleware: propagates/assigns X-Request-ID and logs one line per request."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = dict(scope.get("headers") or [])
        request_id = headers.get(b"x-request-id", b"").decode("latin-1") or uuid.uuid4().hex
        scope.setdefault("state", {})["request_id"] = request_id
        started = time.perf_counter()
        status_code = 500

        async def send_with_request_id(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                MutableHeaders(scope=message).append("X-Request-ID", request_id)
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        finally:
            logger.info(
                "%s %s -> %s %.1fms rid=%s",
                scope["method"],
                scope["path"],
                status_code,
                (time.perf_counter() - started) * 1000,
                request_id,
            )


app = FastAPI(
    title="Employee Attendance & Analytics API",
    version="2.0.0",
    description="HROne attendance service. Contract: openapi.yaml; storage: DATA_MODEL.md.",
    lifespan=lifespan,
)
app.add_middleware(RequestContextMiddleware)


# =========================================================================== #
# 13. Exception handlers
# =========================================================================== #
def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "-")


async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


async def business_rule_error_handler(request: Request, exc: BusinessRuleError) -> JSONResponse:
    # Same shape as FastAPI's RequestValidationError body (contract: detail is an array).
    detail = [{"type": exc.error_type, "loc": list(exc.loc), "msg": exc.msg}]
    return JSONResponse(status_code=422, content={"detail": detail})


async def duplicate_key_handler(request: Request, exc: DuplicateKeyError) -> JSONResponse:
    return JSONResponse(status_code=status.HTTP_409_CONFLICT, content={"detail": "Resource already exists"})


async def database_unavailable_handler(request: Request, exc: PyMongoError) -> JSONResponse:
    logger.error("Database unavailable rid=%s: %s", _request_id(request), exc)
    return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"detail": "Database unavailable"})


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled error rid=%s", _request_id(request))
    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"detail": "Internal server error"})


app.add_exception_handler(AppError, app_error_handler)
app.add_exception_handler(BusinessRuleError, business_rule_error_handler)
app.add_exception_handler(DuplicateKeyError, duplicate_key_handler)
app.add_exception_handler(PyMongoError, database_unavailable_handler)
app.add_exception_handler(Exception, unhandled_exception_handler)


# =========================================================================== #
# 14. Dependencies
# =========================================================================== #
def get_db(request: Request) -> AsyncDatabase:
    return request.app.state.db


def get_pagination(
    page: Annotated[int, Query(ge=1, description="Page number, starting at 1 (R10).")] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE, description="Items per page (R10).")] = DEFAULT_PAGE_SIZE,
) -> Pagination:
    return Pagination(page=page, page_size=page_size)


Db = Annotated[AsyncDatabase, Depends(get_db)]
Paging = Annotated[Pagination, Depends(get_pagination)]


# =========================================================================== #
# 15. Routers & endpoints (thin: validate -> call service -> return)
# =========================================================================== #
system_router = APIRouter(tags=["system"])
employees_router = APIRouter(prefix="/employees", tags=["employees"])
attendance_router = APIRouter(prefix="/attendance", tags=["attendance"])
analytics_router = APIRouter(prefix="/analytics", tags=["analytics"])
admin_router = APIRouter(prefix="/admin", tags=["admin"])


# ---- System ---------------------------------------------------------------- #
@system_router.get("/health", response_model=HealthOut)
async def health(request: Request) -> dict:
    try:
        await request.app.state.db.command("ping")
        await ensure_indexes_once(request.app)
    except (ConnectionFailure, PyMongoError) as exc:
        raise ServiceUnavailableError("MongoDB is unreachable") from exc
    return {"status": "ok"}


# ---- Employees ------------------------------------------------------------- #
@employees_router.post("", response_model=EmployeeOut, status_code=status.HTTP_201_CREATED)
async def create_employee_endpoint(body: EmployeeCreate, db: Db) -> dict:
    return await create_employee(db, body)


@employees_router.get("", response_model=EmployeePage)
async def list_employees_endpoint(
    db: Db,
    paging: Paging,
    department: Annotated[Optional[str], Query(description="Exact, case-sensitive match.")] = None,
) -> dict:
    return await list_employees(db, department, paging)


# ---- Attendance ------------------------------------------------------------ #
@attendance_router.post("/punch-in", response_model=AttendanceRecordOut, status_code=status.HTTP_201_CREATED)
async def punch_in_endpoint(body: PunchInRequest, db: Db) -> dict:
    return await punch_in(db, body)


@attendance_router.post("/punch-out", response_model=AttendanceRecordOut)
async def punch_out_endpoint(body: PunchOutRequest, db: Db) -> dict:
    return await punch_out(db, body)


@attendance_router.get("", response_model=AttendancePage)
async def list_attendance_endpoint(
    db: Db,
    paging: Paging,
    emp_code: Optional[str] = None,
    date_from: Optional[DateStr] = None,
    date_to: Optional[DateStr] = None,
    status_filter: Annotated[Optional[Status], Query(alias="status")] = None,
) -> dict:
    query = build_attendance_list_query(emp_code, date_from, date_to, status_filter)
    return await list_attendance(db, query, paging)


@attendance_router.patch("/{emp_code}/{date}", response_model=AttendanceRecordOut)
async def regularize_attendance_endpoint(
    emp_code: str,
    date: Annotated[DateStr, Path(description="The record's attendance date, YYYY-MM-DD.")],
    body: RegularizeRequest,
    db: Db,
) -> dict:
    return await regularize_attendance(db, emp_code, date, body)


# ---- Analytics ------------------------------------------------------------- #
@analytics_router.get("/employees/{emp_code}/monthly", response_model=EmployeeMonthlyOut)
async def employee_monthly_endpoint(emp_code: str, month: Annotated[MonthStr, Query()], db: Db) -> dict:
    return await employee_monthly(db, emp_code, month)


@analytics_router.get("/departments/summary", response_model=DepartmentSummaryOut)
async def department_summary_endpoint(
    month: Annotated[MonthStr, Query()],
    db: Db,
    department: Optional[str] = None,
) -> dict:
    return await department_summary(db, month, department)


@analytics_router.get("/leaderboard/late", response_model=LeaderboardOut)
async def late_leaderboard_endpoint(
    month: Annotated[MonthStr, Query()],
    db: Db,
    limit: Annotated[int, Query(ge=1, le=MAX_LEADERBOARD_LIMIT)] = DEFAULT_LEADERBOARD_LIMIT,
    department: Optional[str] = None,
) -> dict:
    return await late_leaderboard(db, month, limit, department)


@analytics_router.get("/departments/{department}/trend", response_model=TrendOut)
async def department_trend_endpoint(
    department: str,
    date_from: Annotated[DateStr, Query(alias="from")],
    date_to: Annotated[DateStr, Query(alias="to")],
    db: Db,
) -> dict:
    return await department_trend(db, department, date_from, date_to)


# ---- Admin ----------------------------------------------------------------- #
@admin_router.get("/explain/{endpoint}", response_model=ExplainOut)
async def explain_endpoint_route(
    endpoint: ExplainTarget,
    db: Db,
    paging: Paging,
    emp_code: Optional[str] = None,
    month: Annotated[Optional[MonthStr], Query()] = None,
    department: Optional[str] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LEADERBOARD_LIMIT)] = DEFAULT_LEADERBOARD_LIMIT,
    date_from: Optional[DateStr] = None,
    date_to: Optional[DateStr] = None,
    status_filter: Annotated[Optional[Status], Query(alias="status")] = None,
    range_from: Annotated[Optional[DateStr], Query(alias="from")] = None,
    range_to: Annotated[Optional[DateStr], Query(alias="to")] = None,
) -> dict:
    params = {
        "emp_code": emp_code,
        "month": month,
        "department": department,
        "limit": limit,
        "date_from": date_from,
        "date_to": date_to,
        "status": status_filter,
        "from": range_from,
        "to": range_to,
    }
    return await explain_endpoint(db, endpoint, params, paging)


app.include_router(system_router)
app.include_router(employees_router)
app.include_router(attendance_router)
app.include_router(analytics_router)
app.include_router(admin_router)
