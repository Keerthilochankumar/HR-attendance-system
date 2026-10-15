# DECISIONS.md

1. **Indexes:**
On `employees`: `{emp_code: 1}` (unique, enforces identity); `{(department, emp_code): 1}` (filters/sorts); `{joined_on: 1}` (headcount cutoff).
On `attendance_logs`: `{(emp_code, date): 1}` (unique, prevents duplicate punch-ins and powers lookup); `{(date, emp_code): (-1, 1)}` (list sort and month ranges); `{(status, date, emp_code): (1, -1, 1)}` (status-filtered lists); `{(emp_code, punch_in): (1, -1)}` (punch-out resolution).

2. **Punch-in race:**
Concurrent identical punch-ins attempt `insert_one` simultaneously. MongoDB's unique index on `(emp_code, date)` atomically serializes index entry creation. Exactly one insert succeeds (201); the second triggers `DuplicateKeyError`, caught and mapped to HTTP 409 Conflict.

3. **Ties:**
The leaderboard computes standard competition ranking via MongoDB's `$rank` window operator (ranks 1, 2, 2, 4). The `limit` filter applies after ranking. If employees tie at the cutoff rank, all tied employees are included (which may exceed `limit` rows), ordered deterministically by `emp_code` ascending.

4. **Headcount:**
Department summary roots its aggregation pipeline in `employees` filtered by `joined_on <= month_end`. A correlated `$lookup` fetches monthly logs. Employees with empty logs are preserved and counted towards headcount.

5. **100x data:**
At 100x scale (~10M records), we would maintain pre-aggregated daily/monthly summary collections updated incrementally via MongoDB Change Streams, eliminating runtime cross-collection lookups and densify operations.
