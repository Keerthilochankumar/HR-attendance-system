# Employee Attendance & Analytics API

FastAPI + MongoDB backend implementation compliant with Contract v2.0 (OpenAPI 3.0.3).

## Setup & Running

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment**:
   Ensure `MONGO_URI` and `MONGO_DB` are exported in your environment (or specified in a local `.env` file). Real environment variables take precedence.
   MongoDB 6.0+ is required.

3. **Start the API**:
   ```bash
   uvicorn app.main:app --port 8000
   ```

4. **Seed Sample Data (Optional)**:
   ```bash
   python sample_seed.py
   ```

## Notes & Design Choices
- **Driver**: Implemented with native PyMongo async driver (`AsyncMongoClient`) for high-throughput, non-blocking I/O.
- **Indexes**: All compound and unique indexes are created idempotently at application startup during the lifespan context.
- **Race Condition Safety**: Concurrent punch-ins, punch-outs, and regularizations rely on database-level atomic operators (`find_one_and_update`, unique compound indexes, and optimistic version guards).
- **Analytics In-Database**: Aggregation pipelines perform all computations (including half-up rounding, window functions with `$rank`, and calendar gap filling via `$densify`) natively inside MongoDB without Python-level iteration.
