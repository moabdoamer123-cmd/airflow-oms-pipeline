# Apache Airflow OMS ETL Pipeline

A production-style, end-to-end ETL pipeline for an Order Management System (OMS), built entirely with native Apache Airflow — no dbt, no Great Expectations, no external transformation frameworks. Every stage of the data lifecycle (extract, validate, transform, load, aggregate, notify) is implemented as orchestrated Airflow tasks.

**Author:** Mohamed Amer (Mo Amer) — Data Engineer

---

## Architecture

```
Neon PostgreSQL (oms_core)
        │
Airflow Scheduler (CeleryExecutor + Redis)
        │
Dynamic DAG (Dynamic Task Mapping, one task per table)
        │
Extract & Validate  →  Transform  →  Load Warehouse (star schema)
        │
Business Aggregations  →  Email Notifications (on failure)
        │
Airflow UI / Logs
```

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.9.1 (CeleryExecutor) |
| Source & Warehouse DB | PostgreSQL (Neon, serverless) |
| Message Broker | Redis |
| Deployment | Docker Compose |
| Extraction / Transformation | Python, Pandas, SQLAlchemy |
| Testing | Pytest (DAG integrity tests) |

## Project Structure

```
airflow_oms_project/
├── dags/
│   ├── oms_extraction.py            # Dynamic Task Mapping: extract + validate all 8 tables
│   ├── oms_test_connection.py       # Simple Postgres connectivity smoke test
│   ├── oms_transformation.py        # Full pipeline: stage → validate → dims → fact → aggregations
│   └── sql/
│       └── analytics_schema_ddl.sql # Reference DDL for the analytics star schema
├── eda/
│   └── explore_oms_data.py          # Standalone EDA script — run manually, NOT an Airflow DAG
├── plugins/
│   └── business_days_plugin.py      # Custom BusinessDaysOnlyTimetable (skips weekends)
├── tests/
│   └── test_dag_integrity.py        # Pytest: DAGs import cleanly, no cyclical dependencies
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── .env.example                     # Template — copy to .env and fill in real secrets
└── .gitignore
```

## Getting Started

### 1. Prerequisites
- Docker & Docker Compose
- A PostgreSQL database (this project targets [Neon](https://neon.tech)) with an `oms_core` schema containing: `customers`, `dates`, `employees`, `orderitems`, `orders`, `products`, `stores`, `suppliers`

### 2. Configure secrets
```bash
cp .env.example .env
```
Fill in `.env` with your real Neon connection string, SMTP credentials, and a generated Airflow secret key. **`.env` is gitignored — never commit it.**

### 3. Launch
```bash
docker compose up --build
```
This builds all services (`postgres`, `redis`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`, `airflow-worker`), runs DB migrations, creates the admin user, and provisions the `postgres_pool` (5 slots) automatically.

### 4. Open the UI
Visit `http://localhost:8081` and log in with the admin credentials set in your `.env`.

### 5. Run the pipeline
Unpause and trigger `oms_transformation` from the DAGs list. `oms_extraction` and `oms_test_connection` are earlier, standalone DAGs kept for reference.

## Data Model (Star Schema)

| Table | Type | Key |
|---|---|---|
| `dim_customer` | Dimension | `customerid` (VARCHAR) |
| `dim_product` | Dimension | `productid` (INT) |
| `dim_store` | Dimension | `storeid` (VARCHAR) |
| `dim_employee` | Dimension | `employeeid` (INT) |
| `dim_date` | Dimension | `datekey` (DATE) |
| `fact_sales` | Fact | `orderitemid` (INT) |

`orderitemid` — not `(orderid, productid)` — is used as the fact table's primary key, since it is already unique per line item in the source and avoids false upsert conflicts on multi-line orders.

## Engineering Highlights

- **Dynamic Task Mapping** — each of the 8 source tables runs as its own isolated task instance (`.expand()`); a single table failing retries alone.
- **Bounded connection pooling** — a dedicated 5-slot Airflow pool (`postgres_pool`) caps concurrent connections to the source database.
- **Idempotent by design** — `CREATE TABLE IF NOT EXISTS` schema setup and `ON CONFLICT DO UPDATE` upserts make repeated runs safe.
- **Custom scheduling** — a custom `BusinessDaysOnlyTimetable` Airflow plugin skips weekends automatically.
- **Failure alerting** — an `on_failure_callback` sends a formatted SMTP email (DAG, task, timestamp, error) the moment any task fails.
- **DAG integrity testing** — a Pytest suite validates the DAG imports cleanly and has no cyclical dependencies before deployment.

### Two real debugging case studies

1. **Silent schema mismatch** — 4 of 8 extraction tasks failed on *every* run, always the same four. The constant pattern ruled out infrastructure (pool/connection issues fail randomly, not consistently) and pointed to a logic bug: queries were missing the `oms_core.` schema prefix and silently searching Postgres' default `public` schema instead. Fixed with an explicit schema-qualified query.
2. **Cardinality violation on upsert** — loading `fact_sales` threw `psycopg2.errors.CardinalityViolation: ON CONFLICT DO UPDATE command cannot affect row a second time`. The `JOIN` between staged orders and order items produced duplicate `orderitemid` rows within a single insert batch. Fixed with `SELECT DISTINCT ON (orderitemid) ... ORDER BY orderitemid`, guaranteeing one row per key before the upsert runs.

## Testing

```bash
pytest tests/
```

## Roadmap / Delivery Phases

| Phase | Focus |
|---|---|
| 1 | Docker Compose, LocalExecutor, basic DAGs, TaskGroups, branching, retries |
| 2 | XCom, Variables, Connections, Jinja templates, TaskFlow API, Dynamic Task Mapping |
| 3 | ETL pipeline (PostgresHook + SQL operators), validation, star schema load |
| 4 | Custom timetable, retry policies, failure callbacks, Pytest integrity tests |
| 5 | CeleryExecutor, Redis, monitoring, task duration analysis, worker scaling |

All five phases are complete in the current deployment.

## Notes & Assumptions

- `orders.status` codes (`01`, `02`, `03`) are mapped to `Pending` / `Processing` / `Delivered` based on common OMS conventions — this mapping is **not confirmed from source documentation** and should be verified before relying on it for business reporting.