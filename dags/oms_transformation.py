"""
OMS ETL Transformation Pipeline — Phase 3 (v3, self-contained)
-------------------------------------------------------------
Extracts all 8 oms_core tables from Neon Postgres, cleans/transforms them
(fixing the issues found in the EDA), stages them, then loads the
analytics star schema (dim_customer, dim_product, dim_store, dim_employee,
dim_date, fact_sales).

This version is self-contained: the first task creates the analytics
schema and tables (CREATE ... IF NOT EXISTS) itself, so the DAG can be
deployed and run end-to-end with no separate manual SQL step.
analytics_schema_ddl.sql is kept alongside this file purely as a
human-readable reference (or for a future migration tool) — it is no
longer required to run it by hand.

Prerequisite: make sure the Airflow pool from Phase 1 still exists:
    airflow pools set postgres_pool 5 "Postgres extraction pool"

v3 change:
  18. Folded the DDL into the DAG as a `create_analytics_schema` task
      (CREATE ... IF NOT EXISTS everywhere), so re-running it daily is
      a safe no-op after the first run. Trade-off: schema setup now
      lives inside a recurring pipeline instead of being a separate,
      deliberately-reviewed step. Fine here since every statement is
      IF NOT EXISTS — if this ever needs a real ALTER TABLE / column
      change later, that should go through a proper migration step,
      not silently inside this daily task.

v2 changes (retained):
  13. Dynamic Task Mapping — one task instance per table via
      `.expand()` instead of one PythonOperator for all 8 tables, so
      a single table's failure only retries that table.
  14. `to_sql()` uses chunksize + method='multi'. An optional
      COPY-based loader (bulk_load_via_copy) is included for if a
      table later grows very large — not used by default.
  15. max_active_tasks=3 bounds concurrent Neon connections.
  16. Each mapped extraction task disposes its own SQLAlchemy engine.
  17. Extraction task overrides retries=5 / retry_delay=2min, matching
      the resilience proven necessary against Neon in Phase 1.
"""

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
import logging
from airflow.utils.email import send_email
from business_days_plugin import BusinessDaysOnlyTimetable

SOURCE_SCHEMA = "oms_core"
STAGING_SCHEMA = "staging"
POSTGRES_CONN_ID = "oms_postgres_conn"

OMS_TABLES = [
    "customers", "dates", "employees", "orderitems",
    "orders", "products", "stores", "suppliers",
]

# ASSUMPTION: status codes inferred from common OMS conventions —
# NOT confirmed from source documentation. Verify before production use.
STATUS_MAP = {
    "01": "Pending",
    "02": "Processing",
    "03": "Delivered",
}

# ==========================================
# Table-specific cleaning logic
# ==========================================
def _clean_dates(df):
    df["date"] = pd.to_datetime(df["date"])
    df["is_weekend"] = df["dayofweek"].isin(["Saturday", "Sunday"])
    return df


def _clean_employees(df):
    df["hiredate"] = pd.to_datetime(df["hiredate"])
    df["managerid"] = df["managerid"].astype("Int64")  # nullable int, avoids float64 upcast
    return df


def _clean_orderitems(df):
    df["total_price"] = df["quantity"] * df["unitprice"]
    return df


def _clean_orders(df):
    df["orderdate"] = pd.to_datetime(df["orderdate"])
    df["status_desc"] = df["status"].map(STATUS_MAP)
    unmapped = df[df["status_desc"].isna()]["status"].unique()
    if len(unmapped) > 0:
        logging.warning(f"Unmapped status codes found: {unmapped}")
    return df


# customers, products, stores, suppliers need no special cleaning today.
TABLE_CLEANERS = {
    "dates": _clean_dates,
    "employees": _clean_employees,
    "orderitems": _clean_orderitems,
    "orders": _clean_orders,
}


# ==========================================
# Optional scaling path — NOT used by default, see v2 changelog point 14
# ==========================================
def bulk_load_via_copy(df: pd.DataFrame, table: str, schema: str, pg_hook: PostgresHook):
    """
    Loads a DataFrame using Postgres COPY instead of INSERT statements.
    Assumes the target table already exists and TRUNCATEs it first to
    match to_sql's replace semantics. Switch to this only once a
    table's row count makes to_sql noticeably slow or memory-heavy.
    """
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)
    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {schema}.{table};")
            cursor.copy_expert(
                sql=f"COPY {schema}.{table} FROM STDIN WITH (FORMAT csv, NULL '\\\\N')",
                file=buffer,
            )
        conn.commit()
    finally:
        conn.close()


def validate_staging_counts():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    for table in OMS_TABLES:
        count = pg_hook.get_first(f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.stg_{table}")[0]
        if count == 0:
            raise ValueError(f"Staging table stg_{table} is empty after extraction!")
        logging.info(f"stg_{table}: {count} rows staged. ✅")

def on_failure_callback(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    
    subject = f"Airflow Alert: Task Failed [{dag_id} - {task_id}]"
    
    body = f"""
    <h3>Task Failure Notification</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Execution Time:</b> {execution_date}</p>
    <p><b>Error Details:</b> {exception}</p>
    <hr>
    <p>Please check the Airflow UI logs for more information.</p>
    """
    
   
    send_email(to="mo.abdo.amer123@gmail.com", subject=subject, html_content=body)


default_args = {
    'owner': 'Mo Amer',
    'depends_on_past': False,
    'retries': 3,                     
    'retry_delay': timedelta(minutes=5),  
    'on_failure_callback': on_failure_callback,
}

with DAG(
    dag_id="oms_transformation",
    default_args=default_args,
    start_date=datetime(2026, 7, 20),
    schedule=BusinessDaysOnlyTimetable(),
    max_active_tasks=3,
    catchup=False,
    template_searchpath="/opt/airflow/dags",
) as dag:

    create_schema_task = SQLExecuteQueryOperator(
        task_id="create_analytics_schema",
        conn_id=POSTGRES_CONN_ID,
        sql="sql/analytics_schema_ddl.sql",
    )

    @task(pool="postgres_pool", retries=5, retry_delay=timedelta(minutes=2))
    def extract_and_stage_table(table_name: str):
        """One task instance per table (Dynamic Task Mapping) — a
        failure on one table only retries that table, not all 8."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        try:
            df = pd.read_sql(f"SELECT * FROM {SOURCE_SCHEMA}.{table_name}", engine)

            cleaner = TABLE_CLEANERS.get(table_name)
            if cleaner:
                df = cleaner(df)

            df.to_sql(
                f"stg_{table_name}", engine, schema=STAGING_SCHEMA,
                if_exists="replace", index=False,
                chunksize=1000, method="multi",
            )
            logging.info(f"{table_name}: {len(df)} rows staged.")
            return len(df)
        finally:
            engine.dispose()

    staged = extract_and_stage_table.expand(table_name=OMS_TABLES)

    validate_staging_task = PythonOperator(
        task_id="validate_staging_counts",
        python_callable=validate_staging_counts,
    )

    # ---- Dimensions (parallel, after staging is validated) ----

    load_dim_customer = SQLExecuteQueryOperator(
        task_id="load_dim_customer",
        conn_id=POSTGRES_CONN_ID,
        sql="""
            INSERT INTO analytics.dim_customer (customerid, firstname, lastname, email, phone)
            SELECT customerid, firstname, lastname, email, phone
            FROM staging.stg_customers
            ON CONFLICT (customerid) DO UPDATE SET
                firstname = EXCLUDED.firstname,
                lastname  = EXCLUDED.lastname,
                email     = EXCLUDED.email,
                phone     = EXCLUDED.phone;
        """,
    )

    load_dim_product = SQLExecuteQueryOperator(
        task_id="load_dim_product",
        conn_id=POSTGRES_CONN_ID,
        sql="""
            INSERT INTO analytics.dim_product (productid, name, category, retailprice)
            SELECT productid, name, category, retailprice
            FROM staging.stg_products
            ON CONFLICT (productid) DO UPDATE SET
                name        = EXCLUDED.name,
                category    = EXCLUDED.category,
                retailprice = EXCLUDED.retailprice;
        """,
    )

    load_dim_store = SQLExecuteQueryOperator(
        task_id="load_dim_store",
        conn_id=POSTGRES_CONN_ID,
        sql="""
            INSERT INTO analytics.dim_store (storeid, storename, city, state)
            SELECT storeid, storename, city, state
            FROM staging.stg_stores
            ON CONFLICT (storeid) DO UPDATE SET
                storename = EXCLUDED.storename,
                city      = EXCLUDED.city,
                state     = EXCLUDED.state;
        """,
    )

    load_dim_employee = SQLExecuteQueryOperator(
        task_id="load_dim_employee",
        conn_id=POSTGRES_CONN_ID,
        sql="""
            INSERT INTO analytics.dim_employee (employeeid, firstname, lastname, jobtitle, managerid)
            SELECT employeeid, firstname, lastname, jobtitle, managerid
            FROM staging.stg_employees
            ON CONFLICT (employeeid) DO UPDATE SET
                firstname = EXCLUDED.firstname,
                lastname  = EXCLUDED.lastname,
                jobtitle  = EXCLUDED.jobtitle,
                managerid = EXCLUDED.managerid;
        """,
    )

    load_dim_date = SQLExecuteQueryOperator(
        task_id="load_dim_date",
        conn_id=POSTGRES_CONN_ID,
        sql="""
            INSERT INTO analytics.dim_date (datekey, year, quarter, month, day, dayofweek, is_weekend)
            SELECT date, year, quarter, month, day, dayofweek, is_weekend
            FROM staging.stg_dates
            ON CONFLICT (datekey) DO NOTHING;
        """,
    )

    
    # KPIs (Business Aggregations)
    aggregate_sales_task = SQLExecuteQueryOperator(
        task_id='create_business_aggregations',
        conn_id=POSTGRES_CONN_ID, 
        sql="""
            DROP TABLE IF EXISTS analytics.daily_sales_summary;
            CREATE TABLE analytics.daily_sales_summary AS
            SELECT 
                orderdate,
                storeid,
                SUM(total_price) AS total_revenue,
                COUNT(orderitemid) AS total_orders
            FROM analytics.fact_sales
            GROUP BY orderdate, storeid;
        """
    )
    
    # ---- Fact table (depends on ALL dimensions being loaded first) ----

    load_fact_sales_task = SQLExecuteQueryOperator(
    task_id="load_fact_sales",
    conn_id=POSTGRES_CONN_ID,
    sql="""
        INSERT INTO analytics.fact_sales (
            orderitemid, orderid, customerid, productid, storeid,
            employeeid, orderdate, status_code, status_desc,
            quantity, unitprice, total_price
        )
        SELECT DISTINCT ON (oi.orderitemid)
            oi.orderitemid,
            o.orderid,
            o.customerid,
            oi.productid,
            o.storeid,
            o.employeeid,
            o.orderdate,
            o.status AS status_code,
            o.status_desc,
            oi.quantity,
            oi.unitprice,
            oi.total_price
        FROM staging.stg_orders o
        JOIN staging.stg_orderitems oi ON o.orderid = oi.orderid
        ORDER BY oi.orderitemid
        ON CONFLICT (orderitemid) DO UPDATE SET
            quantity    = EXCLUDED.quantity,
            unitprice   = EXCLUDED.unitprice,
            total_price = EXCLUDED.total_price,
            status_code = EXCLUDED.status_code,
            status_desc = EXCLUDED.status_desc;
    """,
)

    # ==========================================
    # Task Dependencies
    # ==========================================
    # Create schema (idempotent) -> mapped extraction -> validate row
    # counts -> all dimensions in parallel -> fact table
    create_schema_task >> staged >> validate_staging_task
    validate_staging_task >> [
        load_dim_customer,
        load_dim_product,
        load_dim_store,
        load_dim_employee,
        load_dim_date,
    ] >> load_fact_sales_task >> aggregate_sales_task