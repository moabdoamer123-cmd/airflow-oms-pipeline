from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task, task_group
from datetime import datetime, timedelta
import logging

OMS_TABLES = [
    'customers', 
    'dates', 
    'employees', 
    'orderitems', 
    'orders', 
    'products', 
    'stores', 
    'suppliers'
]

default_args = {
    'owner': 'Mo Amer',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='oms_full_dynamic_extraction',
    default_args=default_args,
    start_date=datetime(2026, 7, 17),
    schedule=None,
    max_active_tasks=3,
    catchup=False
) as dag:
    
    @task_group(group_id='extract_and_validate')
    def extract_and_validate_group(table_name):
        
        @task(pool='postgres_pool', retries=5, retry_delay=timedelta(minutes=2))
        def extract_table(table):
            hook = PostgresHook(postgres_conn_id='oms_postgres_conn')
            df = hook.get_pandas_df(f"SELECT * FROM oms_core.{table}")
            return len(df)

        @task
        def validate_data(row_count, table):
            if row_count == 0:
                raise ValueError(f"Table {table} is empty!")
            logging.info(f"Table {table} has exactly {row_count} rows. ✅")

        row_count = extract_table(table_name)
        validate_data(row_count, table_name)

    extract_and_validate_group.expand(table_name=OMS_TABLES)