from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_db_connection():
    hook = PostgresHook(postgres_conn_id='oms_postgres_conn')
    connection = hook.get_conn()
    print("Connection Successful! ✅")
    connection.close()

with DAG(
    dag_id='oms_test_connection',
    start_date=datetime(2026, 7, 1),
    schedule='@once',
    catchup=False
) as dag:

    task = PythonOperator(
        task_id='test_postgres',
        python_callable=check_db_connection
    )