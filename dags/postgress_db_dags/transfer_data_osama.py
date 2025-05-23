from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import pandas as pd

# Extract orders table and save to CSV
def extract_postgres_orders():
    hook = PostgresHook(postgres_conn_id='postgres_osama')
    sql = "SELECT * FROM public.orders"
    df = hook.get_pandas_df(sql)

    # Save the DataFrame to a CSV file
    # 🪟 If you're on Windows, use a proper path like below:
    df.to_csv('C:\\Users\\TKmind\\Desktop\\orders_data.csv', index=False)

with DAG(
    dag_id='extract_orders_from_postgres',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_orders_task',
        python_callable=extract_postgres_orders
    )
