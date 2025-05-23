from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def my_etl_function():
    print("Running ETL task...")

# Default arguments for the DAG
default_args = {
    'start_date': datetime(2025, 5, 23),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='my_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',  
    catchup=False,
    description='A simple DAG to run a Python function',
    tags=['example'],
) as dag:

    run_etl = PythonOperator(
        task_id='run_my_etl_function',
        python_callable=my_etl_function
    )

    run_etl  # This sets the task order (only one task here)