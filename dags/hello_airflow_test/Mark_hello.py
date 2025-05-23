from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def greet():
    print("Hello from Airflow!")

with DAG(
    dag_id="example_airflow_dag_mark",
    schedule_interval=None,
    start_date=datetime(2025, 5, 23),
    catchup=False,
    tags=["example"],
) as dag:
    greet_task = PythonOperator(
        task_id="greet_task",
        python_callable=greet,
    )