from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

def greet():
    print("hello world my name is elham")

with DAG(
    dag_id='hello_world_dag_ElhamShawkat',
    description='A simple hello world DAG',
    start_date=datetime(2025, 5, 15),
) as dag:
    greet_task = PythonOperator(
        task_id='greet_elham_task',
        python_callable=greet
    )

    greet_task
# This DAG is a simple example that prints "hello world my name is elham" to the console.