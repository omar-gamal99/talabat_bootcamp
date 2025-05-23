from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def greet():
    print("Hello from Amir!")


with DAG(
    dag_id="my_airflow_dag",
    schedule=None,
    start_date=datetime(2025, 5, 21),
    catchup=True,
    tags=["example"],
) as dag:
    greet_task = PythonOperator(
        task_id="greet_task",
        python_callable=greet,
    )

