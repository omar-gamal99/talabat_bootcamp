from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello, World!")

with DAG(
    dag_id='moessam_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    print_hello_task = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello
    )