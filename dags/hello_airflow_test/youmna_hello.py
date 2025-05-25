from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print('Hello from Airflow')

with DAG(
    dag_id='print_hello_dag',
    start_date=datetime(2024/8/8),
    catchup=False,
    schedule_interval="@daily"
) as dag:
    hello_task=PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello,
    )

    # help me