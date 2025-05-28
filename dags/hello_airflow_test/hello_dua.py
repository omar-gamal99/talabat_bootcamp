from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# This is the function Airflow will run
def print_hello():
    print(" Hello from Airflow!")

# Define the DAG
with DAG(
    dag_id='simple_print_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Only run manually
    catchup=False
) as dag:

    # Define the Python task
    hello_task = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello
    )
