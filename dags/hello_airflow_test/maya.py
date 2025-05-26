from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the function to run
def print_message():
    print("Hello from Maya!")

# Define the DAG
with DAG(
    dag_id="hello_maya_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Set to None for manual trigger
    catchup=False,
    tags=["example"],
) as dag:

    hello_task = PythonOperator(
        task_id="print_hello_message",
        python_callable=print_message
    )

    hello_task
