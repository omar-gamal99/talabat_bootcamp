import dag_factory
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime   


def hello_world():
    print("Hello, world!")
    

# Define the DAG
with DAG(
    dag_id="hello_world_dag",  # Unique DAG name
    start_date=datetime(2024, 1, 1),  # Start date
    schedule_interval="@daily",  # Run daily (use None for manual triggers)
    catchup=False,  # Disable backfilling
) as dag:

    # Define the task
    hello_task = PythonOperator(
        task_id="say_hello_task",
        python_callable=hello_world,  # Function to execute
    )

# Task dependency (not needed here since there's only one task)
hello_task