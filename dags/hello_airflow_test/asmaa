from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the function to be called
def print_hello():
    print("Hello from Airflow!")

# Define the default arguments for the DAG
default_args = {
    'start_date': datetime(2025, 5, 23),
    'catchup': False,
}

# Instantiate the DAG
with DAG(
    dag_id='hello_airflow_dag_asmaa',
    default_args=default_args,
    schedule_interval='@daily',  # Runs once a day
    tags=['example'],
    description='A simple DAG that prints a message.',
) as dag:

    # Define a task using PythonOperator
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    hello_task
