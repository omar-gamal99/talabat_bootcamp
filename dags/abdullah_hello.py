from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define the Python function to be called
def print_hello():
    print("Hello, Airflow!")

# Define the DAG
with DAG(
    dag_id="hello_airflow_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",  # Runs once per day
    catchup=False,
    tags=["example"]
) as dag:

    # Create a task
    hello_task = PythonOperator(
        task_id="print_hello_task",
        python_callable=print_hello
    )

    hello_task
