from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello():
    print("Hello, World from Adham")


with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2025, 5, 24),  
    schedule_interval="@daily",       
    catchup=False                     
) as dag:

    
    hello_task = PythonOperator(
        task_id="print_hello_task",
        python_callable=print_hello
    )

    # Set task dependencies (optional here since we only have one)
    hello_task
# --------------------------------------------------------------------------------




 