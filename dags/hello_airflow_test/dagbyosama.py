from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define your function here or import it
def greet():
    print("Hello from Airflow!!!!")

# Define the DAG
with DAG(
    dag_id='greet_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',  # runs every day
    catchup=False
) as dag:

    greet_task = PythonOperator(
        task_id='greet_task',
        python_callable=greet
    )
