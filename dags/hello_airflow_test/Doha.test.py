from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def Print_Python():
    print("Hello...Welcome To ITI")
    # Print("Doha Is Here")

with DAG(
    dag_id="example_airflow_dag_Doha",
    schedule=None,
    start_date=datetime(2025, 5, 22),
    catchup=False,
    tags=["example"],
) as dag:
    greet_task = PythonOperator(
        task_id="Python_task",
        python_callable=Print_Python,
    )

