from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_john():
    print("Hello My AirFlow My Name is John.")


with DAG(
    dag_id='john-first_dag',
    schedule=None,
    start_date=datetime(2025,5,23),
    catchup=False,
    tags=['hello_john'],
)as dag:
    hello_john=PythonOperator(
        task_id='Print_Example_By_John',
        python_callable=hello_john
    )