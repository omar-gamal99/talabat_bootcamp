from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello hoda")
with DAG(

    dag_id='hello_frist_dag_1',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # run manually or once
    catchup=False
) as dag:
 
 task_hello = PythonOperator(
        task_id='hello_frist_dag_1',
        python_callable=hello()
    )
