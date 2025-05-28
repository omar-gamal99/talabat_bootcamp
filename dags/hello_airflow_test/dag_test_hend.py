from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def my_function():
    print("new python dag")


with DAG(
    dag_id='python_dag_hend',
    schedule_interval=None,
    start_date=datetime(2025, 5, 15)

)as dag:

    python_task = PythonOperator(
        task_id='python_dag_hend',
        python_callable=my_function
    )
