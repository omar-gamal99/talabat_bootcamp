from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def greetings():
    print("Hi, there!")

default_args = {
    'owner' : 'ayah',
    'start_date' : datetime(2025,5,23)


}
with DAG("greetings-dag",
        description = 'greeting msg in Airflow',
        default_args = default_args) as greetings_dag:
    greet_task = PythonOperator(
        task_id="greetings",
        python_callable=greetings
    )
greet_task