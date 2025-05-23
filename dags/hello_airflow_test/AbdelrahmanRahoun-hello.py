from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime

def rahoun():
    print('hello_rahoun')
#  ggs
with DAG(
    dag_id = 'talabat-test',
    start_date = datetime(2025-5-23),
    schedule = None ,
    tags = ['example']

) as dag :
    hello_task = PythonOperator(
        task_id="say_hello_task",
        python_callable=rahoun,
    )

    hello_task    

