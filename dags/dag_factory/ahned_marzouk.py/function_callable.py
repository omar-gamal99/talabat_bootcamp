from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'Ahmed_marzouk',
    'start_date': datetime(2025, 5, 23),
    'retries': 1,
}

def python_print_function(**kwargs):
    print("Hello from Airflow task!")


with DAG(dag_id = 'run_any_python_function',default_args = default_args,
         schedule_interval=None,
         catchup = False
          ) as dag :
    
    run_function = PythonOperator(task_id ='run_function',
                                  python_callable = python_print_function)
    

    run_function

