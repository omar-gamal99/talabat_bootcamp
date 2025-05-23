from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime


def add2num(num1,num2):
    print(f"the sum is {num1+num2}")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'simple_python_task_toAdd_twoNums',
    default_args=default_args,
    description='A simple DAG with a python task',
    schedule_interval='@daily',  # Run once a day
    catchup=False,  # to prevent the dag from trying to run agian and catch days it didnt run
) as dag:

    # Define the Bash task
    task1 = PythonOperator(
        task_id='run_pyton_command_1',
        python_callable=add2num,
        op_kwargs={"num1":323,"num2":123123}
    )


    task1 

    
