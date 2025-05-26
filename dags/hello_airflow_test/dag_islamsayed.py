from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='talabat_simple_dag_islamsayed',
    default_args=default_args,
    description='A simple example DAG for talabat bootcamp',
    schedule_interval='@daily',  # runs once every day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # Task 1: Print date
    print_date = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    # Task 2: Sleep for 5 seconds
    sleep_task = BashOperator(
        task_id='sleep_for_5_seconds',
        bash_command='sleep 5'
    )

    # Set task dependencies
    print_date >> sleep_task
