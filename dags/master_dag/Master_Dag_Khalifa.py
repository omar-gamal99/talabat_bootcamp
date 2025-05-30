from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2025, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag_names = [
    'orders_db_extract',
    'products_db_extract',
    'customers_db_extract'
]

with DAG(
    dag_id='master_dag_AKhalifa',
    default_args=default_args,
    schedule_interval='1 0 * * *',  # Every day at 12:01 AM
    catchup=False,
    tags=['master', 'trigger']
) as dag:

    for dag_name in dag_names:
        TriggerDagRunOperator(
            task_id=f'trigger_{dag_name}',
            trigger_dag_id=dag_name,
            wait_for_completion=True
        )
