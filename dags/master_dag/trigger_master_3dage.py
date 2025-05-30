from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

# List of child DAGs to trigger
child_dags = ['customers_db_extract', 'orders_db_extract', 'products_db_extract']

with DAG(
    dag_id='master_dags_by_hodaa',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    for child_dag in child_dags:
        TriggerDagRunOperator(
            task_id=f'trigger_{child_dag}',
            trigger_dag_id=child_dag,)