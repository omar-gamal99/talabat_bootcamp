from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

child_dag_ids = [
    'customers_db_extract',
    'orders_db_extract',
    'products_db_extract'
]

with DAG(
    dag_id='parent_trigger_multiple_dags_youmna',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as parent_dag:

    for dag_id in child_dag_ids:
        TriggerDagRunOperator(
            task_id=f'trigger_{dag_id}',
            trigger_dag_id=dag_id,
            wait_for_completion=False,
            reset_dag_run=True,
        )
