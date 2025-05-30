from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

child_dag_ids = [
    "customers_db_extract",
    "orders_db_extract",
    "products_db_extract",
]

with DAG(
    dag_id="parent_dag_amir",
    start_date=datetime(2025, 5, 30),
    schedule_interval=None,
    catchup=False, 
) as dag:

    for dag_id in child_dag_ids:
        TriggerDagRunOperator(
            task_id=f"trigger_dag_{dag_id}",  
            trigger_dag_id=dag_id,
            wait_for_completion=False,
            reset_dag_run=True,
        )

