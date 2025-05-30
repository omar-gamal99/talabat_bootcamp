from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='master_dag_by_hoda',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    trigger = TriggerDagRunOperator(
        task_id='trigger_child',
        trigger_dag_id='api_to_bigquery_payments_youmna'  # must match dag_id in child_dag.py
    )