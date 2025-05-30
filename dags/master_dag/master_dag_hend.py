from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    'trigger_dag_hend',
    start_date=datetime(2025, 5, 30),
    schedule_interval=None,
    catchup=False,
) as dag:

    trigger = TriggerDagRunOperator(
        task_id='trigger_other_dag_hend',
        trigger_dag_id='postgres_to_bigquery_hend',
        wait_for_completion=True
    )