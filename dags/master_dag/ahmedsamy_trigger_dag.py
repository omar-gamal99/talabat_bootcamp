from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="example_airflow_dag_ziad",
    start_date=datetime(2025, 5, 30),
    schedule_interval=None,  # or your preferred schedule
    catchup=False,
    tags=["trigger"]
) as dag:

    trigger = TriggerDagRunOperator(
        task_id="trigger_another_dag",
        trigger_dag_id="target_dag",  # The DAG you want to trigger
    )