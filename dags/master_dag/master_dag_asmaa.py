from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="trigger_postgres_to_bq_asmaa",
    start_date=datetime(2025, 5, 30),
    schedule_interval="@once",  # You can change this to fit your use case
    catchup=False,
    description="Trigger the postgres_to_bq_asmaa DAG",
    tags=["trigger", "postgres", "bigquery"],
) as trigger_dag:

    trigger_dag_task = TriggerDagRunOperator(
        task_id="trigger_postgres_to_bq_asmaa_task",
        trigger_dag_id="postgres_to_bq_asmaa",  # Must match the target DAG's dag_id
    )
