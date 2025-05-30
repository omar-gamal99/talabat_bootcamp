from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="master_dag",
    start_date=datetime(2025, 5, 30),
    schedule_interval="@daily",  # or your preferred schedule
    catchup=False,
    tags=["controller"],
) as dag:

    trigger = TriggerDagRunOperator(
        task_id="trigger_child_dag",
        trigger_dag_id="postgres_to_bigquery_pipeline_Doha",  # The DAG ID of the child DAG
        wait_for_completion=True,    # If you want to wait until the child DAG finishes
        reset_dag_run=True,          # If you want to overwrite any existing runs
        poke_interval=60             # Check every 60s if waiting
    )