from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

with DAG(
    dag_id="master_dag_yousef36kk",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["master_dag_yousef36"],
) as dag:

    trigger_ingest = TriggerDagRunOperator(
        task_id="trigger_ingest_dag_take_data_from_postgres_to_bq",
        trigger_dag_id="simple_task_to_transfer_from_postgres_to_bigquery",
        wait_for_completion=True,  # Waits for the child to finish
        poke_interval=60,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    

    trigger_ingest 
