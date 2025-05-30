from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='trigger_target_dag_MoEssam',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Set to a schedule if needed
    catchup=False,
    tags=['example'],
) as dag:

    trigger = TriggerDagRunOperator(
        task_id='trigger_target_dag_task_MoEssam',
        trigger_dag_id='pg_to_bq_via_gcs_moessam',  # Name of the DAG you want to trigger
        wait_for_completion=True,    # Set to True if you want to wait for the triggered DAG to finish
    )
