from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="parent_dag_amir",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    trigger_child = TriggerDagRunOperator(
        task_id="trigger_amir",
        trigger_dag_id="orders_db_transfer_amir",  # Name of the DAG to trigger
        wait_for_completion=False,  # Set True if you want to wait for child DAG to finish
        reset_dag_run=True,  # Set True to clear the previous run with same execution_date
    )
