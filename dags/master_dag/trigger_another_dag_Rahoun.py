from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="Rahoun_trigger_ziad_transfer_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    trigger_ziad_dag = TriggerDagRunOperator(
        task_id="trigger_orders_db_transfer_ziad",
        trigger_dag_id="orders_db_transfer_ziad",  # DAG B ID from ziad_transfer_dag.py
        wait_for_completion=False,  # Set True if you want to wait for B to finish
        reset_dag_run=True,
    )
