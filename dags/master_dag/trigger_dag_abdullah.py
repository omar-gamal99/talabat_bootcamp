from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    "retries": 1,
}

with DAG(
    dag_id="trigger_orders_db_transfer",
    default_args=default_args,
    description="Triggers the orders_db_transfer_abdullah_adel DAG",
    schedule_interval="@daily",  # or change as needed
    catchup=False,
    start_date=datetime(2025, 5, 30),
) as dag:

    trigger_orders_transfer_dag = TriggerDagRunOperator(
        task_id="trigger_orders_db_transfer_abdullah_adel",
        trigger_dag_id="orders_db_transfer_abdullah_adel",
        wait_for_completion=False,
        reset_dag_run=True,
    )
