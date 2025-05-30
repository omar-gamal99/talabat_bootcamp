from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    "retries": 1,
}

with DAG(
    dag_id="master_db_extract_trigger_abdullah",
    default_args=default_args,
    description="Master DAG to trigger customers, orders, and products DB extract DAGs",
    schedule_interval="@daily",  # or change to None if manually triggered
    catchup=False,
    start_date=datetime(2025, 5, 30),
) as dag:

    trigger_customers = TriggerDagRunOperator(
        task_id="trigger_customers_db_extract",
        trigger_dag_id="customers_db_extract",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    trigger_orders = TriggerDagRunOperator(
        task_id="trigger_orders_db_extract",
        trigger_dag_id="orders_db_extract",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    trigger_products = TriggerDagRunOperator(
        task_id="trigger_products_db_extract",
        trigger_dag_id="products_db_extract",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Run all three in parallel
    [trigger_customers, trigger_orders, trigger_products]
