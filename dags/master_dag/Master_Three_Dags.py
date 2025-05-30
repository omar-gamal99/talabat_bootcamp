from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='Master_trigger_all_extracts',
    start_date=datetime(2024, 10, 10),
    schedule_interval=None,
    catchup=False,
    tags=['controller']
) as dag:

    trigger_customers = TriggerDagRunOperator(
        task_id='trigger_customers_db_extract',
        trigger_dag_id='customers_db_extract',
        wait_for_completion=False
    )

    trigger_orders = TriggerDagRunOperator(
        task_id='trigger_orders_db_extract',
        trigger_dag_id='orders_db_extract',
        wait_for_completion=False
    )

    trigger_products = TriggerDagRunOperator(
        task_id='trigger_products_db_extract',
        trigger_dag_id='products_db_extract',
        wait_for_completion=False
    )

    # Run all three in parallel
    [trigger_customers, trigger_orders, trigger_products]
