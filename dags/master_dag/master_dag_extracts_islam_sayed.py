from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="master_trigger_extracts_islam_sayed",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as master_dag:

    trigger_products = TriggerDagRunOperator(
        task_id="trigger_products_extract",
        trigger_dag_id="products_db_extract",
        wait_for_completion=True,
        deferrable=True
    )

    trigger_customers = TriggerDagRunOperator(
        task_id="trigger_customers_extract",
        trigger_dag_id="customers_db_extract",
        wait_for_completion=True,
        deferrable=True
    )

    trigger_orders = TriggerDagRunOperator(
        task_id="trigger_orders_extract",
        trigger_dag_id="orders_db_extract",
        wait_for_completion=True,
        deferrable=True
    )

    # Trigger all in parallel, while sleeping (deferred) during wait
    [trigger_products, trigger_customers, trigger_orders]
