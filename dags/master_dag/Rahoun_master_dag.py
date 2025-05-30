from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

# Define the master DAG
with DAG(
    dag_id="master_trigger_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Run manually or trigger externally
    catchup=False,
    description="Master DAG to trigger customer, orders, and product DAGs",
) as dag:

    # Trigger customer_db_extract
    trigger_customer = TriggerDagRunOperator(
        task_id="trigger_customer_dag",
        trigger_dag_id="customer_db_extract",  # Must match the actual DAG ID
        wait_for_completion=False
    )

    # Trigger orders_db_extract
    trigger_orders = TriggerDagRunOperator(
        task_id="trigger_orders_dag",
        trigger_dag_id="orders_db_extract",
        wait_for_completion=False
    )

    # Trigger product_db_extract
    trigger_product = TriggerDagRunOperator(
        task_id="trigger_product_dag",
        trigger_dag_id="product_db_extract",
        wait_for_completion=False
    )

    # Run all 3 in parallel
    [trigger_customer, trigger_orders, trigger_product]
