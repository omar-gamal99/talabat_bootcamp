from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='mariam_trigger_3_db_extracts_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['trigger', 'batch_extracts'],
) as dag:

    trigger_products = TriggerDagRunOperator(
        task_id='trigger_products_db_extract',
        trigger_dag_id='products_db_extract',
        wait_for_completion=False,
    )

    trigger_customers = TriggerDagRunOperator(
        task_id='trigger_customers_db_extract',
        trigger_dag_id='customers_db_extract',
        wait_for_completion=False,
    )

    trigger_orders = TriggerDagRunOperator(
        task_id='trigger_orders_db_extract',
        trigger_dag_id='orders_db_extract',
        wait_for_completion=False,
    )

    # Run all triggers in parallel
    [trigger_products, trigger_customers, trigger_orders]
