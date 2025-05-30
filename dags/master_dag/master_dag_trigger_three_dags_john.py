from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

TARGET_DAG_IDS = [
    "customer_db_extract",
    "orders_db_extract",
    "products_db_extract"
]

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='master_db_extract_trigger_dag_john',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['master_trigger', 'db_extracts'],
    doc_md="""
    ### Master DAG for Triggering Database Extract DAGs
    This DAG triggers the following DAGs:
    - customer_db_extract
    - orders_db_extract
    - products_db_extract
    """
) as dag:

    # Loop through the target DAG IDs and create a trigger task for each
    for target_dag_id in TARGET_DAG_IDS:
        trigger_task = TriggerDagRunOperator(
            task_id=f'trigger_{target_dag_id}_task',
            trigger_dag_id=target_dag_id,
        )