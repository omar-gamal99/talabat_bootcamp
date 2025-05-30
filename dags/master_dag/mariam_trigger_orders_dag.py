from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='mariam_trigger_orders_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['trigger', 'orders'],
) as dag:

    trigger_orders_pipeline = TriggerDagRunOperator(
        task_id='trigger_orders_pipeline_dag',
        trigger_dag_id='postgres_to_bq_mariam_pipeline',  # This is the DAG ID to trigger
        wait_for_completion=False,  # Set to True if you want this DAG to wait until the target DAG finishes
    )
