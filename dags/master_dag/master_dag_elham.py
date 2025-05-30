from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'elham',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='master_dag_elham',
    start_date=datetime(2025, 5, 28),
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:
    trigger = TriggerDagRunOperator(
        task_id='triggering_trasfer_api_dag_elham',
        trigger_dag_id='transfer_from_api_to_bq_elham',
        # task_id='triggering_transfer_postgres_to_bq_dag_elham',
        # trigger_dag_id='transfer_from_postgres_to_bq_elham',
        wait_for_completion=True
    )

    trigger

