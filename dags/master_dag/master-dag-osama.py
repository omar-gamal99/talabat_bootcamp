from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='trigger_anotherdag_osama',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    trigger_dag_b = TriggerDagRunOperator(
        task_id='trigger_dag_b',
        trigger_dag_id='postgres_to_gcs_to_bigquery', 
        wait_for_completion=False  
    )
