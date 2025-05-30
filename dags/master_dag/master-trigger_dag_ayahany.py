from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

POSTGRES_CONN_ID = "postgress-conn-aya-hany"
## Define the default arguments for the DAG
default_args = {
    'owner': 'ayah',
    'start_date': datetime(2025, 5, 23),
    'retries': 1,  
    'retry_delay': timedelta(minutes=2), 
}

with DAG(
    dag_id="trigger-dag",
) as dag:

    trigger = TriggerDagRunOperator(
        task_id="TRIGGER-Postgress-GCS-BQ-ayahany",
        trigger_dag_id="Postgress-GCS-BQ-ayahany",  
)

    trigger