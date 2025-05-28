import requests
import pandas as pd
import io
from datetime import timedelta, datetime
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

POSTGRES_CONN_ID = "postgress-conn-aya-hany"
API_URL = "https://payments-table-728470529083.europe-west1.run.app"
GCS_BUCKET = "talabat-labs-postgres-to-gcs"
GCS_OBJECT = 'payments-ayahany.csv'
BIGQUERY_DATASET = "talabat-labs-3927.landing"
BIGQUERY_TABLE = "payments-ayahany"

## Define the default arguments for the DAG
default_args = {
    'owner': 'ayah',
    'start_date': datetime(2025, 5, 23),
    'retries': 1,  
    'retry_delay': timedelta(minutes=2), 
}

def api_data_to_gcs():
    response = requests.get(API_URL)
    # if response.status_code == 200:
    #     df = pd.read_csv(io.StringIO(response.text)) 
    # else:
    #     print(f"Error: {response.status_code} - {response.text}")
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(GCS_OBJECT)
    blob.upload_from_string(response.text, content_type='text/csv')

with DAG(
    "API-GCS-BQ-ayahany",
    default_args=default_args,
    description='Airflow DAG for API to BigQuery ETL',
) as api_bq_etl :
    api_data_to_gcs_task = PythonOperator(
        task_id='api_data_to_gcs',
        python_callable= api_data_to_gcs
    )
    gcs_to_bq_task = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket= GCS_BUCKET,
        source_objects=[GCS_OBJECT],
        source_format="CSV",
        destination_project_dataset_table=f"{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
        write_disposition="WRITE_TRUNCATE", 
    )

api_data_to_gcs_task >> gcs_to_bq_task