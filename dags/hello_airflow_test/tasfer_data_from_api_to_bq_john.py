from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from datetime import datetime
import requests
import pandas as pd
import os

# Configuration
BUCKET_NAME = 'talabat-labs-postgres-to-gcs'
FILENAME = 'payments_john.csv'
GCS_PATH = f'data/{FILENAME}'
API_URL = 'https://payments-table-728470529083.europe-west1.run.app'

default_args = {
    'start_date': datetime(2025, 5, 23),
    'retries': 0,
}

def extract_api_data(**kwargs):
    """Extract CSV data from API"""
    response = requests.get(API_URL)
    response.raise_for_status()
    
    local_path = f'/tmp/{FILENAME}'
    with open(local_path, 'w', encoding='utf-8') as f:
        f.write(response.text)
    
    # Just check record count
    df = pd.read_csv(local_path)
    print(f"Extracted {len(df)} records")
    
    kwargs['ti'].xcom_push(key='local_path', value=local_path)

def upload_to_gcs(**kwargs):
    """Upload CSV to GCS"""
    local_path = kwargs['ti'].xcom_pull(key='local_path')
    
    hook = GCSHook(gcp_conn_id='google_cloud_default')
    hook.upload(
        bucket_name=BUCKET_NAME, 
        object_name=GCS_PATH, 
        filename=local_path
    )
    
    os.remove(local_path)
    print("Uploaded to GCS successfully")

with DAG(
    'api_to_bigquery_john',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_api_data',
        python_callable=extract_api_data,
    )

    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
    )

    load_task = GCSToBigQueryOperator(
        task_id='load_api_csv_to_bq_john',
        bucket=BUCKET_NAME,
        source_objects=[GCS_PATH],
        destination_project_dataset_table='talabat-labs-3927.landing.payments_john',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )

    extract_task >> upload_task >> load_task