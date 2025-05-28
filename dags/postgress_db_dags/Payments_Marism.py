from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
import requests
import pandas as pd
import os


API_URL = "https://payments-table-728470529083.europe-west1.run.app"

GCS_BUCKET = 'talabat-labs-postgres-to-gcs'
GCS_FILENAME = 'api-export/payments.csv'
BQ_DATASET = 'talabat-labs-3927.payments'
BQ_TABLE = 'mariam_payments'

def fetch_and_upload_to_gcs():
    # --- Step 1: Fetch data from API ---
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()

    # --- Step 2: Save to CSV locally ---
    df = pd.DataFrame(data)
    local_file = '/tmp/payments.csv'
    df.to_csv(local_file, index=False)

    # --- Step 3: Upload CSV to GCS ---
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(GCS_FILENAME)
    blob.upload_from_filename(local_file)
    print(f"Uploaded {local_file} to gs://{GCS_BUCKET}/{GCS_FILENAME}")

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='api_to_bq_mariam_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['api', 'gcs', 'bigquery'],
) as dag:

    fetch_upload = PythonOperator(
        task_id='fetch_api_and_upload_gcs',
        python_callable=fetch_and_upload_to_gcs,
    )

    load_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        bucket=GCS_BUCKET,
        source_objects=[GCS_FILENAME],
        destination_project_dataset_table=f'{BQ_DATASET}.{BQ_TABLE}',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )

    fetch_upload >> load_bq
