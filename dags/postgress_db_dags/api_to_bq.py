from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
import requests
import pandas as pd
from io import StringIO
import os

# Constants
API_URL = "https://payments-table-728470529083.europe-west1.run.app"
GCS_BUCKET = 'talabat-labs-payment-data'
GCS_FILENAME = 'payments_Amir.csv'
LOCAL_TMP_FILE = '/tmp/payments.csv'
BQ_PROJECT = 'talabat-labs-3927'
BQ_DATASET = 'payments'
BQ_TABLE = 'payments_Amir'

def download_payments_data_and_upload_to_gcs():

    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    
    df = pd.read_csv(StringIO(response.text))
    df.to_csv(LOCAL_TMP_FILE, index=False)

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(GCS_FILENAME)
    blob.upload_from_filename(LOCAL_TMP_FILE)

    print(f"Uploaded file to gs://{GCS_BUCKET}/{GCS_FILENAME}")

    if os.path.exists(LOCAL_TMP_FILE):
        os.remove(LOCAL_TMP_FILE)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='payments_api_to_bigquery_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    description='Ingests payment data from external API into BigQuery via GCS',
) as dag:

    extract_payments_data = PythonOperator(
        task_id='extract_payments_from_api_and_stage_to_gcs',
        python_callable=download_payments_data_and_upload_to_gcs,
    )

    load_payments_to_bq = GCSToBigQueryOperator(
        task_id='load_staged_payments_data_into_bigquery',
        bucket=GCS_BUCKET,
        source_objects=[GCS_FILENAME],
        destination_project_dataset_table=f'{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )

    extract_payments_data >> load_payments_to_bq
