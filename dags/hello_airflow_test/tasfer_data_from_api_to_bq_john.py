from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from datetime import datetime
import requests
import pandas as pd
import os

BUCKET_NAME = 'talabat-labs-postgres-to-gcs'
FILENAME = 'payments_john.csv'
GCS_PATH = f'data/{FILENAME}'
GCS_URI = f'gs://{BUCKET_NAME}/{GCS_PATH}'
API_URL = 'https://your.api.url/endpoint'  # replace with your actual API

default_args = {
    'start_date': datetime(2025, 5, 23),
    'retries': 1,
}

with DAG(
    'api_to_bigquery_john',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    def extract_api_data(**kwargs):
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)

        local_path = f'/tmp/{FILENAME}'
        df.to_csv(local_path, index=False)

        # Push the path to XCom
        kwargs['ti'].xcom_push(key='local_path', value=local_path)
        kwargs['ti'].xcom_push(key='gcs_path', value=GCS_PATH)

    def upload_to_gcs(**kwargs):
        local_path = kwargs['ti'].xcom_pull(key='local_path')
        gcs_path = kwargs['ti'].xcom_pull(key='gcs_path')
        hook = GCSHook(gcp_conn_id='google_cloud_default')
        hook.upload(bucket_name=BUCKET_NAME, object_name=gcs_path, filename=local_path)

    extract_task = PythonOperator(
        task_id='extract_api_data',
        python_callable=extract_api_data,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        provide_context=True,
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

