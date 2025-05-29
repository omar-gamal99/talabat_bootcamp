from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime
import requests
import json
import re

API_URL = 'https://payments-table-728470529083.europe-west1.run.app'
GCS_BUCKET = 'talabat-labs-payment-data'
GCS_FILENAME = 'payments/{{ ds_nodash }}.json'
BQ_TABLE = 'talabat-labs-3927.payments.payments_youmna'

def generate_filename(**context):
    filename = f"payments/{context['ds_nodash']}.json"
    return filename

def fetch_data_and_upload_to_gcs(**context):
    response = requests.get(API_URL)
    response.raise_for_status()

    raw_text = response.text.strip()


    try:
        # Extract JSON objects using regex if not wrapped in array or NDJSON
        json_objects = re.findall(r'\{.*?\}(?=\{|\Z)', raw_text.replace('\n', ''))
        parsed = [json.loads(obj) for obj in json_objects]
    except json.JSONDecodeError:
        raise ValueError("The API response is not in valid JSON format")

    # Convert to NDJSON format
    data_string = '\n'.join([json.dumps(obj) for obj in parsed])

    gcs_hook = GCSHook()
    gcs_hook.upload(
        bucket_name=GCS_BUCKET.strip(),
        object_name=context['ti'].xcom_pull(task_ids='generate_filename'),
        data=data_string,
        mime_type='application/json'
    )

with DAG(
    dag_id='api_to_bigquery_payments_youmna',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    generate_filename_task = PythonOperator(
        task_id='generate_filename',
        python_callable=generate_filename
    )

    fetch_and_upload = PythonOperator(
        task_id='fetch_data_and_upload_to_gcs',
        python_callable=fetch_data_and_upload_to_gcs,
        provide_context=True
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=GCS_BUCKET,
        source_objects=["{{ ti.xcom_pull(task_ids='generate_filename') }}"],
        destination_project_dataset_table=BQ_TABLE,
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',  # or WRITE_APPEND
        autodetect=True,
    )

    generate_filename_task >> fetch_and_upload >> load_to_bq
