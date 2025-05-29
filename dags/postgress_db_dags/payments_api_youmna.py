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
BQ_TABLE = 'talabat-labs-3927.payments.payments_youmna'

def generate_filename(**context):
    filename = f"payments/{context['ds_nodash']}.json"
    return filename

def fetch_data_and_upload_to_gcs(**context):
    # Get the filename from XCom
    filename = context['ti'].xcom_pull(task_ids='generate_filename')
    if not filename:
        raise ValueError("Filename not found in XCom")
    
    response = requests.get(API_URL)
    response.raise_for_status()

    raw_text = response.text.strip()
    
    # Try to parse as JSON array first
    try:
        parsed = json.loads(raw_text)
        if not isinstance(parsed, list):
            parsed = [parsed]
    except json.JSONDecodeError:
        # If not a JSON array, try NDJSON or individual objects
        try:
            # Split by newlines and parse each line
            lines = raw_text.split('\n')
            parsed = [json.loads(line) for line in lines if line.strip()]
        except json.JSONDecodeError:
            # As last resort, try regex extraction
            try:
                json_objects = re.findall(r'\{.*?\}(?=\s*\{|\s*\Z)', raw_text, re.DOTALL)
                parsed = [json.loads(obj) for obj in json_objects]
            except:
                raise ValueError("The API response is not in valid JSON format")

    # Convert to NDJSON format
    if not parsed:
        raise ValueError("No valid JSON data found in API response")
    
    data_string = '\n'.join([json.dumps(obj) for obj in parsed])

    gcs_hook = GCSHook()
    gcs_hook.upload(
        bucket_name=GCS_BUCKET.strip(),
        object_name=filename,
        data=data_string.encode('utf-8'),  # Ensure data is bytes
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
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    generate_filename_task >> fetch_and_upload >> load_to_bq
