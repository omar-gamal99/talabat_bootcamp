from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime
import requests

API_URL = 'https://payments-table-728470529083.europe-west1.run.app'
GCS_BUCKET = 'talabat-labs-payment-data'
BQ_TABLE = 'talabat-labs-3927.payments.payments_youmna'

def get_response(**context):
    try:
        # Get data from API
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise error for bad status codes
        data = response.text
        
        # Generate filename with date
        filename = f"payments_{context['ds_nodash']}.csv"
        
        # Upload to GCS
        gcs_hook = GCSHook()
        gcs_hook.upload(
            bucket_name=GCS_BUCKET,
            object_name=filename,
            data=data,
            mime_type='text/csv'
        )
        
        return filename  # Return filename for downstream tasks
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

default_args = {
    "retries": 1,
}

with DAG(
    dag_id='api_to_bigquery_payments_youmna',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=["payments"],
) as dag:

    fetch_and_upload = PythonOperator(
        task_id='fetch_and_upload',
        python_callable=get_response,
        provide_context=True
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=GCS_BUCKET,
        source_objects=["{{ ti.xcom_pull(task_ids='fetch_and_upload') }}"],
        destination_project_dataset_table=BQ_TABLE,
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    fetch_and_upload >> load_to_bq