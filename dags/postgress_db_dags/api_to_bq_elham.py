import json
import requests

from airflow import DAG
from datetime import datetime, timedelta
from google.cloud import storage
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'elham',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_from_api_and_load_to_gcs(**context):
    api_url= 'https://payments-table-728470529083.europe-west1.run.app/'
    response= requests.get(api_url)
    data= response.json()

    # converting data to JSON string
    json_data=json.dumps(data)

    #uploading data to GCS
    bucket_name='talabat-labs-payment-data'
    blob_path='data/payments_data_api_elham.json'
    
    client= storage.Client()
    bucket= client.bucket(bucket_name)
    blob= bucket.blob(blob_path)

    blob.uploading_data(
        data=json_data,
        content_type='application/json'
    )

    context['ti'].xcom_push(key='gcs_blob_path', value=blob_path)

with DAG(
    dag_id='transfer_from_api_to_bq_elham',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 5, 28),
    catchup=False,
    description='Transfering payments data from API to BigQuery using GCS as an intermediate step'
) as dag:
    extract_upload_task= PythonOperator(
        task_id= 'extracting_data_and_upload_to_gcs',
        python_callable= extract_from_api_and_load_to_gcs,
        provide_context=True
    )

    load_to_bigquery_task= GCSToBigQueryOperator(
        task_id= 'load_to_bigquery',
        bucket= 'talabat-labs-payment-data',
        source_objects=['data/payments_data_api_elham.json'],
        destination_project_dataset_table='talabat-labs-3927.payments.payments-elham',
        source_format='newline_delimited_json',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True
    )

    extract_upload_task >> load_to_bigquery_task