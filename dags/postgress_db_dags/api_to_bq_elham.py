import requests
from io import BytesIO
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
    
    if response.status_code != 200:
        raise Exception(f'Failed to fetch data from API: {response.status_code}')

    csv_data= response.content

    #uploading data to GCS
    bucket_name='talabat-labs-payment-data'
    blob_path='data/payments_data_api_elham.csv'
    
    client= storage.Client()
    bucket= client.bucket(bucket_name)
    blob= bucket.blob(blob_path)

    blob.upload_from_file(BytesIO(csv_data), content_type='text/csv')

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
        source_objects=['data/payments_data_api_elham.csv'],
        destination_project_dataset_table='talabat-labs-3927.payments.payments-elham',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True
    )

    extract_upload_task >> load_to_bigquery_task