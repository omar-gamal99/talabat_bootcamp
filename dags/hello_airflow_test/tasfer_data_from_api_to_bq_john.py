from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime
import requests

def fetch_and_upload_to_gcs(**context):
    url = "https://payments-table-728470529083.europe-west1.run.app"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data. Status: {response.status_code}")
    
    file_content = response.content
    filename = f"payments_data_john/{context['ds_nodash']}.csv"

    hook = GCSHook(gcp_conn_id='google_cloud_default')
    hook.upload(
        bucket_name='talabat-labs-postgres-to-gcs',
        object_name=filename,
        data=file_content,
        mime_type='text/csv'
    )
    context['ti'].xcom_push(key='gcs_file', value=filename)

with DAG(
    dag_id="api_to_bigquery_john",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["api", "gcs", "bigquery"]
) as dag:

    fetch_csv_to_gcs = PythonOperator(
        task_id='fetch_csv_and_upload_john',
        python_callable=fetch_and_upload_to_gcs,
        provide_context=True
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_api_csv_to_bq_john',
        bucket='talabat-labs-postgres-to-gcs',
        source_objects=["{{ task_instance.xcom_pull(task_ids='fetch_csv_and_upload', key='gcs_file') }}"],
        destination_project_dataset_table='talabat-labs-3927.landing.payments_john',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        autodetect=True
    )

    fetch_csv_to_gcs >> load_to_bq
