from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)


API_URL = 'https://payments-table-728470529083.europe-west1.run.app'

bucket_name = "talabat-labs-payment-data"
file_name = "Mark_Eskander"

def get_response():

# get the response and turn it into json
    response = requests.get(API_URL)
    data = response.json()


# upload the response to gcs
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    gcs_hook.upload(
    bucket_name=bucket_name,
    object_name=file_name,
    data=json.dumps(data),
    mime_type='application/json'
)
default_args = {
    "retries": 1,
}
dag =  DAG(

    "Mark_Payments_table",
    schedule_interval=None,
    default_args = default_args,
    description="Transfer data from API to GCS and load into BigQuery",
    catchup=False,
    tags=["example"],
)
task = PythonOperator(
    task_id="get_response_save_to_gcs",
    python_callable=get_response,
    dag=dag)


load_to_bigquery = GCSToBigQueryOperator(
    task_id="load_to_bigquery",
    source_bucket=bucket_name,
    source_object=file_name,
    destination_project_dataset_table="talabat-labs-3927.landing.Mark_Eskander_Payments",
    autodetect=True,
    source_format="JSON",
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

task >> load_to_bigquery