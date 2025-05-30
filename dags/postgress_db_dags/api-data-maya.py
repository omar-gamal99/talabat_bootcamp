from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from google.cloud import storage
import requests
import pandas as pd
from io import StringIO

# Configuration
API_URL = "https://payments-table-728470529083.europe-west1.run.app"
GCS_BUCKET = 'talabat-labs-postgres-to-gcs'
GCS_FILENAME = 'api-export/payments.csv'
BQ_TABLE = 'talabat-labs-3927.payments.maya-api-payments'

def fetch_and_upload_to_gcs():
    # Fetch CSV from API
    response = requests.get(API_URL)
    response.raise_for_status()

    # Read into DataFrame
    df = pd.read_csv(StringIO(response.text))

    # Save locally then upload to GCS
    local_path = '/tmp/payments.csv'
    df.to_csv(local_path, index=False)

    # Upload to GCS using GCP native credentials
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(GCS_FILENAME)
    blob.upload_from_filename(local_path)

    print(f"Uploaded file to: gs://{GCS_BUCKET}/{GCS_FILENAME}")

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='api_to_bq_gcp_maya',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['api', 'gcs', 'bigquery'],
) as dag:

    fetch_and_upload = PythonOperator(
        task_id='fetch_api_and_upload_to_gcs',
        python_callable=fetch_and_upload_to_gcs,
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_csv_from_gcs_to_bq_maya',
        bucket=GCS_BUCKET,
        source_objects=[GCS_FILENAME],
        destination_project_dataset_table=BQ_TABLE,
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud_default',  # Works out of the box in GCP
    )

    fetch_and_upload >> load_to_bq
