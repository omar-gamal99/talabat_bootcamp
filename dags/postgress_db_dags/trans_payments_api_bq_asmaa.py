from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from datetime import datetime
import requests
import pandas as pd
import os

# CONFIG
API_URL = "https://payments-table-728470529083.europe-west1.run.app"
GCS_BUCKET_NAME = "talabat_labs_postgress_to_gcs"
GCS_OBJECT_NAME = "talabat_labs_postgress_to_gcs/payments_bq.csv"
BQ_DATASET = "talabat-labs-3927.landing"
BQ_TABLE = "source_api_payments_table_asmaa"
LOCAL_TMP_FILE = "/tmp/payments_asmaa.csv"

default_args = {
    'start_date': datetime(2025, 5, 28),
    'retries': 1,
}

with DAG(
    dag_id='api_to_bq_dag_asmaa',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:

    def extract_api_to_gcs():
        response = requests.get(API_URL)
        
        # Log response content to debug
        print("API raw response:", response.text[:500])  # log the first 500 chars
        
        try:
            data = response.json()  # Try to parse JSON
        except Exception as e:
            raise ValueError(f"API did not return valid JSON: {e}\nRaw content: {response.text[:500]}")

        # Convert JSON to DataFrame
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame([data])
        else:
            raise ValueError("Unexpected JSON format from API")

        # Save to CSV and upload
        df.to_csv(LOCAL_TMP_FILE, index=False)

        gcs_hook = GCSHook()
        gcs_hook.upload(
            bucket_name=GCS_BUCKET_NAME,
            object_name=GCS_OBJECT_NAME,
            filename=LOCAL_TMP_FILE
        )

        os.remove(LOCAL_TMP_FILE)

    extract_task = PythonOperator(
        task_id='extract_api_to_gcs_asmaa',
        python_callable=extract_api_to_gcs
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq_asmaa',
        bucket=GCS_BUCKET_NAME,
        source_objects=[GCS_OBJECT_NAME],
        destination_project_dataset_table=f"{BQ_DATASET}.{BQ_TABLE}",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        source_format="CSV",
        skip_leading_rows=1
    )

    extract_task >> load_to_bq
