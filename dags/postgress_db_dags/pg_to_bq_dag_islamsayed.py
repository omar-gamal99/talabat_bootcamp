from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
import pandas as pd

from airflow.operators.python import PythonOperator
import os

default_args = {
    'start_date': days_ago(1),
}

BUCKET_NAME = 'talabat-labs-postgres-to-gcs'
BQ_DATASET = 'talabat-labs-3927.landing'
BQ_TABLE = 'transfered_from_postgres_to_bq_islamsayed'

def extract_postgres_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_connection_islamsayed')
    df = pg_hook.get_pandas_df(sql='SELECT * FROM public.orders')  # customize query
    file_path = '/tmp/postgres_data.csv'
    df.to_csv(file_path, index=False)
    kwargs['ti'].xcom_push(key='file_path', value=file_path)

with DAG('postgres_to_bigquery',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    extract_task = PythonOperator(
        task_id='extract_postgres_data',
        python_callable=extract_postgres_data,
        provide_context=True
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src="{{ ti.xcom_pull(task_ids='extract_postgres_data', key='file_path') }}",
        dst='postgres_data/postgres_data.csv',
        bucket=BUCKET_NAME,
        mime_type='text/csv'
    )

    load_to_bq = BigQueryInsertJobOperator(
        task_id='load_to_bq',
        configuration={
            "load": {
                "sourceUris": [f"gs://{BUCKET_NAME}/postgres_data/postgres_data.csv"],
                "destinationTable": {
                    "projectId": "{{ var.value.gcp_project_id }}",
                    "datasetId": BQ_DATASET,
                    "tableId": BQ_TABLE,
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "autodetect": True,
                "writeDisposition": "WRITE_TRUNCATE"
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    extract_task >> upload_to_gcs >> load_to_bq
