from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.operators.gcs import GCSUploadFileOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'postgres_to_bigquery_orders',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

# POSTGRES_CONN_ID = 'your_postgres_conn_id'
# GCP_CONN_ID = 'your_gcp_conn_id'
GCS_BUCKET = 'talabat-labs-postgres-to-gcs'
GCS_PATH = 'orders-abdullah-adel/orders.csv'
BQ_DATASET = 'talabat-labs-3927.landing'
BQ_TABLE = 'public.orders'

def extract_orders():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df = hook.get_pandas_df(sql='SELECT * FROM orders')
    df.to_csv('/tmp/orders.csv', index=False)

extract_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders,
    dag=dag,
)

upload_task = GCSUploadFileOperator(
    task_id='upload_to_gcs',
    bucket_name=GCS_BUCKET,
    object_name=GCS_PATH,
    filename='/tmp/public.orders.csv',
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

load_to_bq_task = BigQueryInsertJobOperator(
    task_id='load_to_bigquery',
    configuration={
        "load": {
            "sourceUris": [f"gs://{GCS_BUCKET}/{GCS_PATH}"],
            "destinationTable": {
                "projectId": "your-gcp-project",
                "datasetId": BQ_DATASET,
                "tableId": BQ_TABLE,
            },
            "sourceFormat": "CSV",
            "skipLeadingRows": 1,
            "writeDisposition": "WRITE_TRUNCATE",
            "autodetect": True,
        }
    },
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

extract_task >> upload_task >> load_to_bq_task
