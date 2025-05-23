from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

# Constants
BUCKET_NAME = 'talabat-labs-postgres-to-gcs'
GCS_FILE_PATH = 'data/postgres_export_{{ ds_nodash }}.json'
BQ_TABLE = 'talabat-labs-3927.landing.public.orders_john'
POSTGRES_CONN_ID = 'postgres_conn_john'

with DAG(
    dag_id='postgres_to_gcs_to_bigquery_john',
    schedule_interval=None,
    start_date=datetime(2025, 5, 23),
    catchup=False,
    tags=["talabat", "transfer"],
) as dag:

    # Step 1: Export from Postgres to GCS
    export_postgres_to_gcs = PostgresToGCSOperator(
        task_id='export_postgres_to_gcs_john',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='SELECT * FROM public.orders',
        bucket=BUCKET_NAME,
        filename=GCS_FILE_PATH,
        export_format='json',
    )

    # Step 2: Load from GCS to BigQuery
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq_john',
        bucket=BUCKET_NAME,
        source_objects=[GCS_FILE_PATH],
        destination_project_dataset_table=BQ_TABLE,
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True,
    )

    export_postgres_to_gcs >> load_gcs_to_bq


# new comment 1