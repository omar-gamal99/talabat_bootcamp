from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'airflow',
}

SOURCE_TABLE_NAME = 'orders'
FILE_FORMAT = 'csv'
GCS_BUCKET = 'talabat-labs-postgres-to-gcs'
GCS_OBJECT_PATH = f'postgres-export/{SOURCE_TABLE_NAME}.{FILE_FORMAT}'
BQ_DATASET = 'talabat-labs-3927.landing'
BQ_TABLE = 'mariam_orders'

with DAG(
    dag_id='postgres_to_bq_mariam_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['postgres', 'gcs', 'bigquery'],
) as dag:

    # Task 1: Export from Postgres to GCS
    export_to_gcs = PostgresToGCSOperator(
        task_id='export_postgres_to_gcs',
        postgres_conn_id='postgres_mariam',
        sql=f'SELECT * FROM {SOURCE_TABLE_NAME};',
        bucket=GCS_BUCKET,
        filename=GCS_OBJECT_PATH,
        export_format=FILE_FORMAT,
        gzip=False,
        use_server_side_cursor=False,
    )

    # Task 2: Load from GCS to BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        bucket=GCS_BUCKET,
        source_objects=[GCS_OBJECT_PATH],
        destination_project_dataset_table=f'{BQ_DATASET}.{BQ_TABLE}',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',  # Overwrite table if exists
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )

    export_to_gcs >> load_to_bigquery
