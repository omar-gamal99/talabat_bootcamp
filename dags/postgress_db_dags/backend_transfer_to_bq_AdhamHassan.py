from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

# === CONFIGURATION ===
BQ_DS = 'talabat-labs-3927.landing'  # Replace with your BigQuery dataset
BQ_PROJECT = 'my-project'  # Replace with your GCP project ID
GCS_BUCKET = 'talabat-labs-postgres-to-gcs'  # Replace with your GCS bucket name
GCS_OBJECT_PATH = 'postgres-test'  # Folder path in GCS to save the CSV
SOURCE_TABLE_NAME = 'orders'  # PostgreSQL table to export
POSTGRES_CONNECTION_ID = 'postgresql-bq-connection-Adham'  # Airflow connection ID for PostgreSQL
FILE_FORMAT = 'csv'  # Export file format

# === DAG DEFINITION ===
with DAG(
    dag_id='load_postgres_into_bq',
    start_date=days_ago(1),
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='0 9 * * *',
    max_active_runs=1,
    catchup=False,
) as dag:

    # Export data from PostgreSQL to GCS
    postgres_to_gcs_task = PostgresToGCSOperator(
        task_id='postgres_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql=f'SELECT * FROM {SOURCE_TABLE_NAME};',
        bucket=GCS_BUCKET,
        filename=f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME}.{FILE_FORMAT}',
        export_format='csv',
        gzip=False,
        use_server_side_cursor=False,
    )

    # Load data from GCS to BigQuery with autodetected schema
    gcs_to_bq_task = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=GCS_BUCKET,
        source_objects=[f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME}.csv'],
        destination_project_dataset_table=f'{BQ_PROJECT}.{BQ_DS}.{SOURCE_TABLE_NAME}',
        autodetect=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    # Clean up temporary file in GCS
    cleanup_task = GCSDeleteObjectsOperator(
        task_id='cleanup',
        bucket_name=GCS_BUCKET,
        objects=[f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME}.csv'],
    )

    # Task execution order
    postgres_to_gcs_task >> gcs_to_bq_task >> cleanup_task
