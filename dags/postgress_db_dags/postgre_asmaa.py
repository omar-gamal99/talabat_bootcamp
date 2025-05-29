from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

# Constants 
CONNECTION_ID = "db_postgres_asmaa"
SQL_SELECT = "SELECT * FROM public.orders;"
BUCKET_NAME = "talabat-labs-postgres-to-gcs"
FILE_NAME = "orders_asmaa.json"
BQ_DATASET = "talabat-labs-3927.landing"
BQ_TABLE = "bq_orders_asmaa"

with DAG(
    dag_id="postgres_to_bigquery_pipeline",
    start_date=datetime(2025, 5, 28),
    schedule_interval="@once",
    catchup=False,
    description="Export from Postgres to GCS then load into BigQuery",
) as dag:

    # Step 1: Export from PostgreSQL to GCS
    postgres_to_gcs = PostgresToGCSOperator(
        task_id="postgres_to_gcs_asmaa",
        postgres_conn_id=CONNECTION_ID,
        sql=SQL_SELECT,
        bucket=BUCKET_NAME,
        filename=FILE_NAME,
        export_format="NEWLINE_DELIMITED_JSON",
    )

    # Step 2: Load from GCS to BigQuery
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_asmaa",
        bucket=BUCKET_NAME,
        source_objects=[FILE_NAME],
        destination_project_dataset_table=f"{BQ_DATASET}.{BQ_TABLE}",
        skip_leading_rows=1,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",  
        autodetect=True,
    )

    postgres_to_gcs >> gcs_to_bigquery