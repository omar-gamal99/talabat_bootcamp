from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

# Constants (replace with your actual values)
CONNECTION_ID = "Postgress_Connect_Doha"
SQL_SELECT = "SELECT * FROM public.orders;"
BUCKET_NAME = "talabat_labs_postgress_to_gcs"
FILE_NAME = "Data_Doha.json"
BQ_DATASET = "talabat_labs_3927.landing"
BQ_TABLE = "DataTableDoha"

with DAG(
    dag_id="postgres_to_bigquery_pipeline_Doha",
    start_date=datetime(2025, 5, 23),
    schedule_interval="@once",
    catchup=False,
    description="Export from Postgres to GCS then load into BigQuery",
) as dag:

    # Step 1: Export from PostgreSQL to GCS
    postgres_to_gcs = PostgresToGCSOperator(
        task_id="postgres_to_gcs",
        postgres_conn_id=CONNECTION_ID,
        sql=SQL_SELECT,
        bucket=BUCKET_NAME,
        filename=FILE_NAME,
        export_format="json",
    )

    # Step 2: Load from GCS to BigQuery
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=[FILE_NAME],
        destination_project_dataset_table=f"{BQ_DATASET}.{BQ_TABLE}",
        skip_leading_rows=1,
        source_format="json",
        write_disposition="WRITE_TRUNCATE",  # or "WRITE_APPEND"
        autodetect=True,
    )

    postgres_to_gcs >> gcs_to_bigquery
