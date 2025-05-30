from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

POSTGRES_CONN_ID = "postgress-conn-abdullah-adel"
GCS_BUCKET_NAME = "talabat-labs-postgres-to-gcs"
GCS_FILE_NAME = "abdullah"
BQ_DATASET_TABLE = "talabat-labs-3927.landing.abdullah-orders"

default_args = {
    "retries": 1,
}

with DAG(
    dag_id="orders_db_transfer_abdullah",
    default_args=default_args,
    description="Extract orders data from PostgreSQL, store in GCS, and load into BigQuery",
    schedule_interval="@daily",
    catchup=False,
    start_date=datetime(2025, 5, 30), 
) as dag:

    export_to_gcs = PostgresToGCSOperator(
        task_id="export_orders_to_gcs",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="SELECT * FROM orders",
        bucket=GCS_BUCKET_NAME,
        filename=GCS_FILE_NAME,
        export_format="json",
    )

    import_to_bigquery = GCSToBigQueryOperator(
        task_id="import_orders_to_bigquery",
        bucket=GCS_BUCKET_NAME,
        source_objects=[GCS_FILE_NAME],
        destination_project_dataset_table=BQ_DATASET_TABLE,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
    )

    export_to_gcs >> import_to_bigquery
