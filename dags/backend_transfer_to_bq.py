from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

POSTGRES_CONN_ID = "postgres_conn"
GCS_BUCKET = "GCS_BUCKET_NAME"
GCS_FILENAME = "GCS_FILENAME"
BIGQUERY_DATASET = "PROJECT_ID.DATASET_NAME"
BIGQUERY_TABLE = "TABLE_NAME"

default_args = {
    "retries": 1,
}

dag = DAG(
    "DATABASE_NAME_DB_EXTRAct",
    default_args=default_args,
    description="Transfer data from DATABASE NAME PostgreSQL to GCS and load into BigQuery",
    schedule_interval=None,
    catchup=False,
)

transfer_postgres_to_gcs = PostgresToGCSOperator(
    task_id=f"{BIGQUERY_TABLE}_postgres_to_gcs",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="SELECT * FROM orders1",
    bucket=GCS_BUCKET,
    filename=GCS_FILENAME,
    export_format="json",
    dag=dag,
)

load_gcs_to_bigquery = GCSToBigQueryOperator(
    task_id="load_gcs_to_bigquery",
    bucket=GCS_BUCKET,
    source_objects=[GCS_FILENAME],
    destination_project_dataset_table=f"{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
    source_format="NEWLINE_DELIMITED_JSON",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

transfer_postgres_to_gcs >> load_gcs_to_bigquery
