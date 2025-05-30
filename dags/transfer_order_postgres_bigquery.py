from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

POSTGRES_CONN_ID = "postgress-conn-abdullah-adel"
GCS_BUCKET = "talabat-labs-postgres-to-gcs"
GCS_FILENAME = "abdullah"
BIGQUERY_DATASET = "talabat-labs-3927.landing"
BIGQUERY_TABLE = "abdullah-orders"

default_args = {
    "retries": 1,
}

dag = DAG(
    "orders_db_transfer_abdullah_adel",
    default_args=default_args,
    description="Transfer data from orders PostgreSQL to GCS and load into BigQuery",
    schedule_interval=None,
    catchup=False,
)

transfer_postgres_to_gcs = PostgresToGCSOperator(
    task_id=f"{BIGQUERY_TABLE}_postgres_to_gcs",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="SELECT * FROM orders",
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
