from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

## Define the default arguments for the DAG
default_args = {
    'owner': 'islamsayed',
    'start_date': datetime(2023, 5, 22),
    'retries': 1,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5),  # Time between retries
}

## Create a DAG instance
dag = DAG(
    'BigQuery_ETL_islamsayed',
    default_args=default_args,
    description='An Airflow DAG for IDX Stock Postgre to BigQuery',
    schedule_interval=None,  # Set the schedule interval (e.g., None for manual runs)
    catchup=True  # Do not backfill (run past dates) when starting the DAG
)
## BigQuery config variables
BQ_PROJECT = "talabat-labs-3927"
BQ_DATASET = "talabat-labs-3927.landing"
BQ_TABLE1 = "order_data_islamsayed"
BQ_BUCKET = 'talabat-labs-postgres-to-gcs'

## Postgres config variables
PG_CONN_ID = "postgres_connection_islamsayed" # Defined in airflow connection
PG_SCHEMA = "public"
PG_TABLE1 = "orders"

## Task to transfer data from PostgreSQL to GCS
postgres_to_gcs = PostgresToGCSOperator(
    task_id = 'postgres_to_gcs',
    sql = f'SELECT * FROM "{PG_SCHEMA}"."{PG_TABLE1}";',
    bucket = BQ_BUCKET,
    export_format = 'CSV',  # You can change the export format as needed
    postgres_conn_id = PG_CONN_ID,  # Set your PostgreSQL connection ID
    field_delimiter=',',  # Optional, specify field delimiter for CSV
    gzip = False,  # Set to True if you want to compress the output file
    task_concurrency = 1,  # Optional, adjust concurrency as needed
    dag = dag,
)
## Task to transfer data from GCS Bucket to BigQuery
bq_stock_load_csv = GCSToBigQueryOperator(
    task_id="bq_stock_load_csv",
    bucket=BQ_BUCKET,
    destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE1}",
    source_format="CSV",
    create_disposition='CREATE_IF_NEEDED',  # You can change this if needed
    write_disposition="WRITE_TRUNCATE", # You can change this if needed
    dag = dag,
)
postgres_to_gcs >> bq_stock_load_csv