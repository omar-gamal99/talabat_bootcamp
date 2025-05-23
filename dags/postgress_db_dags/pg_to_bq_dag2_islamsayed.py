from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Default arguments
default_args = {
    'owner': 'islamsayed',
    'start_date': datetime(2023, 5, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id='bigquery_etl_islamsayed',
    default_args=default_args,
    description='An Airflow DAG for IDX Stock Postgre to BigQuery',
    schedule_interval=None,
    catchup=True
)

# Configuration
BQ_PROJECT = "talabat-labs-3927"
BQ_DATASET = "landing"
BQ_TABLE1 = "order_data_islamsayed"
BQ_BUCKET = 'talabat-labs-postgres-to-gcs'

PG_CONN_ID = "postgres_connection_islamsayed"
PG_SCHEMA = "public"
PG_TABLE1 = "orders"

# PostgreSQL to GCS
postgres_to_gcs = PostgresToGCSOperator(
    task_id='postgres_to_gcs',
    sql=f'SELECT * FROM "{PG_SCHEMA}"."{PG_TABLE1}";',
    bucket=BQ_BUCKET,
    filename='orders_islam_{{ ds }}.csv',
    export_format='CSV',
    postgres_conn_id=PG_CONN_ID,
    field_delimiter=',',
    gzip=False,
    dag=dag,
)

# GCS to BigQuery
bq_stock_load_csv = GCSToBigQueryOperator(
    task_id="bq_stock_load_csv",
    bucket=BQ_BUCKET,
    source_objects=['orders_islam_{{ ds }}.csv'],
    destination_project_dataset_table=f"{BQ_PROJECT}:{BQ_DATASET}.{BQ_TABLE1}",
    source_format="CSV",
    create_disposition='CREATE_IF_NEEDED',
    write_disposition="WRITE_TRUNCATE",
    skip_leading_rows=1,  # If your CSV has headers
    dag=dag,
)

# DAG dependencies
postgres_to_gcs >> bq_stock_load_csv
