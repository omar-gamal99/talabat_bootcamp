from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator


POSTGRES_CONN_ID = "postgres_ahmedmarzouk"
GCS_BUCKET = "ahmed-marzouk-exported-data"
GCS_FILENAME = "orders_{{ ds }}.json"  
BIGQUERY_DATASET = "talabat-labs-3927.landing"
BIGQUERY_TABLE = "orders__ahmed_marzouk"


default_args = {
    "owner": "Ahmed marzouk",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "orders_db_transfer_Ahmed_marzouk",
    default_args=default_args,
    description="Transfer data from orders PostgreSQL to GCS and load into BigQuery",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["postgres", "bigquery", "etl"],
)

transfer_postgres_to_gcs = PostgresToGCSOperator(
    task_id=f"{BIGQUERY_TABLE}_postgres_to_gcs",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
        SELECT 
            *
        FROM public.orders
    """,  
    bucket=GCS_BUCKET,
    filename=GCS_FILENAME,
    export_format="json",
    gzip=True,  
    dag=dag,
)


load_gcs_to_bigquery = GCSToBigQueryOperator(
    task_id="load_gcs_to_bigquery",
    bucket=GCS_BUCKET,
    source_objects=[GCS_FILENAME],
    destination_project_dataset_table=f"{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
    source_format="NEWLINE_DELIMITED_JSON",
    write_disposition="WRITE_APPEND",  
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,  
    max_bad_records=10,  
    dag=dag,
)




transfer_postgres_to_gcs >> load_gcs_to_bigquery 