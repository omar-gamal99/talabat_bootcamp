from airflow import DAG
from airflow.providers.google.cloud.transfers.sql_to_gcs import SQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='pg_to_bq_via_gcs_AKhalifa', 
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['postgres', 'bigquery', 'talabat']
) as dag:

    export_from_postgres = SQLToGCSOperator(
        task_id='export_pg_to_gcs',
        sql='SELECT * FROM public.orders',  # ←  PostgreSQL
        bucket='talabat-labs-postgres-togcs',
        filename='data_exports/orders_{{ ds_nodash }}.csv',
        export_format='CSV',
        field_delimiter=',',
        gzip=False,
        sql_conn_id='postgress-conn-AKhalifa',
    )

    load_into_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq',
        bucket='talabat-labs-postgres-togcs',
        source_objects=['data_exports/orders_{{ ds_nodash }}.csv'],
        destination_project_dataset_table='talabat-labs-3927.landing.orders',  # ← BigQuery destination
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )

    export_from_postgres >> load_into_bq
