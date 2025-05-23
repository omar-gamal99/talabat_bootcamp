from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime


with DAG(
    dag_id='transfer_from_postgres_to_bq_elham',
    description='Transfer data from Postgres to BigQuery using GCS as an intermediate step',
    schedule=None,
    start_date=datetime(2025, 5, 22),
    catchup=False,
) as dag:
    # Task to transfer data from Postgres to GCS
    transfer_postgres_to_gcs = PostgresToGCSOperator(
        task_id='transfer_postgres_to_gcs',
        postgres_conn_id='postgres_connection_elham',
        sql='SELECT * FROM public.orders',
        bucket_name='talabat-labs-postgres-to-gcs',
        filename='data/postgres_data_elham.json',
        export_format='json',
        gzip=False
    )

    # Task to load data from GCS to BigQuery
    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        bucket_name='talabat-labs-postgres-to-gcs',
        source_objects=['data/postgres_data_elham.json'],
        destination_project_dataset_table='talabat-labs-3927.landing.orders-elham',
        source_format='newline_delimited_json',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True
    )

    transfer_postgres_to_gcs >> load_gcs_to_bigquery