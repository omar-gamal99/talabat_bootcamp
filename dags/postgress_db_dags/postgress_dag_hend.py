from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime



with DAG(
    'postgres_to_bigquery_hend',
    schedule_interval=None,
    start_date=datetime(2025,5,23),
    catchup=False,
    tags=["transfer"]

) as dag:
    postress_to_gcs = PostgresToGCSOperator(
        task_id='postgress_GCS',
        postgres_conn_id='postgres-conn-hend',
        bucket='talabat-labs-postgres-to-gcs',
        sql='select * from public.orders',
        filename='postgres_data/postgres_orders_hend.json',
        export_format='json'
    )
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='gcs_bigquery',
        bucket='talabat-labs-postgres-to-gcs',
        source_objects=['postgres_data/postgres_orders_hend.json'],
        source_format='NEWLINE_DELIMITED_JSON',
        destination_project_dataset_table='talabat-labs-3927.landing.orders_hend',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE'
    )

    postress_to_gcs >> gcs_to_bigquery