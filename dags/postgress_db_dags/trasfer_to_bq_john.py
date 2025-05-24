from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

with DAG(
    dag_id="postgres_to_bigquery_john",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "gcs", "bigquery"]
) as dag:

    export_pg_to_gcs = PostgresToGCSOperator(
        task_id='export_pg_to_gcs_john',
        postgres_conn_id='postgres_conn_john',  
        sql='SELECT * FROM public.orders;',
        bucket='talabat-labs-postgres-to-gcs',
        filename='exported_data_john/{{ ds_nodash }}.json',
        export_format='json'
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bigquery_john',
        bucket='talabat-labs-postgres-to-gcs',
        source_objects=['exported_data_john/{{ ds_nodash }}.json'],
        destination_project_dataset_table='talabat-labs-3927.landing.orders_john',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True
    )

    export_pg_to_gcs >> load_to_bq
