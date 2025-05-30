from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

with DAG(
    dag_id='postgres_to_gcs_to_bigquery',
    start_date=datetime(2024, 8, 8),
    schedule_interval=None,
    catchup=False
) as dag:

    # Step 1: Extract orders from Postgres to GCS1
    extract_to_gcs = PostgresToGCSOperator(
        task_id='extract_orders_to_gcs',
        sql="SELECT * FROM public.orders",
        bucket='talabat-labs-postgres-to-gcs',  
        filename='orders/orders_data_{{ ds_nodash }}.csv',
        export_format='CSV',
        field_delimiter=',',
        postgres_conn_id='postgres_osama',
        gcp_conn_id='google_cloud_default'  
    )

    # Step 2: Load from GCS to BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id='load_orders_to_bigquery',
        bucket='talabat-labs-postgres-to-gcs', 
        source_objects=['orders/orders_data_{{ ds_nodash }}.csv'],
        destination_project_dataset_table='talabat-labs-3927.landing.orders_Osama',
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud_default'
    )

    extract_to_gcs >> load_to_bq
