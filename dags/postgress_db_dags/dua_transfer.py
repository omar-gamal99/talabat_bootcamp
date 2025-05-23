from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

POSTGRES_CONN_ID = 'postgres_dua'
GCP_CONN_ID = 'my_gcp_conn'
GCS_BUCKET = 'talabat-labs-postgres-to-gcs'
GCS_OBJECT = 'data/orders.json'  
BQ_DATASET = 'talabat-labs-3927.landing'
BQ_TABLE = 'orders'

with DAG(
    dag_id='postgres_to_bigquery_orders',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['postgres', 'bigquery'],
) as dag:

    
    export_postgres_to_gcs = PostgresToGCSOperator(
        task_id='export_postgres_to_gcs',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="SELECT * FROM public.orders;", 
        bucket=GCS_BUCKET,
        filename=GCS_OBJECT,
        export_format='json',  
        gzip=False
    )

    
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        bucket=GCS_BUCKET,
        source_objects=[GCS_OBJECT],
        destination_project_dataset_table=f"{BQ_DATASET}.{BQ_TABLE}",
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
        google_cloud_storage_conn_id=GCP_CONN_ID
    )

   
    export_postgres_to_gcs >> load_gcs_to_bq
