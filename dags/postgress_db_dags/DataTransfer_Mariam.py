from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

default_args = {
    'owner': 'airflow',
}


SOURCE_TABLE_NAME = 'orders'  
FILE_FORMAT = 'csv'

with DAG(
    dag_id='postgres_to_gcs_export',
    default_args=default_args,
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    tags=['postgres', 'gcs', 'export'],
) as dag:

    postgres_to_gcs_task = PostgresToGCSOperator(
        task_id='export_postgres_to_gcs',
        postgres_conn_id='postgres_default',  # Must match your Airflow connection ID
        sql=f'SELECT * FROM {SOURCE_TABLE_NAME};',
        bucket='talabat-labs-postgres-to-gcs',
        filename=f'postgres-export/{SOURCE_TABLE_NAME}.{FILE_FORMAT}',
        export_format=FILE_FORMAT,
        gzip=False,
        use_server_side_cursor=False,
    )

    postgres_to_gcs_task
