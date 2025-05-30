from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 5, 23),
    'retries': 1,
}

with DAG(
    dag_id='postgres_to_gcs_export',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['postgres', 'gcs']
) as dag:

    export_to_gcs = PostgresToGCSOperator(
        task_id='export_postgres_data',
        postgres_conn_id='postgres_conn',
        sql='SELECT * FROM your_table_name',
        bucket='your-gcs-bucket-name',
        filename='data/your_table_{{ ds_nodash }}.json',  # or .csv
        export_format='json',  # options: 'json', 'csv', 'parquet'
        field_delimiter=',',  # only for CSV
        gzip=False,
    )
