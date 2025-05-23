from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_bigquery import PostgresToBigQueryOperator
from datetime import datetime



with DAG(
    dag_id='postgres_to_bq_john',
    start_date=datetime(2025, 5, 23),
    schedule_interval=None,
    catchup=False,
) as dag:
    transfer_postgres_to_bigquery = PostgresToBigQueryOperator(
        task_id='transfer_postgres_table_to_bq',
        postgres_conn_id='postgres_conn_john',
        sql='SELECT * FROM public.orders',  
        destination_project_dataset_table='talabat-labs-3927.landing.orders_john',  
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )