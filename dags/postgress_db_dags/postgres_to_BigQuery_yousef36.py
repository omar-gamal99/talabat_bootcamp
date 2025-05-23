from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import PostgreSQLToBigQueryOperator
from datetime import datetime


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'simple_task_to_transfer_from_postgres_to_bigquery',
    default_args=default_args,
    description='A simple DAG with a python task to sum 2 numbers',
    schedule_interval='@daily',  # Run once a day
    catchup=False,  # to prevent the dag from trying to run agian and catch days it didnt run
) as dag:

    # Define the Bash task
    transfer_data = PostgreSQLToBigQueryOperator(
        task_id='transfer_data_from_postgres_to_BigQuery',
        source_table='public.orders',
        destination_table='talabat-labs-3927.landing.yousef-orders',
        postgres_conn_id='postgres-conn_yousef36',
        google_cloud_conn_id='bigquery_default',
    )


    transfer_data 

    
