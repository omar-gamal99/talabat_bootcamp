from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
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
    description='A simple Dag to transfer from posgtres to bigquery',
    schedule_interval='@daily',  # Run once a day
    catchup=False,  # to prevent the dag from trying to run agian and catch days it didnt run
) as dag:

    # Define the Bash task
    postgres_to_gcs = PostgresToGCSOperator(
        task_id="postgres_to_gcs",
        postgres_conn_id="postgres-conn_yousef36",
        sql="select * from orders",
        bucket="talabat-labs-postgres-to-gcs/yousef36",
        filename="yousef36",
        export_format="csv",
    )

    load_csv = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_example",
        bucket="talabat-labs-postgres-to-gcs-yousef36",
        source_objects=["talabat-labs-postgres-to-gcs/yousef36/yousef36.csv"],
        destination_project_dataset_table=f"talabat-labs-3927.landing.orders-yousefkk",
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
    )


    postgres_to_gcs >>  load_csv

    
