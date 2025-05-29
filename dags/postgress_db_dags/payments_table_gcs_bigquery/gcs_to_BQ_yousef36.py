from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime




# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 25),
}

# Define the DAG
with DAG(
    'task_transfer_from_payment_gcs_to_bigquery',
    default_args=default_args,
    description='A simple Dag to transfer from gcs payment to bigquery',
    schedule_interval='@daily',  # Run once a day
    catchup=False,  # to prevent the dag from trying to run agian and catch days it didnt run
) as dag:

    # Define the Bash task
    load_data_to_BQ = GCSToBigQueryOperator(
        task_id="gcs_to_bigQuery",
        bucket="talabat-labs-payment-data",
        source_objects=["data/payment_202505230116.csv"],
        destination_project_dataset_table=f"talabat-labs-3927.landing.payments-yousefkk",
        create_disposition='CREATE_IF_NEEDED',
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,source_format="CSV",
        skip_leading_rows=1,
    )


    load_data_to_BQ

    
