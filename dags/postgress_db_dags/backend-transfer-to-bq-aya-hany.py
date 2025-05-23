from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


## Define the default arguments for the DAG
default_args = {
    'owner': 'ayah',
    'start_date': datetime(2025, 5, 23),
    'retries': 1,  
    'retry_delay': timedelta(minutes=2), 
}

with DAG(
    'Postgress to BQ aya-hany',
    default_args=default_args,
    description='Airflow DAG for Postgres to BigQuery ETL',
) as postgress_bq_etl :
    
    ## Task to transfer data from PostgreSQL to GCS
    postgres_to_gcs = PostgresToGCSOperator(
        task_id = 'postgres_to_gcs',
        sql = 'SELECT * FROM Orders;',
        bucket = 'talabat-labs-postgres-to-gcs',
        filename = 'orders-aya-hany',
        export_format = 'CSV',  
        field_delimiter=',',  
        gzip = False,  
    )

    ## Task to transfer data from GCS Bucket to BigQuery
    bq_load_csv = GCSToBigQueryOperator(
        task_id="bq_load_csv",
        bucket='talabat-labs-postgres-to-gcs',
        source_objects=['orders-aya-hany.csv'],
        source_format="CSV",
        destination_project_dataset_table=f"{talabat-labs-3927}.{landing}.{orders-aya-hany}",
        # schema_fields=[
        #     {"name": "id", "type": "STRING", "mode": "REQUIRED"},
        #     {"name": "ticker", "type": "STRING", "mode": "NULLABLE"},
        #     {"name": "date", "type": "STRING", "mode": "NULLABLE"},
        #     {"name": "open", "type": "FLOAT64", "mode": "NULLABLE"},
        #     {"name": "high", "type": "FLOAT64", "mode": "NULLABLE"},
        #     {"name": "low", "type": "FLOAT64", "mode": "NULLABLE"},
        #     {"name": "close", "type": "FLOAT64", "mode": "NULLABLE"},
        #     {"name": "volume", "type": "INT64", "mode": "NULLABLE"},
        # ],
        # create_disposition='CREATE_IF_NEEDED',  # You can change this if needed
        write_disposition="WRITE_TRUNCATE", 
    )

postgres_to_gcs >> bq_load_csv