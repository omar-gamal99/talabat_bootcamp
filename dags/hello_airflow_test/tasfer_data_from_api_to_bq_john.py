from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.exceptions import AirflowException

from datetime import datetime
import requests
import pandas as pd
import os
import logging

# Configuration
BUCKET_NAME = 'talabat-labs-postgres-to-gcs'
FILENAME = 'payments_john.csv'
GCS_PATH = f'data/{FILENAME}'
GCS_URI = f'gs://{BUCKET_NAME}/{GCS_PATH}'
API_URL = 'https://payments-table-728470529083.europe-west1.run.app'


API_HEADERS = {
    'User-Agent': 'Airflow-ETL/1.0',
    'Accept': 'application/json'
}
API_TIMEOUT = 30

default_args = {
    'start_date': datetime(2025, 5, 23),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

def extract_api_data(**kwargs):
    """Extract data from API and save as CSV locally"""
    try:
        logging.info(f"Making API request to: {API_URL}")
        
        response = requests.get(
            API_URL, 
            headers=API_HEADERS,
            timeout=API_TIMEOUT
        )
        response.raise_for_status()
        
        data = response.json()
        logging.info(f"Successfully retrieved {len(data)} records from API")
        

        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):

            if 'data' in data:
                df = pd.DataFrame(data['data'])
            elif 'results' in data:
                df = pd.DataFrame(data['results'])
            else:
                df = pd.DataFrame([data])
        else:
            raise AirflowException(f"Unexpected data format from API: {type(data)}")
        
        if df.empty:
            raise AirflowException("No data received from API")
        
        logging.info(f"DataFrame created with shape: {df.shape}")
        logging.info(f"Columns: {list(df.columns)}")
        
 
        local_path = f'/tmp/{FILENAME}'
        df.to_csv(local_path, index=False)
        logging.info(f"Data saved to local file: {local_path}")
        

        kwargs['ti'].xcom_push(key='local_path', value=local_path)
        kwargs['ti'].xcom_push(key='gcs_path', value=GCS_PATH)
        kwargs['ti'].xcom_push(key='record_count', value=len(df))
        
        return f"Successfully extracted {len(df)} records"
        
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Connection error: {str(e)}")
        raise AirflowException(f"Failed to connect to API: {API_URL}. Please check the URL and network connectivity.")
    
    except requests.exceptions.Timeout as e:
        logging.error(f"Request timeout: {str(e)}")
        raise AirflowException(f"API request timed out after {API_TIMEOUT} seconds")
    
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error: {str(e)}")
        raise AirflowException(f"API returned HTTP error: {e.response.status_code}")
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {str(e)}")
        raise AirflowException(f"API request failed: {str(e)}")
    
    except pd.errors.EmptyDataError:
        logging.error("No data to parse from API response")
        raise AirflowException("API returned empty data")
    
    except Exception as e:
        logging.error(f"Unexpected error in extract_api_data: {str(e)}")
        raise AirflowException(f"Failed to extract data from API: {str(e)}")

def upload_to_gcs(**kwargs):
    """Upload CSV file to Google Cloud Storage"""
    try:
        # Get paths from previous task
        local_path = kwargs['ti'].xcom_pull(key='local_path')
        gcs_path = kwargs['ti'].xcom_pull(key='gcs_path')
        record_count = kwargs['ti'].xcom_pull(key='record_count')
        
        if not local_path or not os.path.exists(local_path):
            raise AirflowException(f"Local file not found: {local_path}")
        
        logging.info(f"Uploading {local_path} to gs://{BUCKET_NAME}/{gcs_path}")
        

        hook = GCSHook(gcp_conn_id='google_cloud_default')
        hook.upload(
            bucket_name=BUCKET_NAME, 
            object_name=gcs_path, 
            filename=local_path
        )
        
        logging.info(f"Successfully uploaded {record_count} records to GCS")
        

        try:
            os.remove(local_path)
            logging.info(f"Cleaned up local file: {local_path}")
        except OSError:
            logging.warning(f"Could not remove local file: {local_path}")
        
        return f"Successfully uploaded {record_count} records to GCS"
        
    except Exception as e:
        logging.error(f"Failed to upload to GCS: {str(e)}")
        raise AirflowException(f"GCS upload failed: {str(e)}")


with DAG(
    'api_to_bigquery_john',
    default_args=default_args,
    description='Extract data from API, upload to GCS, and load to BigQuery',
    schedule_interval=None,
    catchup=False,
    tags=['api', 'gcs', 'bigquery', 'etl'],
) as dag:


    extract_task = PythonOperator(
        task_id='extract_api_data',
        python_callable=extract_api_data,
        doc_md="""
        ### Extract API Data
        This task extracts data from the configured API endpoint and saves it as a CSV file locally.
        """,
    )

 
    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        doc_md="""
        ### Upload to GCS
        This task uploads the CSV file to Google Cloud Storage.
        """,
    )


    load_task = GCSToBigQueryOperator(
        task_id='load_api_csv_to_bq_john',
        bucket=BUCKET_NAME,
        source_objects=[GCS_PATH],
        destination_project_dataset_table='talabat-labs-3927.landing.payments_john',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
        doc_md="""
        ### Load to BigQuery
        This task loads the CSV data from GCS into BigQuery table.
        """,
    )

    # Define task dependencies
    extract_task >> upload_task >> load_task