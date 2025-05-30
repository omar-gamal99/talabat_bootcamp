from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from google.cloud import storage
import csv
import os

bucket_name= 'talabat-labs-postgres-to-gcs'
DEST_FILENAME = 'exported_data.csv'
DEST_PATH = f'/tmp/{DEST_FILENAME}'
GCS_PATH = f'data/{DEST_FILENAME}'

hook = PostgresHook(postgres_conn_id='34.57.107.112')
conn = hook.get_conn()
cursor = conn.cursor()
cursor.execute("SELECT * FROM public.orders")