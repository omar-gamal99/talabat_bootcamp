import os
import csv
import yaml
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

def extract_and_upload_to_gcs(table_name, columns, postgres_conn_id, gcs_bucket, **kwargs):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    sql = f"SELECT {columns} FROM {table_name};"
    logging.info(f"Running query: {sql}")

    records = hook.get_records(sql)
    logging.info(f"Extracted {len(records)} rows from {table_name}")

    tmp_dir = '/tmp/airflow_exports'
    os.makedirs(tmp_dir, exist_ok=True)
    csv_path = os.path.join(tmp_dir, f"{table_name}.csv")

    # Write CSV including header
    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        cursor = hook.get_conn().cursor()
        cursor.execute(sql)
        colnames = [desc[0] for desc in cursor.description]
        writer.writerow(colnames)
        writer.writerows(records)

    gcs_hook = GCSHook()
    destination_blob_name = f"{table_name}.csv"
    gcs_hook.upload(
        bucket_name=gcs_bucket,
        object_name=destination_blob_name,
        filename=csv_path
    )
    logging.info(f"Uploaded {csv_path} to gs://{gcs_bucket}/{destination_blob_name}")

def create_dag_from_yaml(config):
    default_args = {
        'owner': config['default_args']['owner'],
        'depends_on_past': config['default_args']['depends_on_past'],
        'start_date': datetime.strptime(config['default_args']['start_date'], "%Y-%m-%d"),
        'retries': config['default_args']['retries'],
        'email': config['default_args']['email'],
        'email_on_failure': config['default_args']['email_on_failure'],
        'catchup': config['default_args']['catchup'],
    }

    dag = DAG(
        dag_id=config['dag_id'],
        description=config.get('description', ''),
        schedule_interval=config['schedule_interval'],
        concurrency=config.get('concurrency', 16),
        max_active_runs=config.get('max_active_runs', 1),
        default_args=default_args,
        catchup=config['default_args']['catchup'],
    )

    with TaskGroup("extract_tables", tooltip="Extract tables and upload to GCS") as tg:
        for table in config['tables']:
            PythonOperator(
                task_id=f"extract_upload_{table['table_name']}",
                python_callable=extract_and_upload_to_gcs,
                op_kwargs={
                    'table_name': table['table_name'],
                    'columns': table['columns'],
                    'postgres_conn_id': config.get('default_postgres_conn_id', 'postgres_default'),
                    'gcs_bucket': 'talabat-labs-postgres-to-gcs',
                },
                dag=dag,
            )

    return dag

# List your 3 YAML file paths here:
yaml_paths = [
    'C:\\Users\\Maya Emad\\Downloads\\talabat-assignment\\talabat-session-3\\talabat_bootcamp-1\\dags\\dag_factory\\TEMP\\customers_db\\tables_config.yaml',
    'C:\\Users\\Maya Emad\\Downloads\\talabat-assignment\\talabat-session-3\\talabat_bootcamp-1\\dags\\dag_factory\\TEMP\\orders_db\\tables_config.yaml',
    'C:\\Users\\Maya Emad\\Downloads\\talabat-assignment\\talabat-session-3\\talabat_bootcamp-1\\dags\\dag_factory\\TEMP\\products_db\\tables_config.yaml',
]

dags = {}

for path in yaml_paths:
    with open(path) as f:
        config = yaml.safe_load(f)
    dag = create_dag_from_yaml(config)
    dags[config['dag_id']] = dag

globals().update(dags)
