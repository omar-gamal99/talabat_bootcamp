import os
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import psycopg2
import pandas as pd
from datetime import datetime

def extract_and_upload_to_gcs(table_name, conn_id, schema, dataset, bucket_name):
    conn = {
        'backend-customers-prod-db': {
            'host': '34.57.107.112',
            'user': 'postgres',
            'password': 'talabat-iti',
            'dbname': 'postgres',
            'port': 5432,
        },
        'backend-orders-prod-db': {
            'host': '34.57.107.112',
            'user': 'postgres',
            'password': 'talabat-iti',
            'dbname': 'postgres',
            'port': 5432,
        },
        'backend-products-prod-db': {
            'host': '34.57.107.112',
            'user': 'postgres',
            'password': 'talabat-iti',
            'dbname': 'postgres',
            'port': 5432,
        },
    }[conn_id]

    conn_pg = psycopg2.connect(**conn)
    df = pd.read_sql_query(f'SELECT * FROM {schema}.{table_name};', conn_pg)
    conn_pg.close()

    local_path = f"/tmp/{table_name}.csv"
    df.to_csv(local_path, index=False)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{dataset}/{table_name}.csv")
    blob.upload_from_filename(local_path)
    print(f"Uploaded {table_name} to gs://{bucket_name}/{dataset}/{table_name}.csv")

# Full paths to each tables_config.yaml file
base_dir = os.path.dirname(__file__)
yaml_files = [
    os.path.join(base_dir, "TEMP", "customers_db", "tables_config.yaml"),
    os.path.join(base_dir, "TEMP", "orders_db", "tables_config.yaml"),
    os.path.join(base_dir, "TEMP", "products_db", "tables_config.yaml"),
]

BUCKET_NAME = "talabat-labs-postgres-to-gcs"

for filepath in yaml_files:
    with open(filepath) as f:
        config = yaml.safe_load(f)

    dag_id = f"mariam_{config['dag_id']}"
    default_args = config.get("default_args", {})

    if isinstance(default_args.get("start_date"), str):
        default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")

    dag = DAG(
        dag_id=dag_id,
        description=config.get("description", ""),
        schedule_interval=config.get("schedule_interval"),
        default_args=default_args,
        catchup=default_args.get("catchup", False),
        concurrency=config.get("concurrency", 1),
        max_active_runs=config.get("max_active_runs", 1),
        tags=["mariam", "dynamic", "generated_from_yaml"],
    )

    with dag:
        for table in config.get("tables", []):
            task_id = f"mariam_extract_and_upload_{table['table_name']}"
            PythonOperator(
                task_id=task_id,
                python_callable=extract_and_upload_to_gcs,
                op_kwargs={
                    "table_name": table["table_name"],
                    "conn_id": config["default_postgres_conn_id"],
                    "schema": config["default_source_schema"],
                    "dataset": config["default_dataset"],
                    "bucket_name": BUCKET_NAME,
                },
            )

    globals()[dag_id] = dag
