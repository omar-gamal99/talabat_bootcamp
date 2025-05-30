import os
import yaml
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

def load_yaml_configs(config_folder):
    dag_configs = []
    for root, _, files in os.walk(config_folder):
        for file_name in files:
            if file_name.endswith(".yaml") or file_name.endswith(".yml"):
                file_path = os.path.join(root, file_name)
                with open(file_path) as f:
                    dag_configs.append(yaml.safe_load(f))
    return dag_configs

def create_dag_from_config(config):
    dag_id = config["dag_id"]
    default_args = config["default_args"]
    dataset = config["default_dataset"]
    source_schema = config["default_source_schema"]
    conn_id = config["default_postgres_conn_id"]
    tables = config["tables"]
    bucket = "talabat-labs-postgres-to-gcs"  # ✅ Update this with your real bucket name if needed

    with DAG(
        dag_id=dag_id,
        description=config.get("description", ""),
        schedule_interval=config.get("schedule_interval", None),
        default_args=default_args,
        concurrency=config.get("concurrency", 5),
        max_active_runs=config.get("max_active_runs", 1),
        catchup=False,
        start_date=days_ago(1),
        tags=["pg-to-bq", "dynamic"]
    ) as dag:

        for table in tables:
            table_name = table["table_name"]
            gcs_filename = f"{dag_id}/{table_name}_{{{{ ds_nodash }}}}.json"

            export_to_gcs = PostgresToGCSOperator(
                task_id=f"export_{table_name}_to_gcs",
                postgres_conn_id=conn_id,
                sql=f"SELECT {table['columns']} FROM {source_schema}.{table_name}",
                bucket=bucket,
                filename=gcs_filename,
                export_format="json"
            )

            load_to_bq = GCSToBigQueryOperator(
                task_id=f"load_{table_name}_to_bq",
                bucket=bucket,
                source_objects=[gcs_filename],
                destination_project_dataset_table=f"{dataset}.{table_name}",
                source_format="NEWLINE_DELIMITED_JSON",
                write_disposition="WRITE_APPEND",
                autodetect=True
            )

            export_to_gcs >> load_to_bq

        return dag

# Set config path relative to this script
config_folder = os.path.join(os.path.dirname(__file__), "Temp")

# Create DAGs from YAML configs
for config in load_yaml_configs(config_folder):
    dag_id = config["dag_id"]
    globals()[dag_id] = create_dag_from_config(config)
