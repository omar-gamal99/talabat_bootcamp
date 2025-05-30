import os
import yaml
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

# ---------- Load YAML config ----------
def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

# ---------- DAG Factory Function ----------
def create_dag_from_config(config: dict, dag_id: str) -> DAG:
    default_args = config.get("default_args", {})
    # Parse start_date to datetime
    if "start_date" in default_args:
        default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")

    schedule = config.get("schedule_interval")  # keep 'schedule_interval' key to match your YAML

    dag = DAG(
        dag_id=dag_id,
        description=config.get("description", ""),
        schedule_interval=schedule,
        default_args=default_args,
        catchup=default_args.get("catchup", False),
        concurrency=config.get("concurrency", 1),
        max_active_runs=config.get("max_active_runs", 1),
        tags=["dynamic", config.get("default_dataset", "dataset")]
    )

    schema = config.get("default_source_schema")
    conn_id = config.get("default_postgres_conn_id")
    bucket = config.get("gcs_bucket", "talabat-labs-postgres-to-gcs")  # Optionally override bucket in YAML

    with dag:
        for table in config.get("tables", []):
            table_name = table["table_name"]
            columns = table.get("columns", "*")

            PostgresToGCSOperator(
                task_id=f"export_{table_name}",
                postgres_conn_id=conn_id,
                sql=f"SELECT {columns} FROM {schema}.{table_name}",
                bucket=bucket,
                filename=f"{dag_id}/{table_name}_{{{{ ds_nodash }}}}.json",
                export_format='json',
                field_delimiter=',',
                dag=dag
            )

    return dag


# ---------- Auto-discover & create DAGs ----------
BASE_DIR = os.path.dirname(__file__)

for subdir in os.listdir(BASE_DIR):
    full_path = os.path.join(BASE_DIR, subdir)
    config_path = os.path.join(full_path, "tables_config.yaml")

    if os.path.isdir(full_path) and os.path.exists(config_path):
        dag_config = load_config(config_path)
        dag_id_with_name = dag_config["dag_id"] + "_islam_sayed"
        globals()[dag_id_with_name] = create_dag_from_config(dag_config, dag_id_with_name)
