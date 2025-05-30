import os
import yaml
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import datetime

# 👇 path to the YAMLs directory
BASE_YAML_PATH = os.path.join(os.path.dirname(__file__), "TEMP")

def load_yaml_files(base_path):
    """Recursively load all YAML files under TEMP folder."""
    yamls = []
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(".yaml") or file.endswith(".yml"):
                yamls.append(os.path.join(root, file))
    return yamls

def create_dag_from_yaml(yaml_file_path):
    with open(yaml_file_path, 'r') as stream:
        config = yaml.safe_load(stream)

    dag_id = f'{config["dag_id"]}_mayaa'
    default_args = config.get("default_args", {})

    # Convert start_date from str to datetime
    if "start_date" in default_args:
        default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")

    dag = DAG(
        dag_id=dag_id,
        description=config.get("description", ""),
        schedule_interval=config.get("schedule_interval", None),
        concurrency=config.get("concurrency", 1),
        max_active_runs=config.get("max_active_runs", 1),
        default_args=default_args,
        catchup=default_args.get("catchup", False),
        tags=["dynamic", config.get("default_dataset", "dataset")]
    )

    schema = config["default_source_schema"]
    conn_id = config.get("default_postgres_conn_id", "default_conn_id_if_missing")
    tables = config["tables"]

    with dag:
        for table in tables:
            table_name = table["table_name"]
            columns = table.get("columns", "*")

            PostgresToGCSOperator(
                task_id=f"export_{table_name}",
                postgres_conn_id=conn_id,
                sql=f"SELECT {columns} FROM {schema}.{table_name}",
                bucket='talabat-labs-postgres-to-gcs',  
                filename=f"hoda_{dag_id}/{table_name}_{{{{ ds_nodash }}}}.json",
                export_format='json',
                field_delimiter=',',
                dag=dag
            )

    globals()[dag_id] = dag  # Register DAG globally


# 🔁 Load and create all DAGs
for yaml_path in load_yaml_files(BASE_YAML_PATH):
    create_dag_from_yaml(yaml_path)