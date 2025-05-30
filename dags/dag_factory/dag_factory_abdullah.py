import os
import yaml
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from pathlib import Path
from datetime import datetime

# Base path to all configs
CONFIG_BASE_PATH = "/home/abdullah/talabat_bootcamp/dags/dag_factory/TEMP"

def load_yaml_configs(base_path):
    """
    Load all YAML files in the TEMP folder recursively.
    """
    config_files = Path(base_path).rglob("tables_config.yaml")
    for config_file in config_files:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
            yield config

def create_dag_from_config(config):
    original_dag_id = config.get("dag_id")
    dag_id = f"{original_dag_id}_abdullah"  # Add "abdullah" to DAG ID

    description = config.get("description", "No description provided")
    schedule_interval = config.get("schedule_interval", None)
    concurrency = config.get("concurrency", 1)
    max_active_runs = config.get("max_active_runs", 1)

    default_args = config.get("default_args", {})
    default_args["start_date"] = datetime.fromisoformat(default_args.get("start_date", "2025-01-01"))

    with DAG(
        dag_id=dag_id,
        description=description,
        schedule_interval=schedule_interval,
        concurrency=concurrency,
        max_active_runs=max_active_runs,
        default_args=default_args,
        catchup=default_args.get("catchup", False),
        tags=["dag_factory"],
    ) as dag:

        start = DummyOperator(task_id="start")
        end = DummyOperator(task_id="end")

        for table in config.get("tables", []):
            table_name = table["table_name"]
            task = DummyOperator(task_id=f"extract_{table_name}")
            start >> task >> end

    return dag_id, dag

# Generate DAGs dynamically
for config in load_yaml_configs(CONFIG_BASE_PATH):
    dag_id, dag = create_dag_from_config(config)
    globals()[dag_id] = dag
