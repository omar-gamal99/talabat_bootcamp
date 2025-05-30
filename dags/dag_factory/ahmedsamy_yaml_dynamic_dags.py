import os
import yaml
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# 🔧 Manually specify the full paths to each YAML config
YAML_PATHS = [
    "/opt/airflow/dags/custom_configs/dag_sales.yaml",
    "/opt/airflow/dags/custom_configs/dag_ops.yaml",
]

# Load a single YAML config
def load_yaml_config(filepath):
    with open(filepath, "r") as f:
        return yaml.safe_load(f)

# Create a DAG from config
def create_dag_from_config(config):
    dag_id = config["dag_id"]
    schedule = config.get("schedule_interval", "@daily")
    start_date = datetime.fromisoformat(config.get("start_date", "2023-01-01"))

    default_args = {
        "start_date": start_date,
    }

    dag = DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        default_args=default_args,
        catchup=False,
        tags=config.get("tags", []),
    )

    tasks_dict = {}
    with dag:
        for task_cfg in config.get("tasks", []):
            task = BashOperator(
                task_id=task_cfg["task_id"],
                bash_command=task_cfg["bash_command"],
                dag=dag
            )
            tasks_dict[task.task_id] = task

        for task_cfg in config.get("tasks", []):
            for upstream_id in task_cfg.get("upstream", []):
                tasks_dict[upstream_id] >> tasks_dict[task_cfg["task_id"]]

    return dag

# Loop over each path you define manually
for yaml_path in YAML_PATHS:
    if os.path.exists(yaml_path):
        config = load_yaml_config(yaml_path)
        dag = create_dag_from_config(config)
        globals()[config["dag_id"]] = dag
    else:
        print(f"⚠️ File not found: {yaml_path}")
