import os
import yaml
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------- Load YAML config ----------
def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

# ---------- Example Task Function ----------
def extract_table(table_name, schema, dataset, db_type, conn_id, **kwargs):
    print(f"Extracting {table_name} from schema {schema}, saving to {dataset}, using {db_type}, conn_id={conn_id}")

# ---------- DAG Factory Function ----------
def create_dag_from_config(config: dict, dag_id: str) -> DAG:
    default_args = config.get("default_args", {})
    # Parse start_date to datetime
    default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
    
    # Use 'schedule' instead of deprecated 'schedule_interval'
    schedule = config.get("schedule_interval")  # you can rename key in yaml if you want 'schedule'
    
    dag = DAG(
        dag_id=dag_id,  # Use the modified dag_id here with your name appended
        description=config.get("description", ""),
        schedule=schedule,
        default_args=default_args,
        catchup=default_args.get("catchup", False),
        max_active_tasks=config.get("concurrency", 10),  # Use max_active_tasks, but config key is still 'concurrency' for backward compat
        max_active_runs=config.get("max_active_runs", 1),
    )

    with dag:
        for table in config.get("tables", []):
            PythonOperator(
                task_id=f"extract_{table['table_name']}",
                python_callable=extract_table,
                op_kwargs={
                    "table_name": table["table_name"],
                    "schema": config["default_source_schema"],
                    "dataset": config["default_dataset"],
                    "db_type": config["default_db_type"],
                    "conn_id": config["default_postgres_conn_id"],
                }
            )
    return dag

# ---------- Auto-discover & create DAGs ----------
BASE_DIR = os.path.dirname(__file__)  # e.g., dags/dag_factory

for subdir in os.listdir(BASE_DIR):
    full_path = os.path.join(BASE_DIR, subdir)
    config_path = os.path.join(full_path, "tables_config.yaml")

    if os.path.isdir(full_path) and os.path.exists(config_path):
        dag_config = load_config(config_path)
        
        # Append "_islam_sayed" to the dag_id
        dag_id_with_name = dag_config["dag_id"] + "_islam_sayed"
        
        globals()[dag_id_with_name] = create_dag_from_config(dag_config, dag_id_with_name)
