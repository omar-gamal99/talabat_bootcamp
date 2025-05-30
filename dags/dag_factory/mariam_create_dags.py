import os
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def extract_table(**kwargs):
    print(f"[Mariam's DAG] Extracting table {kwargs['table_name']} from {kwargs['conn_id']} into dataset {kwargs['dataset']}")

# Path to your existing YAML files
CONFIG_DIR = os.path.join(os.path.dirname(__file__), "yaml_configs")

# Load each YAML file and create a DAG
for filename in os.listdir(CONFIG_DIR):
    if filename.endswith(".yaml"):
        with open(os.path.join(CONFIG_DIR, filename)) as f:
            config = yaml.safe_load(f)

        # Add "mariam_" prefix to DAG ID
        original_dag_id = config["dag_id"]
        mariam_dag_id = f"mariam_{original_dag_id}"

        # Prepare default_args
        default_args = config["default_args"]
        default_args["start_date"] = days_ago(1)

        dag = DAG(
            dag_id=mariam_dag_id,
            description=config.get("description", ""),
            schedule_interval=config.get("schedule_interval"),
            default_args=default_args,
            catchup=config["default_args"].get("catchup", False),
            concurrency=config.get("concurrency", 1),
            max_active_runs=config.get("max_active_runs", 1),
            tags=["mariam", "auto", "extract"],
        )

        with dag:
            for table in config["tables"]:
                PythonOperator(
                    task_id=f"mariam_extract_{table['table_name']}",
                    python_callable=extract_table,
                    op_kwargs={
                        "table_name": table["table_name"],
                        "conn_id": config["default_postgres_conn_id"],
                        "dataset": config["default_dataset"],
                    },
                )

        # Register the DAG so Airflow detects it
        globals()[mariam_dag_id] = dag
