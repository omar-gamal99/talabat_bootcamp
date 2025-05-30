import glob
import itertools
from datetime import datetime
import yaml

from airflow.decorators import dag, task_group
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

BUCKET_NAME = "talabat-labs-data-platform-airflow-maindb-exports"

def generate_dag(dag_id, schedule, tables, default_args, config):
    @dag(
        dag_id=dag_id,
        schedule_interval=schedule,
        default_args=default_args,
        catchup=False,
        concurrency=config.get("concurrency", 10),
        max_active_runs=config.get("max_active_runs", 1),
        description=config.get("description", ""),
        tags=config.get("tags", []),
    )
    def extract_and_load():
        @task_group(group_id="backend_gcs_bq_tasks")
        def backend_gcs_bq_tasks():
            for table_data in tables:
                table_name = table_data["table_name"]
                columns = table_data.get("columns", "*")

                dataset = config.get("default_dataset", "data_platform_export")
                schema = config.get("default_source_schema", "public")
                conn_id = config.get("default_postgres_conn_id", "default_postgres_conn")

                gcs_prefix = f"{dataset}/{table_name}/data_json/raw_data/"
                gcs_file = f"{gcs_prefix}raw_data/{{{{ ts_nodash }}}}.json"

                export_task = PostgresToGCSOperator(
                    task_id=f"{table_name}_to_gcs",
                    sql=f"""SELECT {columns} FROM {schema}."{table_name}" """,
                    postgres_conn_id=conn_id,
                    bucket=BUCKET_NAME,
                    filename=gcs_file,
                    export_format="json",
                )

                load_task = GCSToBigQueryOperator(
                    task_id=f"gcs_to_bq_{table_name}",
                    bucket=BUCKET_NAME,
                    source_objects=[f"{gcs_prefix}raw_data/*.json"],
                    destination_project_dataset_table=f"talabat_labs.{dataset}.staging_{table_name}",
                    source_format="NEWLINE_DELIMITED_JSON",
                    write_disposition="WRITE_TRUNCATE",
                )

                export_task >> load_task

        backend_gcs_bq_tasks()

    return extract_and_load()


config_paths = list(itertools.chain(
    glob.glob("./gcs/dags/dag_factory/TEMP/customers_db/*.yaml", recursive=True),
    glob.glob("./gcs/dags/dag_factory/TEMP/orders_db/*.yaml", recursive=True),
    glob.glob("./gcs/dags/dag_factory/TEMP/products_db/*.yaml", recursive=True),
))

for config_path in config_paths:
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    required_keys = ["dag_id", "tables"]
    missing = [k for k in required_keys if k not in config]
    if missing:
        raise KeyError(f"Missing required keys {missing} in file: {config_path}")

    dag_id = config["dag_id"]
    tables = config["tables"]
    schedule = config.get("schedule_interval", None)

    defaults = config.get("default_args", {})
    default_args = {
        "owner": defaults.get("owner", "DE team"),
        "email": defaults.get("email", ["data.eng.team@talabat.com"]),
        "start_date": datetime.strptime(defaults.get("start_date", "2024-05-02"), "%Y-%m-%d"),
        "depends_on_past": defaults.get("depends_on_past", False),
        "catchup": defaults.get("catchup", False),
        "retries": defaults.get("retries", 1),
        "email_on_failure": defaults.get("email_on_failure", True),
        "gcp_conn_id": defaults.get("gcp_conn_id", "bigquery_default"),
        "max_active_runs": defaults.get("max_active_runs", 1),
        "concurrency": defaults.get("concurrency", 1),
    }

    globals()[dag_id] = generate_dag(dag_id, schedule, tables, default_args, config)
