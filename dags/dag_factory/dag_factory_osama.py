import glob
import itertools
import os
from datetime import datetime

import yaml
from airflow.decorators import dag, task_group
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

# Global constants
BUCKET_NAME = "talabat-labs-data-platform-airflow-maindb-exports"
CONFIG_DIRECTORIES = ["customers_db", "orders_db", "products_db"]
DAG_FACTORY_BASE_PATH = os.path.join("dags", "dag_factory")


def load_yaml_configs():
    """Load and return all YAML config paths from config directories."""
    config_files = list(
        itertools.chain.from_iterable(
            glob.glob(os.path.join(DAG_FACTORY_BASE_PATH, db, "*.yaml"))
            for db in CONFIG_DIRECTORIES
        )
    )
    return config_files


def build_default_args(config):
    """Construct and return default_args dict from config."""
    args = config.get("default_args", {})
    return {
        "owner": args.get("owner", "DE team"),
        "email": args.get("email", ["data.eng.team@talabat.com"]),
        "start_date": datetime.strptime(args.get("start_date", "2024-05-01"), "%Y-%m-%d"),
        "retries": args.get("retries", 1),
        "email_on_failure": args.get("email_on_failure", True),
        "depends_on_past": args.get("depends_on_past", False),
    }


def create_dag(dag_id, schedule, tables, default_args, config):
    """DAG generator from config dictionary."""

    @dag(
        dag_id=dag_id,
        schedule_interval=schedule,
        default_args=default_args,
        catchup=False,
        concurrency=config.get("concurrency", 10),
        max_active_runs=config.get("max_active_runs", 1),
        description=config.get("description", ""),
        tags=["dag_factory"],
    )
    def dag_definition():
        @task_group(group_id="extract_and_load_tasks")
        def data_tasks():
            for table in tables:
                table_name = table["table_name"]
                columns = table.get("columns", "*")
                dataset = config.get("default_dataset", "data_platform_export")
                schema = config.get("default_source_schema", "public")
                conn_id = config.get("default_postgres_conn_id", "default_postgres_conn")

                gcs_prefix = f"{dataset}/{table_name}/data_json/raw_data"
                file_template = f"{gcs_prefix}/raw_data_{{{{ ds_nodash }}}}.json"

                export_task = PostgresToGCSOperator(
                    task_id=f"export_{table_name}_to_gcs",
                    sql=f'SELECT {columns} FROM {schema}."{table_name}"',
                    postgres_conn_id=conn_id,
                    bucket=BUCKET_NAME,
                    filename=file_template,
                    export_format="json",
                )

                load_task = GCSToBigQueryOperator(
                    task_id=f"load_{table_name}_to_bq",
                    bucket=BUCKET_NAME,
                    source_objects=[file_template.replace("{{ ds_nodash }}", "*")],
                    destination_project_dataset_table=f"talabat_labs.{dataset}.staging_{table_name}",
                    source_format="NEWLINE_DELIMITED_JSON",
                    write_disposition="WRITE_TRUNCATE",
                    autodetect=True,
                )

                export_task >> load_task

        data_tasks()

    return dag_definition()


# Load and process each YAML config file
for config_path in load_yaml_configs():
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    required_keys = ["dag_id", "tables"]
    if not all(key in config for key in required_keys):
        raise KeyError(f"Missing keys in config file: {config_path}")

    raw_dag_id = config["dag_id"]
    dag_id = f"osama_{raw_dag_id}"

    schedule = config.get("schedule_interval", None)
    tables = config["tables"]
    default_args = build_default_args(config)

    globals()[dag_id] = create_dag(dag_id, schedule, tables, default_args, config)
