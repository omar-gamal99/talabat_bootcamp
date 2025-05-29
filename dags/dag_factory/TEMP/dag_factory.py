import glob
import itertools
from datetime import datetime
import yaml
from airflow.decorators import dag, task_group
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)


bucket_name = "talabat-labs-data-platform-airflow-maindb-exports"


def generate_dag(dag_id, schedule, tables, default_args, config):
    @dag(
        dag_id=dag_id,
        schedule_interval=schedule,
        default_args=default_args,
        concurrency=config.get("concurrency", 10),
        max_active_runs=config.get("max_active_runs", 1),
        description=config.get("description", ""),
        catchup=False,
    )
    def extract_from_db():
        @task_group("backend_gcs_bq_tasks")
        def backend_gcs_bq_tasks():
            for table_data in tables:
                table_name = table_data["table_name"]
                columns_select = table_data.get("columns", "*")
                dataset = config.get("default_dataset", "data_platform_export")
                source_table_schema = config.get("default_source_schema", "public")
                db_type = config.get("default_db_type", "PostgreSQL")
                postgres_conn_id = (
                    config.get("default_postgres_conn_id", "default_postgres_conn"),
                )

                base_path = f"{dataset}/{table_name}/data_json/raw_data/"
                filename_path = f"{base_path}raw_data/"

                transfer_postgres_to_gcs = PostgresToGCSOperator(
                    task_id=f"{table_name}_to_gcs",
                    sql=f"""select {columns_select} from {source_table_schema}."{table_name}""",
                    postgres_conn_id=postgres_conn_id,
                    bucket=bucket_name,
                    filename=filename_path + "{}.json",
                    export_format="json",
                )

                load_gcs_to_bigquery = GCSToBigQueryOperator(
                    task_id=f"From_GCS_To_Staging_{table_name}_BQ",
                    bucket=bucket_name,
                    source_objects=[filename_path + "*.json"],
                    destination_project_dataset_table=f"talabat_labs.{dataset}.staging_{table_name}",
                    source_format="NEWLINE_DELIMITED_JSON",
                    write_disposition="WRITE_TRUNCATE",
                )

                (transfer_postgres_to_gcs >> load_gcs_to_bigquery)

        backend_gcs_bq_tasks()

    return extract_from_db()


config_files = list(
    itertools.chain(
        glob.glob(
            "./gcs/dags/dag_factory/customers_db/TEMP/*.yaml",
            recursive=True,
        ),
        glob.glob(
            "./gcs/dags/dag_factory/orders_db/TEMP/*.yaml",
            recursive=True,
        ),
        glob.glob(
            "./gcs/dags/dag_factory/products_db/TEMP/*.yaml",
            recursive=True,
        ),
    )
)

for config_path in config_files:
    with open(config_path, "r") as stream:
        config = yaml.safe_load(stream)

    required_keys = ["dag_id", "tables"]
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise KeyError(
            f"Required keys {missing_keys} are missing in the YAML file: {config_path}"
        )

    dag_id = config["dag_id"]
    schedule = config.get("schedule_interval", None)
    tables = config["tables"]

    default_args = {
        "owner": config.get("default_args", {}).get("owner", "DE team"),
        "email": config.get("default_args", {}).get(
            "email", ["data.eng.team@talabat.com"]
        ),
        "start_date": datetime.strptime(
            config.get("default_args", {}).get("start_date", "2024-05-02"), "%Y-%m-%d"
        ),
        "depends_on_past": config.get("default_args", {}).get("depends_on_past", False),
        "catchup": config.get("default_args", {}).get("catchup", False),
        "retries": config.get("default_args", {}).get("retries", 1),
        "email_on_failure": config.get("default_args", {}).get(
            "email_on_failure", True
        ),
        "gcp_conn_id": config.get("default_args", {}).get(
            "gcp_conn_id", "bigquery_default"
        ),
        "max_active_runs": config.get("default_args", {}).get("max_active_runs", 1),
        "concurrency": config.get("default_args", {}).get("concurrency", 1),
    }

    globals()[dag_id] = generate_dag(dag_id, schedule, tables, default_args, config)
