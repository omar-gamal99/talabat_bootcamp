import glob
import itertools
from datetime import datetime
import yaml
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import dag, task_group
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)


config_files = list(
    itertools.chain(
        glob.glob(
            "./gcs/dags/dag_factory/TEMP/customers_db/*.yaml",
            recursive=True,
        ),
        glob.glob(
            "./gcs/dags/dag_factory/TEMP/orders_db/*.yaml",
            recursive=True,
        ),
        glob.glob(
            "./gcs/dags/dag_factory/TEMP/products_db/*.yaml",
            recursive=True,
        ),
        glob.glob(
            "./gcs/dags/dag_factory/TEMP/vendors_db_dummy/*.yaml",
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

    with DAG(
        dag_id=f"{dag_id}_dag_yousef36kk",
        description=config["description"],
        start_date=datetime(2023, 1, 1),
        schedule_interval=schedule,
        catchup=False,
        tags=[f"{dag_id}_dag_yousef36"],
        default_args=default_args

    ) as dag:
        for table in tables:
            table_name = table["table_name"]
            columns_select = table.get("columns", "*")
            source_table_schema = config.get("default_source_schema", "public")
            
            postgres_conn_id = (
                config.get("default_postgres_conn_id", "default_postgres_conn"),
            )

            postgres_to_gcs = PostgresToGCSOperator(
                task_id=f"{table_name}_postgres_to_gcs",
                postgres_conn_id=postgres_conn_id,
                sql=f"select {columns_select} from {source_table_schema}.{table}",
                bucket="talabat-labs-postgres-to-gcs",
                filename=f"yousef36/yousef36{table_name}.csv",
                export_format="csv",
            )

            load_csv = GCSToBigQueryOperator(
                task_id=f"gcs_{table_name}_to_bigquery_example",
                bucket="talabat-labs-postgres-to-gcs",
                source_objects=[f"yousef36/yousef36{table_name}.csv"],
                destination_project_dataset_table=f"talabat-labs-3927.landing.{table_name}-yousefkk",
                create_disposition='CREATE_IF_NEEDED',
                write_disposition="WRITE_TRUNCATE",
                autodetect=True,source_format="CSV",
                skip_leading_rows=1,
            )

            postgres_to_gcs >> load_csv

    #globals()[dag_id] = generate_dag(dag_id, schedule, tables, default_args, config)
