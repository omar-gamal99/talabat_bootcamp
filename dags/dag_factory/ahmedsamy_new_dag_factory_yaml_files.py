# import os
# import yaml
# from datetime import datetime
# from airflow import DAG
# from airflow.operators.bash import BashOperator


# #  Manually specify the full paths to each YAML config

# YAML_PATHS = [


#     "./gcs/dags/dag_factory/TEMP/customers_db/*.yaml",
#     "./gcs/dags/dag_factory/TEMP/orders_db/*.yaml",
#     "./gcs/dags/dag_factory/TEMP/products_db/*.yaml"


# ]

# # Load a single YAML config
# def load_yaml_config(filepath):
#     with open(filepath, "r") as f:
#         return yaml.safe_load(f)

# # Create a DAG from config
# def create_dag_from_config(config):
#     dag_id = config["dag_id"]
#     schedule = config.get("schedule_interval", "@daily")
#     start_date = datetime.fromisoformat(config.get("start_date", "2025-05-30"))

#     default_args = {
#         "start_date": start_date,
#     }

#     dag = DAG(
#         dag_id = dag_id,
#         schedule_interval=schedule,
#         default_args=default_args,
#         catchup=False,
#         tags=config.get("tags", []),
#     )

#     # tasks_dict = {}
#     with dag:
#         for table in config.get("tables", []):

#             table_name = table["table_name"]
#             columns = table["columns"]




#             export_to_gcs = PostgresToGCSOperator
#             (
#                 task_id="ahmedsamy_export_orders_to_gcs",
#                 postgres_conn_id=POSTGRES_CONN_ID,
#                 sql=f"SELECT {columns} FROM {table_name}",
#                 bucket="talabat-labs-postgres-to-gcs",
#                 filename=f"yousef36/yousef36{table_name}.csv",
#                 export_format="csv",
#             )

#             import_to_bigquery = GCSToBigQueryOperator

#             (
#                 task_id=f"ahmedsamy_gcs_{table_name}_to_bigquery_example",
#                 bucket="talabat-labs-postgres-to-gcs",
#                 source_objects=[f"yousef36/yousef36{table_name}.csv"],
#                 destination_project_dataset_table=f"talabat-labs-3927.landing.{table_name}-yousefkk",
#                 create_disposition='CREATE_IF_NEEDED',
#                 write_disposition="WRITE_TRUNCATE",
#                 autodetect=True,source_format="CSV",
#                 skip_leading_rows=1,
#             )
    

#             export_to_gcs >> import_to_bigquery

# # Loop over each path you define manually
# for yaml_path in YAML_PATHS:
#     if os.path.exists(yaml_path):
#         config = load_yaml_config(yaml_path)
#         dag = create_dag_from_config(config)
#         globals()[config["dag_id"]] = dag
#     else:
#         print(f" File not found: {yaml_path}")
        
        
        
        
# ---------------------------------------------



import os
import glob
import yaml
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Manually specify the full paths to each YAML config
YAML_PATHS = [
    "./gcs/dags/dag_factory/TEMP/customers_db/*.yaml",
    "./gcs/dags/dag_factory/TEMP/orders_db/*.yaml",
    "./gcs/dags/dag_factory/TEMP/products_db/*.yaml"
]

# POSTGRES_CONN_ID = "your_postgres_conn_id"  # Define your connection ID

# Load a single YAML config
def load_yaml_config(filepath):
    with open(filepath, "r") as f:
        return yaml.safe_load(f)

# Create a DAG from config
def create_dag_from_config(config):
    dag_id = config["dag_id"]
    schedule = config.get("schedule_interval", "@daily")
    start_date = datetime.fromisoformat(config.get("start_date", "2025-05-30"))

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

    with dag:
        for table in config.get("tables", []):
            table_name = table["table_name"]
            columns = table["columns"]

            export_to_gcs = PostgresToGCSOperator(
                task_id=f"ahmedsamy_dagfactory_export_{table_name}_to_gcs",
                postgres_conn_id=config.get("default_postgres_conn_id",None),
                sql=f"SELECT {columns} FROM {table_name}",
                bucket="talabat-labs-postgres-to-gcs",
                filename=f"yousef36/{table_name}.csv",
                export_format="csv",
            )

            import_to_bigquery = GCSToBigQueryOperator(
                task_id=f"ahmedsamy_dagfactory_load_{table_name}_to_bigquery",
                bucket="talabat-labs-postgres-to-gcs",
                source_objects=[f"yousef36/{table_name}.csv"],
                destination_project_dataset_table=f"talabat-labs-3927.landing.{table_name}_yousefkk",
                create_disposition='CREATE_IF_NEEDED',
                write_disposition="WRITE_TRUNCATE",
                autodetect=True,
                source_format="CSV",
                skip_leading_rows=1,
            )

            export_to_gcs >> import_to_bigquery

    return dag

# Loop over each path and create DAGs
for path_pattern in YAML_PATHS:
    for filepath in glob.glob(path_pattern):
        config = load_yaml_config(filepath)
        dag = create_dag_from_config(config)
        # globals()[config["dag_id"]] = dag
        
        # this comment
        # this comment


# ahmed_Samy_new_branch_factory_dag
