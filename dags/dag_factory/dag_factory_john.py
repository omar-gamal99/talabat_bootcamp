import os
import yaml
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.utils.dates import days_ago

# --- Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
YAML_CONFIG_BASE_PATH = os.path.join(SCRIPT_DIR, 'dag_factory', 'TEMP')

DEFAULT_GCS_BUCKET = 'talabat-labs-postgres-to-gcs'
DEFAULT_POSTGRES_CONN_ID = 'postgress-conn-john'

def create_dag_from_config(dag_id, description, schedule_interval, concurrency, max_active_runs, dag_default_args,
                           tables_config, postgres_conn_id, source_schema, gcs_bucket_name):
    """
    Dynamically creates an Airflow DAG and its tasks based on the provided configuration.
    """
    dag = DAG(
        dag_id=dag_id,
        description=description,
        schedule_interval=schedule_interval,
        default_args=dag_default_args,
        concurrency=concurrency,
        max_active_runs=max_active_runs,
        catchup=dag_default_args.get('catchup', False),
        tags=['dynamic_yaml_generated', source_schema or 'general_extract', 'john_suffix']
    )

    with dag:
        if not tables_config:
            print(f"INFO: No tables defined for DAG: {dag_id}. DAG will have no tasks.")
            return dag

        for table_info in tables_config:
            table_name = table_info.get('table_name')
            if not table_name:
                print(f"WARNING: Skipping table in DAG {dag_id} due to missing 'table_name'.")
                continue

            columns = table_info.get('columns', '*')
            if isinstance(columns, list):
                columns_str = ", ".join(columns)
            else:
                columns_str = str(columns)

            sql_query = f'SELECT {columns_str} FROM {source_schema}.{table_name};'
            gcs_object_name = f"{dag_id}/{table_name}/{{{{ ds_nodash }}}}_{table_name}.json"

            PostgresToGCSOperator(
                task_id=f'extract_{table_name.replace("-", "_")}_to_gcs',
                postgres_conn_id=postgres_conn_id,
                sql=sql_query,
                bucket=gcs_bucket_name,
                filename=gcs_object_name,
                export_format='json',
            )
    return dag

def register_dags_from_yaml_files(base_path):
    """
    Scans for YAML files in the base_path, parses them, and creates Airflow DAGs.
    """
    print(f"INFO: Starting YAML DAG discovery in resolved base_path: {base_path}")
    if not os.path.exists(base_path):
        print(f"ERROR: The base_path for YAML files does not exist: {base_path}. No DAGs will be generated from YAML.")
        return

    found_yaml_files = 0
    created_dags_count = 0

    for root_dir, _, files_in_dir in os.walk(base_path):
        for file_name in files_in_dir:
            if file_name.lower().endswith(('.yaml', '.yml')):
                found_yaml_files += 1
                yaml_file_path = os.path.join(root_dir, file_name)
                print(f"INFO: Processing YAML file: {yaml_file_path}")

                try:
                    with open(yaml_file_path, 'r') as f:
                        config_from_yaml = yaml.safe_load(f)

                    if not isinstance(config_from_yaml, dict):
                        print(f"WARNING: Skipping {yaml_file_path}: content is not a valid YAML dictionary.")
                        continue

                    original_dag_id = config_from_yaml.get('dag_id')
                    if not original_dag_id:
                        print(f"WARNING: Skipping {yaml_file_path}: 'dag_id' is missing from YAML.")
                        continue
                    
                    dag_id_with_suffix = f"{original_dag_id}_john"
                    print(f"INFO: Original DAG ID from YAML: '{original_dag_id}', Generated DAG ID: '{dag_id_with_suffix}'")

                    dag_default_args = config_from_yaml.get('default_args', {})
                    if 'start_date' in dag_default_args:
                        try:
                            dag_default_args['start_date'] = datetime.strptime(str(dag_default_args['start_date']), '%Y-%m-%d')
                        except ValueError:
                            print(f"ERROR: Skipping DAG {dag_id_with_suffix} from {yaml_file_path}: Invalid 'start_date' format. Use YYYY-MM-DD.")
                            continue
                    else:
                        print(f"WARNING: For DAG {dag_id_with_suffix}: 'start_date' not in default_args. Using 'days_ago(1)'.")
                        dag_default_args['start_date'] = days_ago(1)
                    
                    if 'owner' not in dag_default_args:
                        dag_default_args['owner'] = 'DefaultOwner'

                    description = config_from_yaml.get('description', f"DAG for {original_dag_id}")
                    schedule_interval = config_from_yaml.get('schedule_interval', None)
                    concurrency = config_from_yaml.get('concurrency', 10)
                    max_active_runs = config_from_yaml.get('max_active_runs', 1)

                    postgres_conn_id = config_from_yaml.get('default_postgres_conn_id', DEFAULT_POSTGRES_CONN_ID)
                    source_schema = config_from_yaml.get('default_source_schema', 'public')
                    gcs_bucket_name = config_from_yaml.get('gcs_bucket', DEFAULT_GCS_BUCKET)
                    
                    if gcs_bucket_name == 'your-default-gcs-landing-bucket' and DEFAULT_GCS_BUCKET == 'your-default-gcs-landing-bucket':
                        print(f"CRITICAL WARNING for DAG {dag_id_with_suffix}: GCS bucket is still the placeholder '{gcs_bucket_name}'. This DAG will likely fail.")

                    tables_config = config_from_yaml.get('tables', [])
                    if not tables_config:
                        print(f"WARNING: For DAG {dag_id_with_suffix}: 'tables' array is missing or empty. DAG will have no tasks.")
                    
                    dag_object = create_dag_from_config(
                        dag_id_with_suffix, description, schedule_interval, concurrency, max_active_runs, dag_default_args,
                        tables_config, postgres_conn_id, source_schema, gcs_bucket_name
                    )
                    
                    globals()[dag_id_with_suffix] = dag_object
                    created_dags_count +=1
                    print(f"INFO: Successfully registered DAG: {dag_id_with_suffix}")

                except yaml.YAMLError as e:
                    print(f"ERROR: Parsing YAML file {yaml_file_path}: {e}")
                except Exception as e:
                    print(f"ERROR: Processing DAG configuration from {yaml_file_path} for {config_from_yaml.get('dag_id', 'Unknown DAG')}: {e}")
    
    print(f"INFO: Finished YAML DAG discovery. Found {found_yaml_files} YAML files. Successfully created {created_dags_count} DAGs.")

register_dags_from_yaml_files(YAML_CONFIG_BASE_PATH)