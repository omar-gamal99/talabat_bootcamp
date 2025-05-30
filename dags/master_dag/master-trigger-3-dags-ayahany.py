from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

POSTGRES_CONN_ID = "postgress-conn-aya-hany"
## Define the default arguments for the DAG
default_args = {
    'owner': 'ayah',
    'start_date': datetime(2025, 5, 23),
    'retries': 1,  
    'retry_delay': timedelta(minutes=2), 

}
dag_ids_to_trigger = ["customers_db_extract", "orders_db_extract", "products_db_extract"]

with DAG(
    dag_id="TRIGGER-3-DAGs",
    default_args=default_args,
    tags=["master", "trigger"]
) as dag:

    for dag_id in dag_ids_to_trigger:
        trigger = TriggerDagRunOperator(
            task_id=f"TRIGGER-{dag_id}",
            trigger_dag_id= dag_id,  
        )

    

    trigger