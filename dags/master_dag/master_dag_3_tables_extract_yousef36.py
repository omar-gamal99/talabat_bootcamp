from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime


lis = ["customers_db_extract","orders_db_extract","products_db_extract"]
lis2 = ["customers_db","orders_db","products_db"]
with DAG(
    dag_id="master_dag_3_tables_extract_yousef36",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["master_dag_yousef36"],
) as dag:
    for i,j in zip(lis,lis2):

        trigger_ingest = TriggerDagRunOperator(
            task_id=f"trigger_ingest_dag_take_data_from_postgres{j}_to_bq",
            trigger_dag_id=f"{i}",
            wait_for_completion=True,  # Waits for the child to finish
            poke_interval=60,
            reset_dag_run=True,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )
        

        trigger_ingest 
