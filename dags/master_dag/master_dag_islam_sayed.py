from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="masterdag_trigger_postgre_to_bq_islam_sayed",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as master_dag:

    trigger_child_dag = TriggerDagRunOperator(
        task_id="trigger_postgre_to_bq_dag",
        trigger_dag_id="orders_db_transfer_ziad",
        wait_for_completion=True  # Optional: waits until the child DAG finishes
    )
    trigger_child_dag 
