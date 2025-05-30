from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 5, 29),
}

with DAG(
    dag_id='DAG_Trigger_by_Khalifa',
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
    tags=['trigger', 'manual']
) as dag:

    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_pg_to_bq',
        trigger_dag_id='orders_db_transfer_ziad',
        wait_for_completion=True
    )
