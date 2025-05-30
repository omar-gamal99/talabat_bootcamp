from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='parent_trigger_dag_youmna',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as parent_dag:

    trigger_child = TriggerDagRunOperator(
        task_id='trigger_talabat_dag',
        trigger_dag_id='talabat-youmna',  
        wait_for_completion=False,      
        reset_dag_run=True,             
    )
