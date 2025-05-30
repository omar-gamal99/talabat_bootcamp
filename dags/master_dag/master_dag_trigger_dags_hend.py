from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

dags=['customer_db_extract','orders_db_extract','products_db_extract']

with DAG(
    'trigger_dag_hend',
    start_date=datetime(2025, 5, 30),
    schedule_interval=None,#0 0 * * for 12 am
    catchup=False,
) as dag:
    
    for dag_id in dags:
        trigger = TriggerDagRunOperator(
            task_id=f'Dag_{dag_id}_dags_hend',
            trigger_dag_id=dag_id,
            wait_for_completion=True
        )