from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='master_trigger_dag',
    start_date=datetime(2025, 5, 30),
    schedule_interval=None,  # or any schedule you want
    catchup=False
) as dag:

    trigger_dag_1 = TriggerDagRunOperator(
        task_id='trigger_dag_1',
        trigger_dag_id='customers_db_extract',
    )

    trigger_dag_2 = TriggerDagRunOperator(
        task_id='trigger_dag_2',
        trigger_dag_id='orders_db_extract',
    )

    trigger_dag_3 = TriggerDagRunOperator(
        task_id='trigger_dag_3',
        trigger_dag_id='products_db_extract',
    )

    [trigger_dag_1, trigger_dag_2, trigger_dag_3] 
