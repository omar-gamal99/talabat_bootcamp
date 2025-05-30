from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'elham',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag_ids=['customers_db_extract', 'orders_db_extract', 'products_db_extract']

with DAG(
    dag_id='master_dag_elhamshawkat',
    start_date=datetime(2025, 5, 28),
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:
    for dag_id in dag_ids:
        trigger = TriggerDagRunOperator(
            task_id=f'trigger_task_{dag_id}',
            trigger_dag_id=dag_id,
            conf={"comment": f"Triggered from master_dag for {dag_id}"},
            dag=dag,
            wait_for_completion=False
        )