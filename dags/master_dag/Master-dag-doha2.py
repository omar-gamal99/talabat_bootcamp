from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2025, 5, 30),
}

dag_ids = ["customers_db_extract", "orders_db_extract", "products_db_extract "]

with DAG(
    dag_id="master_dag_trigger_once_doha",
    default_args=default_args,
    schedule_interval=None,  # Run only when triggered manually
    catchup=False,
    tags=["master"],
) as dag:

    for dag_id in dag_ids:
        TriggerDagRunOperator(
            task_id=f"trigger_{dag_id}",
            trigger_dag_id=dag_id,
            wait_for_completion=False,
            reset_dag_run=True,
            dag=dag,
        )
