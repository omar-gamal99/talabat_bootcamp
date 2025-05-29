from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow import DAG


default_args = {
    "owner": "Data Engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 22),
    "bigquery_conn_id": "bigquery_default",
    "max_active_runs": 1,
    "retries": 0,
}
ext_date = "{{ execution_date }}"

dag = DAG(
    dag_id="trigger_dag_from_another_dag",
    description="All transfer DAGs",
    schedule_interval=None,
    concurrency=6,
    max_active_runs=1,
    default_args=default_args,
    tags=["master_transfer"],
    catchup=False,
)

TriggerDagRunOperator(
    task_id="trigger_orders_db_transfer_ziad",
    dag=dag,
    trigger_dag_id="orders_db_transfer_ziad",
    execution_date=ext_date,
    wait_for_completion=True,
    poke_interval=30,
    deferrable=True,
    trigger_rule="all_done",
)
