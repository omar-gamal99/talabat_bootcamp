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


dag = DAG(
    dag_id="daily_transfer_run",
    description="All transfer DAGs",
    schedule_interval="05 00 * * *",
    concurrency=6,
    max_active_runs=1,
    default_args=default_args,
    tags=["master_transfer"],
    catchup=False,
)

ext_date = "{{ execution_date }}"


def create_trigger_task(trigger_dag_id):
    return TriggerDagRunOperator(
        task_id=trigger_dag_id,
        dag=dag,
        trigger_dag_id=trigger_dag_id,
        execution_date=ext_date,
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True,
        trigger_rule="all_done",
    )


with TaskGroup(group_id="P1_pipelines", dag=dag) as P1_pipelines:
    p1_dags = [
        "FIRST_DATABASE_DAG_ID",
        "SECOND_DATABASE_DAG_ID",
    ]
    for task_id in p1_dags:
        create_trigger_task(task_id)

with TaskGroup(group_id="P2_pipelines", dag=dag) as P2_pipelines:
    p2_dags = [
        "THIRD_DATABASE_DAG_ID",
        "FOURTH_DATABASE_DAG_ID",
    ]
    for task_id in p2_dags:
        create_trigger_task(task_id)
