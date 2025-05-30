from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='trigger_postgres_to_bq_john',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['trigger', 'postgres_to_bq_john_trigger'],
    doc_md="""
    ### Trigger DAG for postgres_to_bigquery_john
    This DAG is responsible for triggering the `postgres_to_bigquery_john` DAG.
    """
) as dag:

    trigger_target_dag = TriggerDagRunOperator(
        task_id='trigger_postgres_to_bq_john_task',
        trigger_dag_id='postgres_to_bigquery_john',
    )
