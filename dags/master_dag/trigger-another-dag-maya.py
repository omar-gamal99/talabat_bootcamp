from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='trigger_orders_db_extract_maya',
    start_date=days_ago(1),
    schedule_interval=None,  # Manual only
    catchup=False,
    tags=['trigger', 'api', 'bigquery'],
) as dag:

    trigger_task = TriggerDagRunOperator(
        task_id='trigger_orders_db_extract_dag',
        trigger_dag_id='orders_db_extract',  # 👈 Must match your actual DAG name
    )

    trigger_task
