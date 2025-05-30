from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='trigg_api_to_bq_gcp_maya',
    start_date=days_ago(1),
    schedule_interval=None,  # Manual only
    catchup=False,
    tags=['trigger', 'api', 'bigquery'],
) as dag:

    trigger_task = TriggerDagRunOperator(
        task_id='trigg_api_to_bq_gcp_maya_dag',
        trigger_dag_id='api_to_bq_gcp_maya',  # 👈 Must match your actual DAG name
    )

    trigger_task
