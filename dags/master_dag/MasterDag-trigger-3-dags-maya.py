from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

dag_ids = [
    'customers_db_extract',
    'orders_db_extract',
    'products_db_extract'
]

with DAG(
    dag_id='master_dag_trigger_3_dags_maya',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['master', 'parallel', 'trigger'],
) as dag:

    for dag_id_to_trigger in dag_ids:
        TriggerDagRunOperator(
            task_id=f'trigger_{dag_id_to_trigger}',
            trigger_dag_id=dag_id_to_trigger,
        )
