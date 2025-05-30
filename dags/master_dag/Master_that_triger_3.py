
"""
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='master_dag_trigers_3',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Change if you want this to run on a schedule
    catchup=False,
    tags=['master'],
) as dag:

    trigger_dag_1 = TriggerDagRunOperator(
        task_id='trigger_dag_1',
        trigger_dag_id='dag_1',  # Replace with your actual DAG ID
    )

    trigger_dag_2 = TriggerDagRunOperator(
        task_id='trigger_dag_2',
        trigger_dag_id='dag_2',  # Replace with your actual DAG ID
    )

    trigger_dag_3 = TriggerDagRunOperator(
        task_id='trigger_dag_3',
        trigger_dag_id='dag_3',  # Replace with your actual DAG ID
    )

    # Run them in parallel
    [trigger_dag_1, trigger_dag_2, trigger_dag_3]
    """
######################

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

# List of DAGs you want to trigger
dag_ids_to_trigger = ['customers_db_extract', 'orders_db_extract', 'products_db_extract']

with DAG(
    dag_id='master_dag_moessam',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['master'],
) as dag:

    trigger_tasks = []

    for dag_id in dag_ids_to_trigger:
        trigger = TriggerDagRunOperator(
            task_id=f'trigger_{dag_id}',
            trigger_dag_id=dag_id,
            wait_for_completion=False,  # Set to True if needed
        )
        trigger_tasks.append(trigger)
"""
    # Optionally set sequential triggering
    for i in range(1, len(trigger_tasks)):
        trigger_tasks[i - 1] >> trigger_tasks[i]
"""