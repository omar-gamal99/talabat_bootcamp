from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="trigger_all_dags_asmaa",
    start_date=datetime(2025, 5, 30),
    schedule_interval="@once",  
    catchup=False,
    description="Trigger all DAGs together",
    tags=["trigger", "Asmaa"],
) as master_dag:

    trigger_dag_1 = TriggerDagRunOperator(
        task_id="trigger_customer_db_extract",
        trigger_dag_id="customer_db_extract",
    )

    trigger_dag_2 = TriggerDagRunOperator(
        task_id="trigger_orders_db_extract",
        trigger_dag_id="orders_db_extract",
    )

    trigger_dag_3 = TriggerDagRunOperator(
        task_id="trigger_products_db_extract",
        trigger_dag_id="products_db_extract",
    )

    # Run all three in parallel
    [trigger_dag_1, trigger_dag_2, trigger_dag_3]
