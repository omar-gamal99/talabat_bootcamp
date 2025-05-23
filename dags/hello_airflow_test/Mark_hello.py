import dag_factory
def greet():
    print("Hello from Airflow!")

with DAG(
    dag_id="example_airflow_dag_mark",
    schedule=None,
    start_date=datetime(2025, 5, 23),
    catchup=False,
    tags=["example"],
) as dag:
    greet_task = PythonOperator(
        task_id="greet_task",
        python_callable=greet,
    )