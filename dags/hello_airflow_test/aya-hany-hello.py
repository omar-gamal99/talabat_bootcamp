import DAG from airflow 

def greetings():
    print("Hi, there!")

default_args = {
    'owner' : 'ayah',
    'start_date' : dt.datetime(2025,5,23)


}
with DAG("greetings-dag", default_args = default_args) as greetings_dag:
    PythonOperator(
        task_id="greetings",
        python_callable=greetings
    )