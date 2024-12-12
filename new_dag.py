from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def my_python_function():
    print("Hello from Python function!")

with DAG(
    'simple_python_dag',
    default_args=default_args,
    description='A simple DAG using PythonOperator',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 12),
    catchup=False,
) as dag:

    run_python = PythonOperator(
        task_id='run_python',
        python_callable=my_python_function,
    )

    run_python
