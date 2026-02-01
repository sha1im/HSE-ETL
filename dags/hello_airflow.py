from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import get_current_context

def print_context():
    ctx = get_current_context()

    run_id = ctx["run_id"]
    logical_date = ctx["logical_date"]
    dag_id = ctx["dag"].dag_id
    task_id = ctx["task"].task_id

    print(f"Run id: {run_id}")
    print (f"logical date: {logical_date}")
    print (f"DAG id: {dag_id}")
    print (f"task id: {task_id}")

with DAG (
        'hello_airflow',
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        tags=["homework",  "test"],
        ) as dag:
    
    t1 = PythonOperator(
        task_id="print_context",
        python_callable=print_context
    )

    t2 = BashOperator(
        task_id="hello",
        bash_command="echo 'Hello from Airflow'"
    )

    t1 >> t2