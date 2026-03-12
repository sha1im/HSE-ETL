from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="build_datamarts",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mart", "analytics", "postgres"],
) as dag:

    build_marts = BashOperator(
        task_id="build_marts",
        bash_command="python /opt/airflow/scripts/build_marts.py",
    )