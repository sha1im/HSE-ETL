from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="mongo_to_postgres_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "mongo", "postgres"],
) as dag:

    mongo_to_stg = BashOperator(
        task_id="mongo_to_stg",
        bash_command="python /opt/airflow/scripts/mongo_to_stg.py",
    )

    stg_to_dds = BashOperator(
        task_id="stg_to_dds",
        bash_command="python /opt/airflow/scripts/stg_to_dds.py",
    )

    mongo_to_stg >> stg_to_dds