import json
import csv

from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


COLUMNS = ["name", "species", "favFoods", "birthYear", "photo"]

def JsonToCsv(input_path: str, output_path: str) -> None:
    with open(input_path, "r", encoding="utf-8") as input_file:
        data = json.load(input_file)

    with open(output_path, "w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file, delimiter="|")
        writer.writerow(COLUMNS)

        for pet in data.get("pets", []):
            row = [
                pet.get("name", ""),
                pet.get("species", ""),
                json.dumps(pet.get("favFoods", []), ensure_ascii=False),
                pet.get("birthYear", ""),
                pet.get("photo", ""),
            ]
            writer.writerow(row)

with DAG (
        'homework_2',
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        tags=["Homework N2" , "JSON->SQL"],
        ) as dag:

    t1 = PythonOperator(
        task_id="Transform",
        python_callable=JsonToCsv,
        op_args=[
            "/opt/airflow/data/in/pets-data.json",
            "/opt/airflow/data/processed/pets-data.csv"
        ]
    )
    
    t2 = SQLExecuteQueryOperator(
        task_id="Load",
        conn_id="Postgres_airflow",
        sql="""
            TRUNCATE TABLE pets;

            COPY pets FROM '/data/processed/pets-data.csv'
            WITH (FORMAT csv, DELIMITER '|', HEADER true, ENCODING 'UTF8');
        """
    )

    t1 >> t2