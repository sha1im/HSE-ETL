import json
import csv

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


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

def LoadCsvToSql():
    

with DAG (
        'homework(2)',
        start_date=datetime(2025, 1, 1)
        schedule=None,
        catchup=False,
        tags=["Homework N2" , "JSON->SQL"],
        ) as dag:

    t1 = PythonOperator(
        task_id="Extract",
        python_callable=JsonToCsv,
        op_args=[
            "/opt/airflow/data/in/pets-data.json",
            "/opt/airflow/data/processed/pets-data.csv"
        ]
    )
    
    t2 = PythonOperator(
        task_id="Load",
        python_callable=LoadCsvToSql
    )