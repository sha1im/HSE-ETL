from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG (
        dag_id = "homework_3",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        tags=["Homework N3" , "kaggle-dataset"],
        ) as dag:
    
    extract = BashOperator(
        task_id="extract_kaggle_dataset",
        cwd="/opt/airflow/data/in",
        bash_command="""
        set -euo pipefail

        echo "Starting extract from Kaggle..."

        CSV_FILE="IOT-temp.csv"

        if [ -f "$CSV_FILE" ]
        then
            rm "$CSV_FILE"
        fi
            
        kaggle datasets download atulanandjha/temperature-readings-iot-devices --unzip

        if [ ! -f "$CSV_FILE" ]
        then
            echo "ERROR: CSV file not found after unzip"
            ls -l
            exit 1
        fi

        echo "Extract completed successfully"
        echo "CSV file location: $(pwd)/$CSV_FILE"
        """
    )

    load = SQLExecuteQueryOperator(
        task_id="load_csv_to_PSql",
        conn_id="Postgres_airflow",
        sql="""
            DROP TABLE IF EXISTS tmp_iot_temp;

            CREATE TABLE tmp_iot_temp (
                id TEXT,
                room_id TEXT,
                noted_date TEXT,
                temp DOUBLE PRECISION,
                out_in VARCHAR(3)
            );

            COPY tmp_iot_temp FROM '/data/in/IOT-temp.csv'
            WITH (FORMAT csv, DELIMITER ',', HEADER true);
        """
    )

    transform = SQLExecuteQueryOperator(
        task_id="transform_dataset",
        conn_id="Postgres_airflow",
        sql="""
            DROP MATERIALIZED VIEW IF EXISTS top_days;
            DROP TABLE IF EXISTS iot_temp;
        
            CREATE TABLE iot_temp (
                id TEXT PRIMARY KEY,
                room_id TEXT,
                noted_date DATE,
                temp DOUBLE PRECISION,
                out_in VARCHAR(2)
                CHECK (out_in = 'In')
            );

            INSERT INTO iot_temp
            SELECT id, room_id,
            to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI')::date AS noted_date,
            temp, out_in FROM tmp_iot_temp
            WHERE out_in = 'In';

            DROP TABLE tmp_iot_temp;

            WITH bounds AS (
                SELECT 
                    percentile_cont(0.05) WITHIN GROUP (ORDER BY temp) AS lower_bound,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY temp) AS upper_bound
                FROM iot_temp
            )
            DELETE FROM iot_temp
            WHERE temp < (SELECT lower_bound FROM bounds)
            OR temp > (SELECT upper_bound FROM bounds);
        """
    )

    compute = SQLExecuteQueryOperator(
        task_id="compute_top_days",
        conn_id="Postgres_airflow",
        sql="""
            CREATE MATERIALIZED VIEW top_days AS
            WITH hottest_days AS (
                SELECT noted_date,
                round(avg(temp)::numeric, 1) AS avg_temp,
                'hottest' AS category
                FROM iot_temp
                GROUP BY noted_date
                ORDER BY avg_temp DESC
                LIMIT 5
            ),
            coldest_days AS (
                SELECT noted_date,
                round(avg(temp)::numeric, 1) AS avg_temp,
                'coldest' AS category
                FROM iot_temp
                GROUP BY noted_date
                ORDER BY avg_temp ASC
                LIMIT 5
            )
            SELECT * FROM hottest_days
            UNION ALL
            SELECT * FROM coldest_days
            ORDER BY category DESC, avg_temp DESC;
        """
    )

    extract >> load >> transform >> compute