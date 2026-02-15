from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG (
        dag_id="Homework_4_incremental",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        tags=["Homework N4" , "Incremental load to target table"],
    ):

    data_check=SQLExecuteQueryOperator(
        task_id="tables_checks",
        conn_id="Postgres_airflow",
        sql="""
        DO $$
        BEGIN
        IF to_regclass('homework.iot_temp') IS NULL THEN
            RAISE EXCEPTION 'Source table homework.iot_temp does not exist';
        END IF;

        IF to_regclass('homework.target_table') IS NULL THEN
            RAISE EXCEPTION 'Target table homework.target_table does not exist';
        END IF;
        END $$;
        """
    )
    
    load = SQLExecuteQueryOperator(
        task_id="load_to_target_table",
        conn_id="Postgres_airflow",
        sql="""
        INSERT INTO target_table (id, room_id, noted_date, temp, out_in)
        SELECT id, room_id, noted_date, temp, out_in FROM iot_temp
        WHERE noted_date >= CURRENT_DATE - '14 days'::interval 
        AND noted_date <= CURRENT_DATE
        ON CONFLICT (id) DO UPDATE
        SET room_id = EXCLUDED.room_id,
            noted_date = EXCLUDED.noted_date,
            temp = EXCLUDED.temp,
            out_in = EXCLUDED.out_in;
        """
    )

    data_check >> load