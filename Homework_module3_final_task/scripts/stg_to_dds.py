import psycopg2


POSTGRES_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}


def build_dds():
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE dds.user_sessions;")
    cur.execute("TRUNCATE TABLE dds.support_tickets;")

    cur.execute(
        """
        INSERT INTO dds.user_sessions (
            session_id,
            user_id,
            start_time,
            end_time,
            pages_count,
            actions_count,
            device_type,
            session_duration_minutes
        )
        SELECT
            session_id,
            user_id,
            start_time,
            end_time,
            pages_count,
            actions_count,
            device_type,
            EXTRACT(EPOCH FROM (end_time - start_time)) / 60 AS session_duration_minutes
        FROM stg.user_sessions;
        """
    )

    cur.execute(
        """
        INSERT INTO dds.support_tickets (
            ticket_id,
            user_id,
            status,
            issue_type,
            created_at,
            updated_at,
            message_count,
            resolution_time_hours
        )
        SELECT
            ticket_id,
            user_id,
            status,
            issue_type,
            created_at,
            updated_at,
            message_count,
            ROUND((EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600)::numeric, 2) AS resolution_time_hours
        FROM stg.support_tickets;
        """
    )

    conn.commit()
    cur.close()
    conn.close()


def main():
    build_dds()
    print("DDS tables were successfully built from STG")


if __name__ == "__main__":
    main()