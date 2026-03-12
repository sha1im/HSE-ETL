import psycopg2


POSTGRES_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}


def build_marts():
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE mart.user_activity;")
    cur.execute("TRUNCATE TABLE mart.support_performance;")

    cur.execute(
        """
        INSERT INTO mart.user_activity (
            user_id,
            sessions_count,
            avg_session_duration_minutes,
            total_pages_count,
            total_actions_count
        )
        SELECT
            user_id,
            COUNT(*) AS sessions_count,
            ROUND(AVG(session_duration_minutes)::numeric, 2) AS avg_session_duration_minutes,
            SUM(pages_count) AS total_pages_count,
            SUM(actions_count) AS total_actions_count
        FROM dds.user_sessions
        GROUP BY user_id;
        """
    )

    cur.execute(
        """
        INSERT INTO mart.support_performance (
            status,
            issue_type,
            tickets_count,
            avg_resolution_time_hours,
            open_tickets_count
        )
        SELECT
            status,
            issue_type,
            COUNT(*) AS tickets_count,
            ROUND(AVG(resolution_time_hours)::numeric, 2) AS avg_resolution_time_hours,
            SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_tickets_count
        FROM dds.support_tickets
        GROUP BY status, issue_type;
        """
    )

    conn.commit()
    cur.close()
    conn.close()


def main():
    build_marts()
    print("Mart tables were successfully built from DDS")


if __name__ == "__main__":
    main()