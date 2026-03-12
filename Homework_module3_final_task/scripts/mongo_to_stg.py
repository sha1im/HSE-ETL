from pymongo import MongoClient
import psycopg2


MONGO_URI = "mongodb://mongo:27017/"
MONGO_DB = "etl_db"

POSTGRES_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}


def extract_from_mongo():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    user_sessions = list(db.user_sessions.find({}, {"_id": 0}))
    support_tickets = list(db.support_tickets.find({}, {"_id": 0}))

    return user_sessions, support_tickets


def transform_user_sessions(user_sessions):
    result = []

    for row in user_sessions:
        result.append({
            "session_id": row["session_id"],
            "user_id": row["user_id"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "pages_count": len(row.get("pages_visited", [])),
            "actions_count": len(row.get("actions", [])),
            "device_type": row["device_type"],
        })

    return result


def transform_support_tickets(support_tickets):
    result = []

    for row in support_tickets:
        result.append({
            "ticket_id": row["ticket_id"],
            "user_id": row["user_id"],
            "status": row["status"],
            "issue_type": row["issue_type"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "message_count": row["message_count"],
        })

    return result


def load_to_stg(user_sessions, support_tickets):
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE stg.user_sessions;")
    cur.execute("TRUNCATE TABLE stg.support_tickets;")

    for row in user_sessions:
        cur.execute(
            """
            INSERT INTO stg.user_sessions (
                session_id, user_id, start_time, end_time, pages_count, actions_count, device_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row["session_id"],
                row["user_id"],
                row["start_time"],
                row["end_time"],
                row["pages_count"],
                row["actions_count"],
                row["device_type"],
            )
        )

    for row in support_tickets:
        cur.execute(
            """
            INSERT INTO stg.support_tickets (
                ticket_id, user_id, status, issue_type, created_at, updated_at, message_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row["ticket_id"],
                row["user_id"],
                row["status"],
                row["issue_type"],
                row["created_at"],
                row["updated_at"],
                row["message_count"],
            )
        )

    conn.commit()
    cur.close()
    conn.close()


def main():
    user_sessions_raw, support_tickets_raw = extract_from_mongo()

    user_sessions_transformed = transform_user_sessions(user_sessions_raw)
    support_tickets_transformed = transform_support_tickets(support_tickets_raw)

    load_to_stg(user_sessions_transformed, support_tickets_transformed)

    print(f"Loaded {len(user_sessions_transformed)} rows into stg.user_sessions")
    print(f"Loaded {len(support_tickets_transformed)} rows into stg.support_tickets")


if __name__ == "__main__":
    main()