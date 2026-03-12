import random
from datetime import datetime, timedelta
from pymongo import MongoClient


MONGO_URI = "mongodb://mongo:27017/"
DB_NAME = "etl_db"


def random_datetime(start: datetime, end: datetime) -> datetime:
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


def generate_user_sessions(n: int = 5000) -> list[dict]:
    device_types = ["mobile", "desktop", "tablet"]
    pages = ["home", "catalog", "product", "cart", "checkout", "profile", "support"]
    actions = ["view", "click", "scroll", "add_to_cart", "remove_from_cart", "purchase"]

    data = []
    start_period = datetime.now() - timedelta(days=60)
    end_period = datetime.now()

    for i in range(1, n + 1):
        start_time = random_datetime(start_period, end_period)
        duration_minutes = random.randint(1, 180)
        end_time = start_time + timedelta(minutes=duration_minutes)

        pages_visited = random.sample(pages, k=random.randint(1, min(5, len(pages))))
        session_actions = random.choices(actions, k=random.randint(1, 10))

        data.append({
            "session_id": i,
            "user_id": random.randint(1, 1000),
            "start_time": start_time,
            "end_time": end_time,
            "pages_visited": pages_visited,
            "device_type": random.choice(device_types),
            "actions": session_actions,
        })

    return data


def generate_support_tickets(n: int = 2000) -> list[dict]:
    statuses = ["open", "in_progress", "closed"]
    issue_types = ["payment", "delivery", "account", "technical", "refund"]

    data = []
    start_period = datetime.now() - timedelta(days=60)
    end_period = datetime.now()

    for i in range(1, n + 1):
        created_at = random_datetime(start_period, end_period)

        status = random.choices(
            ["open", "in_progress", "closed"],
            weights=[0.2, 0.3, 0.5],
            k=1
        )[0]

        if status == "closed":
            updated_at = created_at + timedelta(hours=random.randint(1, 72))
        elif status == "in_progress":
            updated_at = created_at + timedelta(hours=random.randint(1, 24))
        else:
            updated_at = created_at

        data.append({
            "ticket_id": i,
            "user_id": random.randint(1, 1000),
            "status": status,
            "issue_type": random.choice(issue_types),
            "created_at": created_at,
            "updated_at": updated_at,
            "message_count": random.randint(1, 15),
        })

    return data


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    user_sessions = generate_user_sessions()
    support_tickets = generate_support_tickets()

    db.user_sessions.delete_many({})
    db.support_tickets.delete_many({})

    db.user_sessions.insert_many(user_sessions)
    db.support_tickets.insert_many(support_tickets)

    print(f"Inserted {len(user_sessions)} documents into user_sessions")
    print(f"Inserted {len(support_tickets)} documents into support_tickets")


if __name__ == "__main__":
    main()