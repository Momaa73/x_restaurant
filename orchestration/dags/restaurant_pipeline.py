from pathlib import Path
import json
import random
from datetime import datetime, timedelta
from faker import Faker

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pendulum import timezone

# ----- הגדרות כלליות -----
faker = Faker()
MSG_DIR = Path("/opt/streaming/messages")


# ----- פונקציות ליצירת דאטה רנדומלי -----
def generate_checkins(n=3):
    return [
        {
            "checkin_id": random.randint(1000, 9999),
            "customer_name": faker.name(),
            "phone_number": faker.phone_number(),
            "branch_id": random.choice([101, 102]),
            "table_id": random.randint(200, 210),
            "is_prebooked": random.choice([True, False]),
            "checkin_date": datetime.now().strftime("%Y-%m-%d"),
            "checkin_time": (datetime.now() - timedelta(minutes=random.randint(0, 120))).strftime("%H:%M"),
            "guests_count": random.randint(1, 6),
            "shift_manager": faker.name()
        }
        for _ in range(n)
    ]

def generate_feedback(n=3):
    return [
        {
            "feedback_id": random.randint(1000, 9999),
            "branch_id": random.choice([101, 102]),
            "customer_name": faker.name(),
            "phone_number": faker.phone_number(),
            "feedback_text": faker.sentence(nb_words=6),
            "rating": random.randint(1, 5),
            "dining_date": datetime.now().strftime("%Y-%m-%d"),
            "dining_time": (datetime.now() - timedelta(minutes=random.randint(0, 120))).strftime("%H:%M"),
            "submission_time": datetime.now().isoformat(timespec='seconds')
        }
        for _ in range(n)
    ]

def generate_reservations(n=3):
    return [
        {
            "reservation_id": random.randint(1000, 9999),
            "branch_id": random.choice([101, 102]),
            "customer_name": faker.name(),
            "phone_number": faker.phone_number(),
            "reservation_date": datetime.now().strftime("%Y-%m-%d"),
            "reservation_time": (datetime.now() + timedelta(minutes=random.randint(10, 120))).strftime("%H:%M"),
            "guests_count": random.randint(1, 8),
            "is_confirmed": random.choice([True, False])
        }
        for _ in range(n)
    ]

# ----- כתיבה לקבצי JSON -----
def write_json_files():
    MSG_DIR.mkdir(parents=True, exist_ok=True)
    mapping = {
        MSG_DIR / "checkins.json": generate_checkins(),
        MSG_DIR / "feedback.json": generate_feedback(),
        MSG_DIR / "reservations.json": generate_reservations(),
    }
    for path, rows in mapping.items():
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

# ----- הגדרות ה-DAG -----
default_args = {"owner": "moran", "retries": 0}

with DAG(
    dag_id="restaurant_pipeline",
    description="Generate → Kafka → Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2025, 7, 18, tzinfo=timezone("Asia/Jerusalem")),
    schedule="*/2 * * * *",  # ← נכון ל-Airflow 3
    catchup=False,
) as dag:

    gen_json = PythonOperator(
        task_id="generate_json_files",
        python_callable=write_json_files,
    )

    send_to_kafka = BashOperator(
        task_id="send_to_kafka",
        bash_command="python /opt/streaming/producer.py",
    )

    stream_to_bronze = BashOperator(
        task_id="stream_to_bronze",
        bash_command="python /opt/processing/spark/stream_to_bronze.py",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="python /opt/processing/spark/bronze_to_silver.py",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="python /opt/processing/spark/silver_to_gold.py",
    )

    # זרימת הדאג
    gen_json >> send_to_kafka >> stream_to_bronze >> bronze_to_silver >> silver_to_gold
