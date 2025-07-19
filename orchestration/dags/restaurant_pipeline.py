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
default_args = {
    "owner": "moran",
    "retries": 0,
    "email_on_failure": True,  # Send email on task failure
    "email": ["your@email.com"],  # Update to your real email
}

with DAG(
    dag_id="restaurant_pipeline",
    description="End-to-end restaurant analytics pipeline: Generate demo data → Kafka → Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2025, 7, 18, tzinfo=timezone("Asia/Jerusalem")),
    schedule="*/2 * * * *",  # Run every 2 minutes for demo
    catchup=False,
    max_active_runs=1,  # Only one DAG run at a time
    concurrency=1,      # Only one task at a time
) as dag:

    # Step 1: Generate demo JSON files for reservations, checkins, and feedback
    gen_json = PythonOperator(
        task_id="generate_json_files",
        python_callable=write_json_files,
        # Generates random data and writes to /opt/streaming/messages
        # Used as the source for the Kafka producer
    )

    # Step 2: Send generated JSON messages to Kafka topics using the Python producer
    send_to_kafka = BashOperator(
        task_id="send_to_kafka",
        bash_command="python /opt/streaming/producer.py",
        # Reads JSON files and sends each record to the appropriate Kafka topic
    )

    # Step 3: Spark streaming job reads from Kafka and writes raw data to Iceberg bronze tables
    stream_to_bronze = BashOperator(
        task_id="stream_to_bronze",
        bash_command="docker exec -e AIRFLOW_CTX_DAG_RUN_ID='{{ dag_run.run_id }}' spark-iceberg spark-submit /home/iceberg/spark/stream_to_bronze.py",
        # Handles late-arriving data, deduplication, and watermarking
    )

    # Step 4: Spark batch job transforms bronze data to silver (cleaning, deduplication, DQ checks)
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="docker exec spark-iceberg spark-submit /home/iceberg/spark/bronze_to_silver.py",
        # Cleans and enriches data, runs data quality checks, and upserts to silver tables
    )

    # Step 5: Spark batch job transforms silver data to gold (analytics-ready facts/dimensions)
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="docker exec spark-iceberg spark-submit /home/iceberg/spark/silver_to_gold.py",
        # Aggregates, joins, and builds fact/dimension tables for BI/reporting
    )

    # Define the pipeline flow
    gen_json >> send_to_kafka >> stream_to_bronze >> bronze_to_silver >> silver_to_gold
