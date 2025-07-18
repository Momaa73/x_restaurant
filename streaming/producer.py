from kafka import KafkaProducer
import json
import time
from pathlib import Path

# Kafka from inside Docker = use service name
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# קבצים והטופיקים תואמים
files_topics = {
    "reservations.json": "reservations",
    "checkins.json": "checkins",
    "feedback.json": "feedback"
}

# נתיב אל תיקיית ההודעות
MSG_DIR = Path(__file__).resolve().parent / "messages"

for filename, topic in files_topics.items():
    file_path = MSG_DIR / filename
    if not file_path.exists():
        print(f"[WARNING] File not found: {file_path}")
        continue

    with open(file_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue  # דילוג על שורות ריקות
            try:
                data = json.loads(line)
                producer.send(topic, value=data)
                print(f"[✓] Sent to {topic}: {data}")
                time.sleep(0.2)
            except json.JSONDecodeError as e:
                print(f"[ERROR] Failed to parse line in {file_path}: {e}")

producer.flush()
print("✅ All messages sent.")
