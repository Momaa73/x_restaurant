# streaming/producer.py
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


files_topics = {
    "messages/reservations.json": "reservations",
    "messages/checkins.json": "checkins",
    "messages/feedback.json": "feedback"
}


for file_path, topic in files_topics.items():
    with open(file_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue  # דילוג על שורות ריקות
            try:
                data = json.loads(line)
                producer.send(topic, value=data)
                print(f"Sent to {topic}: {data}")
                time.sleep(1)
            except json.JSONDecodeError as e:
                print(f"[ERROR] Failed to parse line in {file_path}: {e}")

