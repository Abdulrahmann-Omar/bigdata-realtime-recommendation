"""Kafka producer for ratings-stream topic.

Emits ~20 events/sec with a 10% spike-injection towards one trending item
to exercise the alert path.
"""
import json, time, random, os, signal, sys
from datetime import datetime, timezone
from kafka import KafkaProducer
import pandas as pd

RATINGS_PATH = os.environ.get("RATINGS_PATH", "data/ml-1m/ratings.csv")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "ratings-stream")
SAMPLE_N = int(os.environ.get("SAMPLE_N", "50000"))
EVENT_INTERVAL = float(os.environ.get("EVENT_INTERVAL", "0.05"))

print(f"[producer] loading sample of {SAMPLE_N} from {RATINGS_PATH}")
df = pd.read_csv(RATINGS_PATH)
if len(df) > SAMPLE_N:
    df = df.sample(SAMPLE_N, random_state=1)
users = df.userId.unique().tolist()
items = df.movieId.unique().tolist()
trending_item = items[0]
print(f"[producer] {len(users)} users, {len(items)} items, trending item = {trending_item}")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: str(k).encode(),
    value_serializer=lambda v: json.dumps(v).encode(),
)

stopped = False
def stop(*_):
    global stopped
    stopped = True
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

print(f"[producer] producing to topic '{TOPIC}'...")
sent = 0
while not stopped:
    u = random.choice(users)
    if random.random() < 0.10:
        i = trending_item
        r = round(random.uniform(4.5, 5.0), 1)
    else:
        i = random.choice(items)
        r = round(random.uniform(1.0, 5.0), 1)
    evt = {
        "user_id": int(u),
        "item_id": int(i),
        "rating": float(r),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    producer.send(TOPIC, key=u, value=evt)
    sent += 1
    if sent % 500 == 0:
        print(f"[producer] sent {sent}")
    time.sleep(EVENT_INTERVAL)

producer.flush()
producer.close()
print(f"[producer] stopped after {sent} events")
