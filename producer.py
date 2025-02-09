import json
import random
import time
from confluent_kafka import Producer

# kafka
KAFKA_BROKER = "localhost:9092"
TOPIC = "page_views"

# Kafka producer
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# stranici koi gi koristat korisnicite
pages = ["/home", "/blog", "/product", "/contact"]

def generate_event():
    """Generate a random web traffic event."""
    return {
        "user_id": random.randint(1, 100),  # random user ID
        "page": random.choice(pages),       # random stranica
        "timestamp": int(time.time())       #  timestamp
    }

def delivery_report(err, msg):
    """Callback function to check delivery status."""
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message sent to {msg.topic()} [{msg.partition()}]")

# Postojano prakjanje na nastani
while True:
    event = generate_event()
    producer.produce(TOPIC, key=str(event["user_id"]), value=json.dumps(event), callback=delivery_report)
    producer.flush()
    time.sleep(random.uniform(0.5, 2))

