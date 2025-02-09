import json
from confluent_kafka import Consumer, KafkaError

# Kafka 
KAFKA_BROKER = "localhost:9092"
TOPIC = "page_views"
GROUP_ID = "web_traffic_group"

# Kafka consumer
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'  # cita poraki od najranite
})

# Subscribe
consumer.subscribe([TOPIC])

print(f"Listening for messages on topic: {TOPIC}...")

try:
    while True:
        msg = consumer.poll(1.0) 
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break
        
        # print na poraka
        event = json.loads(msg.value().decode('utf-8'))
        print(f"Received event: {event}")

except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    consumer.close()

