import json
import time
import psycopg2
from confluent_kafka import Consumer, KafkaError
import requests

# Kafka
KAFKA_BROKER = "localhost:9092"
TOPIC = "page_views"
GROUP_ID = "web_traffic_group"

# PostgreSQL
DB_NAME = "web_traffic"
DB_USER = "kafka_user"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"

# Kafka consumer
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'  # od pocetok
})

consumer.subscribe([TOPIC])

# Database
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cursor = conn.cursor()

# Stats
page_views_count = {}
unique_users = {}

FLASK_URL = 'http://localhost:5000/consumer_output'


def send_consumer_output_to_flask(output):
    try:
        print(f"Sending consumer output to Flask: {output}")
        requests.post(FLASK_URL, json=output)
    except Exception as e:
        print(f"Failed to send consumer output to Flask: {e}")

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

        event = json.loads(msg.value().decode('utf-8'))
        page = event["page"]
        user_id = event["user_id"]


        if page not in page_views_count:
            page_views_count[page] = 0
        page_views_count[page] += 1


        if page not in unique_users:
            unique_users[page] = set()
        unique_users[page].add(user_id)

        # prakja na postgres
        if int(time.time()) % 5 == 0:
            aggregated_data = []
            for page, count in page_views_count.items():
                unique_count = len(unique_users[page])

                # update db
                cursor.execute("""
                    INSERT INTO page_traffic (page, total_visits, unique_users, last_updated)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (page) DO UPDATE
                    SET total_visits = EXCLUDED.total_visits,
                        unique_users = EXCLUDED.unique_users,
                        last_updated = NOW();
                """, (page, count, unique_count))

                aggregated_data.append({
                    "page": page,
                    "total_visits": count,
                    "unique_users": unique_count
                })

            conn.commit()
            print("\n--- Aggregated Data Stored in PostgreSQL ---")
            for page, count in page_views_count.items():
                print(f"Page: {page} | Total Visits: {count} | Unique Users: {len(unique_users[page])}")
            send_consumer_output_to_flask(aggregated_data)

except KeyboardInterrupt:
    print("Analytics Consumer stopped.")
finally:
    consumer.close()
    cursor.close()
    conn.close()

