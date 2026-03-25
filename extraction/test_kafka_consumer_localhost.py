import psycopg2

from kafka import KafkaConsumer
from datetime import datetime
from tkinter import ON
import json

from models import event_deserializer

server = 'localhost:9092'
topic_name = 'ny-traffic-events'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='traffic-events-database',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Listening to {topic_name} and writing to PostgreSQL...")

count = 0
for message in consumer:
    # print(f"Received message: {message.value}")
    trafficEvent = message.value
    count += 1
    if count % 100 == 0:
        print(f"Retrieved {count} rows...")

consumer.close()
