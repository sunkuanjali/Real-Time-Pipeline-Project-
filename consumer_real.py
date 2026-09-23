from kafka import KafkaConsumer
import json, uuid

consumer = KafkaConsumer(
    'events-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id=f'real-{uuid.uuid4()}',
    value_deserializer=lambda m: json.loads(m.decode())
)

print("Real Consumer - Waiting for events on 'events-topic'...")

for msg in consumer:
    data = msg.value
    print(f"Processed Event: User {data['user_id']} - {data['event_type']} on {data['page']}")