from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode()
)

print("Real Producer started... sending events...")

for i in range(100):
    event = {
        "user_id": random.randint(1, 1000),
        "event_type": "click",
        "page": f"/page/{random.randint(1,5)}"
    }
    producer.send('events-topic', value=event)
    print(f"Sent: {event}")
    time.sleep(0.5)

producer.close()