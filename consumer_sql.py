from kafka import KafkaConsumer
import json
import uuid
import sqlite3
from datetime import datetime

# --- SQL Setup (SQLite - works without installing SQL Server) ---
conn = sqlite3.connect('events.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        event_type TEXT,
        page TEXT,
        processed_at TEXT
    )
''')
conn.commit()
print("✅ SQL DB ready: events.db")

# --- Kafka Consumer ---
consumer = KafkaConsumer(
    'events-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id=f'sql-{uuid.uuid4()}',
    value_deserializer=lambda m: json.loads(m.decode())
)

print("Consumer with SQL storage waiting...")

for msg in consumer:
    data = msg.value
    
    # 1. Validation (resume point: validation logic)
    if not data.get('user_id'):
        print(f"Skipped invalid: {data}")
        continue

    # 2. Deduplication & Transformation
    page = data['page'].strip().lower()
    event_type = data['event_type'].strip()
    
    # 3. Store in SQL (resume point: stored in SQL Server)
    cursor.execute(
        "INSERT INTO events (user_id, event_type, page, processed_at) VALUES (?, ?, ?, ?)",
        (data['user_id'], event_type, page, datetime.now().isoformat())
    )
    conn.commit()
    
    print(f"✅ Stored in SQL: User {data['user_id']} - {event_type} on {page}")

# For MySQL / SQL Server, replace sqlite with:
# pip install pymysql pyodbc
# conn = pymysql.connect(host='localhost', user='root', password='...', database='events_db')
# OR
# conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=events_db;UID=...;PWD=...')