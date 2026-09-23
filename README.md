# Real-Time Data Pipeline | Python, Kafka, SQL

Built streaming pipeline ingesting 10k+ events/day via Kafka producers/consumers; processed with filtering & enrichment.

## Tech: Python, Kafka 3.7, Zookeeper, SQLite/SQL Server

### Run
1. Start Zookeeper & Kafka
2. Create topic: events-topic
3. Terminal 1: python consumer_sql.py
4. Terminal 2: python producer_real.py

Output: ✅ Stored in SQL: User 975 - click on /page/1

### Features
- Validation, deduplication, transformation
- SQL storage (events.db)
- Real-time processing