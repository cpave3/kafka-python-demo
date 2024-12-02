from confluent_kafka import Consumer, Producer
import json
import sqlite3

# SQLite database for persistent storage
db_path = "event_store.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Initialize SQLite tables if they do not exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,
    amount REAL NOT NULL
)
""")
conn.commit()

# Kafka Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': "balance-consumer-group",
    'auto.offset.reset': 'earliest'  # Start from the earliest message for new consumers
}

consumer = Consumer(consumer_conf)

# Kafka Producer configuration (if needed)
producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_conf)

# Rebuild state from the SQLite database
def rebuild_state():
    print("Rebuilding state from persisted events...")
    account_events = {}
    cursor.execute("SELECT account_id, amount FROM transactions")
    rows = cursor.fetchall()

    for account_id, amount in rows:
        if account_id not in account_events:
            account_events[account_id] = []
        account_events[account_id].append({'account_id': account_id, 'amount': amount})

    print(f"State rebuilt: {account_events}")
    return account_events

# Initialize state
account_events = rebuild_state()

# Function to apply event and update the SQLite database
def apply_event(event):
    account_id = event['account_id']
    amount = event['amount']

    # Persist the event to SQLite
    cursor.execute("INSERT INTO transactions (account_id, amount) VALUES (?, ?)", (account_id, amount))
    conn.commit()

    # Append the event to in-memory state
    if account_id not in account_events:
        account_events[account_id] = []
    account_events[account_id].append(event)

    # Recalculate the balance
    balance = sum(e['amount'] for e in account_events[account_id])
    print(f"Account ID: {account_id} - Balance: {balance}")

# Function to handle incoming events
def handle_event(msg):
    event = json.loads(msg.value().decode('utf-8'))
    apply_event(event)

# Subscribe to the 'transactions' topic
consumer.subscribe(['transactions'])

print("Consumer started with persistent storage, waiting for transactions...")

# Consume messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0)  # Poll for new messages every second

        if msg is None:  # No new message
            continue
        if msg.error():  # Handle Kafka errors
            print(f"Consumer error: {msg.error()}")
            continue

        # Process the event
        handle_event(msg)

finally:
    # Close the consumer and database connection gracefully
    consumer.close()
    conn.close()
