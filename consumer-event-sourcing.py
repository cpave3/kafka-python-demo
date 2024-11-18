from confluent_kafka import Consumer, Producer
import sys
import json
from collections import defaultdict
import uuid

# Create a unique id so that we start from the earliest event each time the consumer starts.
# In real life, you'd likely want to persist events somewhere for state reconstruction instead.
unique_id = uuid.uuid4()

# Kafka Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': f"balance-consumer-group-{unique_id}",
    'auto.offset.reset': 'earliest'  # Start from the earliest message to replay events
}

consumer = Consumer(consumer_conf)

# Kafka Producer configuration (to send events back if needed)
producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_conf)

# In-memory storage of transaction history for each account
account_events = defaultdict(list)

# Function to apply event and update balance
def apply_event(event):
    account_id = event['account_id']
    amount = event['amount']
    
    # Append the event to the account's event history
    account_events[account_id].append(event)
    
    # Recalculate the balance by replaying the events
    balance = sum(event['amount'] for event in account_events[account_id])

    # Print the account balance after applying the event
    print(f"Account ID: {account_id} - Balance (via event sourcing): {balance}")

# Function to handle incoming events
def handle_event(msg):
    event = json.loads(msg.value().decode('utf-8'))
    apply_event(event)

# Subscribe to the 'transactions' topic where transaction events are stored
consumer.subscribe(['transactions'])

print("Consumer started with Event Sourcing, waiting for transactions...")

# Continuously consume messages and apply event sourcing logic
try:
    while True:
        msg = consumer.poll(1.0)  # Poll for new messages every second

        if msg is None:  # No new message
            continue
        if msg.error():  # Handle Kafka errors
            print(f"Consumer error: {msg.error()}")
            continue

        # Process the consumed event
        handle_event(msg)

finally:
    # Close the consumer gracefully
    consumer.close()
