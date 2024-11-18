from confluent_kafka import Consumer
import sys
import json

# Define Kafka Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'balance-consumer-group',
    'auto.offset.reset': 'earliest'  # Start reading from the earliest available message
}

consumer = Consumer(consumer_conf)

# In-memory account balances (for demo purposes)
balances = {}

# Consume transaction events and update account balance
def process_transaction(event):
    account_id = event['account_id']
    amount = event['amount']

    # Update balance in-memory (you can store it in a DB in a real-world app)
    balances[account_id] = balances.get(account_id, 0) + amount

    # Print updated balance to terminal
    print(f"Account ID: {account_id} - New Balance: {balances[account_id]}")

# Subscribe to the 'transactions' topic
consumer.subscribe(['transactions'])

print("Consumer started, waiting for transactions...")

# Continuously consume messages and print balances
try:
    while True:
        msg = consumer.poll(1.0)  # 1 second timeout for new messages

        if msg is None:  # No new message
            continue
        if msg.error():  # Handle Kafka errors
            print(f"Consumer error: {msg.error()}")
            continue

        # Deserialize transaction event
        event = json.loads(msg.value().decode('utf-8'))

        # Process the transaction and update balance
        process_transaction(event)

finally:
    # Close the consumer gracefully
    consumer.close()
