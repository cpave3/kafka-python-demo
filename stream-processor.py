from confluent_kafka import Consumer, Producer
import sys
import json

# Kafka Consumer and Producer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'balance-stream-processor-group',
    'auto.offset.reset': 'earliest'
}

producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'balance-stream-processor'
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

# In-memory account balances (for demo purposes)
balances = {}

# Delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Process incoming transaction and update balance
def process_transaction(event):
    account_id = event['account_id']
    amount = event['amount']

    # Update balance in-memory (you can store it in a DB in a real-world app)
    balances[account_id] = balances.get(account_id, 0) + amount

    # Produce updated balance to 'balance-updates' topic
    balance_update_event = {
        'account_id': account_id,
        'new_balance': balances[account_id]
    }
    producer.produce('balance-updates', key=account_id, value=json.dumps(balance_update_event), callback=delivery_report)
    producer.flush()

# Subscribe to the 'transactions' topic
consumer.subscribe(['transactions'])

print("Stream Processor started, waiting for transactions...")

# Continuously consume and process transactions
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Deserialize transaction event
        event = json.loads(msg.value().decode('utf-8'))

        # Process the transaction and update the balance
        process_transaction(event)

finally:
    consumer.close()
