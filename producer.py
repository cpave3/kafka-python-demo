from confluent_kafka import Producer
import sys
import json

# Define the Kafka Producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker
    'client.id': 'python-producer'
}

# Create Producer instance
producer = Producer(conf)

# Delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Send transaction event to Kafka
def send_transaction(account_id, amount):
    transaction_event = {
        'account_id': account_id,
        'amount': amount
    }
    producer.produce('transactions', key=account_id, value=json.dumps(transaction_event), callback=delivery_report)
    producer.flush()

if __name__ == "__main__":
    # Ensure correct usage
    if len(sys.argv) != 3:
        print("Usage: python transaction-producer.py <account_id> <amount>")
        sys.exit(1)

    account_id = sys.argv[1]
    amount = float(sys.argv[2])

    # Send the transaction event
    send_transaction(account_id, amount)
