import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

store_ids = [101, 102, 103]
products = ["apple", "banana", "milk", "bread", "coffee"]


def generate_transaction():
    store_id = random.choice(store_ids)
    product = random.choice(products)
    quantity = random.randint(1, 5)
    price = round(random.uniform(1, 20), 2)
    total = round(quantity * price, 2)
    return {
        "store_id": store_id,
        "product": product,
        "quantity": quantity,
        "price": price,
        "total_amount": total,
        "timestamp": datetime.now().isoformat(),
    }


while True:
    txn = generate_transaction()
    print(f"Sending: {txn}")
    producer.send("pos_transactions", txn)
    time.sleep(random.uniform(0.5, 1.5))
