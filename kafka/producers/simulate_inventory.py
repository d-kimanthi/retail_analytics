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


def generate_inventory_update():
    return {
        "store_id": random.choice(store_ids),
        "product": random.choice(products),
        "new_stock_level": random.randint(50, 500),
        "timestamp": datetime.now().isoformat(),
    }


while True:
    update = generate_inventory_update()
    print(f"Sending: {update}")
    producer.send("inventory_updates", update)
    time.sleep(random.uniform(2, 4))
