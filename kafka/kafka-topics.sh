#!/bin/bash
# This script creates Kafka topics for a retail application.
docker exec -it kafka-kafka-1 \
  kafka-topics --create --topic pos_transactions --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

docker exec -it kafka-kafka-1 \
  kafka-topics --create --topic inventory_updates --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
