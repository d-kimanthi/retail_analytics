#!/bin/bash
# This script consumes messages from Kafka topics for a retail application.
docker exec -it kafka-kafka-1 \
  kafka-console-consumer --bootstrap-server localhost:9092 --topic pos_transactions --from-beginning

# docker exec -it kafka-kafka-1 \
#   kafka-console-consumer --bootstrap-server localhost:9092 --topic inventory_updates --from-beginning
