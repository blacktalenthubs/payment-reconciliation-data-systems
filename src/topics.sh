#!/usr/bin/env bash

# file: create_topics.sh

# List of topics to create
TOPICS=(
  "users_topic"
  "merchants_topic"
  "policies_topic"
  "transactions_topic"
  "fraud_signals_topic"
)

# Kafka container name
KAFKA_CONTAINER="local-kafka"

# Common Kafka options
BOOTSTRAP="localhost:9092"
REPLICATION_FACTOR=1
PARTITIONS=1

# Create each topic
for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic: $TOPIC ..."
  docker exec -it "$KAFKA_CONTAINER" kafka-topics.sh --create \
    --bootstrap-server "$BOOTSTRAP" \
    --replication-factor "$REPLICATION_FACTOR" \
    --partitions "$PARTITIONS" \
    --topic "$TOPIC"
  echo "Topic '$TOPIC' created."
  echo
done

echo "All topics created!"