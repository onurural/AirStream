#!/bin/bash

# Kafka broker and topic details
BROKER="localhost:9092"  # You can replace this with your broker's address if it's different
TOPIC_NAME=$1  # Topic name passed as the first argument to the script
PARTITIONS=${2:-1}  # Number of partitions, default is 1
REPLICATION_FACTOR=${3:-1}  # Replication factor, default is 1

# Check if topic name is passed as an argument
if [ -z "$TOPIC_NAME" ]; then
  echo "Error: Topic name not provided."
  echo "Usage: ./create_topic.sh <topic_name> [partitions] [replication_factor]"
  exit 1
fi

# Create a Kafka topic using docker exec to run the command in the Kafka container
docker exec -it kafka kafka-topics --create \
  --topic "$TOPIC_NAME" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR" \
  --bootstrap-server "$BROKER"

# Output success message
if [ $? -eq 0 ]; then
  echo "Topic '$TOPIC_NAME' created successfully with $PARTITIONS partition(s) and replication factor of $REPLICATION_FACTOR."
else
  echo "Failed to create topic '$TOPIC_NAME'."
fi
