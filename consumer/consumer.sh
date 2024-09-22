#!/bin/bash

# Kafka broker and topic details
BROKER="localhost:9092"  # Kafka broker address, adjust if different
TOPIC_NAME=$1  # Topic name passed as the first argument to the script

# Check if topic name is passed as an argument
if [ -z "$TOPIC_NAME" ]; then
  echo "Error: Topic name not provided."
  echo "Usage: ./consumer.sh <topic_name>"
  exit 1
fi

# Start the Kafka console consumer inside the Kafka container
docker exec -it kafka kafka-console-consumer --bootstrap-server "$BROKER" --topic "$TOPIC_NAME" --from-beginning
