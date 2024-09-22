import requests
import json
import time
from kafka import KafkaProducer

# AirNow API configuration
AIRNOW_API_KEY = '3C5EFCAD-B668-4D7D-A175-C026F6C0B372'  # Replace with your actual AirNow API key
AIRNOW_API_URL = 'http://www.airnowapi.org/aq/observation/zipCode/current/'

# Kafka configuration
KAFKA_TOPIC = 'test-topic'  # Replace with your Kafka topic name
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address

# Initialize Kafka producer
print(f"Attempting to connect to Kafka broker at {KAFKA_BROKER}")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_air_quality_data(zip_code, distance=25):
    """
    Fetch air quality data from AirNow API for a specific zip code.
    """
    params = {
        'format': 'application/json',
        'zipCode': zip_code,
        'distance': distance,
        'API_KEY': AIRNOW_API_KEY
    }
    
    try:
        response = requests.get(AIRNOW_API_URL, params=params)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from AirNow API: {e}")
        return None

def send_data_to_kafka(data):
    """
    Send the air quality data to Kafka.
    """
    if data:
        for record in data:
            producer.send(KAFKA_TOPIC, value=record)
            print(f"Sent record to Kafka: {record}")
        producer.flush()  # Ensure all messages are sent
    else:
        print("No data to send to Kafka.")

if __name__ == "__main__":
    # Fetch and send data continuously every minute
    ZIP_CODE = '20002'  # Replace with your desired ZIP code
    while True:
        print("Fetching air quality data...")
        air_quality_data = fetch_air_quality_data(ZIP_CODE)
        
        if air_quality_data:
            print(f"Fetched {len(air_quality_data)} records")
        else:
            print("No data fetched.")
        
        print("Sending data to Kafka...")
        send_data_to_kafka(air_quality_data)
        
        # Wait for a minute before fetching the next batch of data
        # time.sleep(60)
