from kafka import KafkaProducer
import json
import requests
import time
import pandas as pd

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# OpenWeatherMap API URLs (replace with your lat/lon if needed)
pollution_api_url = 'http://api.openweathermap.org/data/2.5/air_pollution'
weather_api_url = 'https://api.openweathermap.org/data/2.5/weather'
params = {
    'lat': 37.7596,
    'lon': -122.4420,
    'appid': '005610094dcbe6eea3d2f3dce02b3471'
}
# 1728510323
# 1728510323
# 1728510323
# Fetch data from APIs
def get_real_time_data():
    # Get air pollution data
    pollution_data = requests.get(pollution_api_url, params=params).json()
    # Get weather data
    weather_data = requests.get(weather_api_url, params=params).json()
    
    # Extract relevant data from both APIs
    # weather_data['dt'] = pd.to_datetime(weather_data['dt'])
    timestamp = pd.to_datetime(weather_data['dt'], unit='s')

    data = {
        'co': pollution_data['list'][0]['components']['co'],
        'no': pollution_data['list'][0]['components']['no'],
        'no2': pollution_data['list'][0]['components']['no2'],
        'o3': pollution_data['list'][0]['components']['o3'],
        'so2': pollution_data['list'][0]['components']['so2'],
        'pm2_5': pollution_data['list'][0]['components']['pm2_5'],
        'pm10': pollution_data['list'][0]['components']['pm10'],
        'nh3': pollution_data['list'][0]['components']['nh3'],
        'aqi': pollution_data['list'][0]['main']['aqi'],
        'temperature': weather_data['main']['temp'],
        'dew_point': weather_data['main']['temp'],
        'feels_like': weather_data['main']['feels_like'],
        'temp_min': weather_data['main']['temp_min'],
        'temp_max': weather_data['main']['temp_max'],
        'pressure': weather_data['main']['pressure'],
        'humidity': weather_data['main']['humidity'],
        'wind_speed': weather_data['wind']['speed'],
        'wind_deg': weather_data['wind']['deg'],
        'clouds_all': weather_data['clouds']['all'],
        'datetime': weather_data['dt'],
        'hour' : timestamp.hour,
        'day_of_week' : timestamp.dayofweek,
        'month' : timestamp.month,
    }
    
    return data

# Send data to Kafka topic
while True:
    try:
        real_time_data = get_real_time_data()
        producer.send('raw-data-topic-1', real_time_data)
        producer.flush()
        print("Data sent successfully.")
    except Exception as e:
        print(f"Error occurred: {e}")
    
    # Add a 60-second delay between API calls
    time.sleep(60)
