from kafka import KafkaProducer
import json
import requests
import time
import pandas as pd
import time
from datetime import datetime, timezone
import json

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_last_72_hours_historical_weather_data(api_key, lat, lon):
    # Get the current timestamp
    current_timestamp = int(time.time())
    # Calculate timestamps for each of the last 72 hours
    timestamps = [current_timestamp - (i * 3600) for i in range(72)]
    
    # Base URL for the API
    base_url = "https://api.openweathermap.org/data/3.0/onecall/timemachine"
    
    # Store results in a list
    weather_data = []

    # Loop over each timestamp and fetch data
    for dt in timestamps:
        # Construct the request URL with the current timestamp
        url = f"{base_url}?appid={api_key}&lat={lat}&lon={lon}&dt={dt}"
        
        try:
            # Make the API request
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad status codes
            
            # Append the response JSON to the weather_data list
            weather_data.append(response.json())
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for timestamp {dt}: {e}")
    
    return weather_data



def generate_last_72_hours_air_pollution_url(api_key, lat, lon):
    pollution_data = []
    # Get the current timestamp
    end_timestamp = int(time.time())
    # Calculate the timestamp for 72 hours ago
    start_timestamp = end_timestamp - (72 * 60 * 60)
    
    # Construct the URL with the provided parameters and calculated timestamps
    url = f"http://api.openweathermap.org/data/2.5/air_pollution/history?appid={api_key}&lat={lat}&lon={lon}&start={start_timestamp}&end={end_timestamp}"

    response = requests.get(url)
    json_data  = response.json()
    response.raise_for_status()  # Raise an error for bad status codes
            
            # Append the response JSON to the weather_data list
    pollution_data.append(response.json())
    return json_data

# Usage
api_key = ""  
lat = 37.7596
lon = -122.4420

# OpenWeatherMap API URLs (replace with your lat/lon if needed)
# pollution_api_url = 'http://api.openweathermap.org/data/2.5/air_pollution'
weather_api_url = 'https://api.openweathermap.org/data/2.5/weather'
params = {
    'lat': 37.7596,
    'lon': -122.4420,
    'appid': ''
}

# Fetch data from APIs
def get_real_time_data():
    # Get air pollution data
    pollution_data = generate_last_72_hours_air_pollution_url(api_key=api_key, lat=lat, lon=lon)

    # Get weather data
    weather_data = fetch_last_72_hours_historical_weather_data(api_key=api_key, lat=lat, lon=lon)
    pollution_records = []

    print(type(pollution_data))

    for entry in pollution_data['list']:  # Only the last 72 hours
            pollution_record = {
                'dt':entry["dt"],
                'aqi': entry["main"]["aqi"],
                'co': entry["components"]["co"],
                'no': entry["components"]["no"],
                'no2': entry["components"]["no2"],
                'o3': entry["components"]["o3"],
                'so2': entry["components"]["so2"],
                'pm2_5': entry["components"]["pm2_5"],
                'pm10': entry["components"]["pm10"],
                'nh3': entry["components"]["nh3"]
            }
            pollution_records.append(pollution_record)

    pollution_df = pd.DataFrame(pollution_records)

    weather_records = []
    for entry in weather_data[-72:]:
            for hour_data in entry['data']:
                weather_record = {
                    'dt': hour_data['dt'],
                    'temp': hour_data["temp"],
                    'feels_like': hour_data["feels_like"],
                    'pressure': hour_data["pressure"],
                    'humidity': hour_data["humidity"],
                    'dew_point': hour_data["dew_point"],
                    'clouds_all': hour_data["clouds"],
                    'wind_speed': hour_data["wind_speed"],
                    'wind_deg': hour_data["wind_deg"]
                }
            weather_records.append(weather_record)

    weather_df = pd.DataFrame(weather_records)

    # print('pollution Data, ', pollution_data)
    
    # Extract relevant data from both APIs
    merged_data = pd.merge_asof(pollution_df.sort_values('dt'), 
                            weather_df.sort_values('dt'), 
                            on='dt', direction='nearest')

# Drop rows with any missing values 
    merged_data.dropna(inplace=True)    
    
    return merged_data

# Send data to Kafka topic
while True:
    try:
        real_time_df = get_real_time_data()
        real_time_json = real_time_df.to_json(orient='records', lines=True)
        print('Real time json: ', real_time_json)
        producer.send('raw-data-topic-2', real_time_json)
        producer.flush()
        print("Data sent successfully.")
    except Exception as e:
        print(f"Error occurred: {e.with_traceback()}")
    
    # Add a 60-second delay between API calls
    time.sleep(60)
