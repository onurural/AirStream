import requests
import pandas as pd
from datetime import datetime, timedelta

# AirNow API Configuration
API_KEY = '3C5EFCAD-B668-4D7D-A175-C026F6C0B372'  # Replace with your actual AirNow API Key
BASE_URL = 'https://www.airnowapi.org/aq/observation/latLong/historical'
LATITUDE = 37.7596  # Replace with the desired latitude
LONGITUDE = -122.4420  # Replace with the desired longitude
DISTANCE = 25  # Radius in miles
DATE_FORMAT = '%Y-%m-%dT00-0000'  # AirNow API date format

def fetch_historical_data(date, latitude, longitude):
    """Fetch historical AQI data for a specific date and location by lat/long."""
    params = {
        'format': 'application/json',
        'latitude': latitude,
        'longitude': longitude,
        'date': date,
        'distance': DISTANCE,
        'API_KEY': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for {date}: {response.status_code}")
        return None

def get_date_range(start_date, end_date):
    """Generate a list of dates between start_date and end_date."""
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date.strftime(DATE_FORMAT))
        current_date += timedelta(days=1)
    return date_range

def collect_historical_aqi(start_date, end_date, latitude, longitude):
    """Collect AQI data between a range of dates."""
    historical_data = []
    dates = get_date_range(start_date, end_date)
    
    for date in dates:
        print(f"Fetching data for {date}...")
        data = fetch_historical_data(date, latitude, longitude)
        if data:
            historical_data.extend(data)
    
    return historical_data

# Define date range for historical data collection
start_date = datetime(2019, 1, 1)  # Start date
end_date = datetime(2019, 12, 31)  # End date

# Collect historical data by latitude and longitude
historical_data = collect_historical_aqi(start_date, end_date, LATITUDE, LONGITUDE)

# Convert to DataFrame
df = pd.DataFrame(historical_data)

# Display the collected data
print(df.head())

# Save to CSV for future use
df.to_csv('historical_aqi_data_19.csv', index=False)
