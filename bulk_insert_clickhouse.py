import requests

# ClickHouse server URL (adjust to match your setup)
clickhouse_url = 'http://localhost:8123/'

# Path to the CSV file you want to insert
csv_file_path = '/Users/onurural/Downloads/merged_pollution_weather_data.csv'

# Query to insert data into the 'weather_data.weather' table
query = "INSERT INTO weather_data.weather FORMAT CSV"

# Read the CSV file and send the data to ClickHouse using an HTTP POST request
with open(csv_file_path, 'rb') as f:
    next(f)
    response = requests.post(clickhouse_url, params={'query': query}, data=f)

# Check response status
if response.status_code == 200:
    print("Data inserted successfully.")
else:
    print(f"Failed to insert data. Error: {response.text}")
