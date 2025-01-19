import pandas as pd

# Load the CSV file
df = pd.read_csv('/Users/onurural/onur/AirStream/merged_pollution_weather_data.csv')

# Convert the dt column to the required format
df['dt'] = pd.to_datetime(df['dt']).dt.strftime('%Y-%m-%d %H:%M:%S')

print(df.head())

# Save the processed file
df.to_csv('/Users/onurural/onur/AirStream/merged_pollution_weather_data.csv', index=False)
