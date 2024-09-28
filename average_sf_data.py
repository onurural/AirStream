import pandas as pd

# Load the CSV file
weather_data = pd.read_csv('sanfrancisco_15-24-weather.csv')

# Convert 'dt_iso' to datetime format with the correct format specified
weather_data['date'] = pd.to_datetime(weather_data['dt_iso'], format='%Y-%m-%d %H:%M:%S %z', utc=True).dt.date

# Drop the original 'dt_iso' column as it's no longer needed
weather_data = weather_data.drop(columns=['dt_iso'])

# Group the data by the 'date' column and calculate the average of the daily values
daily_weather_data = weather_data.groupby('date').mean().reset_index()

# Save the result to a new CSV file
daily_weather_data.to_csv('sf_daily_weather_data.csv', index=False)

print("Daily averaged weather data has been saved as 'sf_daily_weather_data.csv'")
