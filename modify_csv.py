import pandas as pd

# Load the dataset
file_path = 'merged_aqi_2015_2019.csv'
data = pd.read_csv(file_path)

# Drop unnecessary columns
columns_to_drop = ['HourObserved', 'LocalTimeZone', 'ReportingArea', 'StateCode', 'Latitude', 'Longitude']
data = data.drop(columns=columns_to_drop)

# Pivot to create separate columns for Ozone and PM2.5
data_pivot = data.pivot_table(index='DateObserved', columns='ParameterName', values='AQI').reset_index()

# Rename columns for clarity
data_pivot = data_pivot.rename(columns={'OZONE': 'Ozone', 'PM2.5': 'PM2.5'})

# Extract 'Number' and 'AQI_Category' from the 'Category' column
data['Category'] = data['Category'].apply(eval)  # Convert string to dictionary
data_pivot['Number'] = data['Category'].apply(lambda x: x['Number'])  # Extract AQI Number
data_pivot['AQI_Category'] = data['Category'].apply(lambda x: x['Name'])  # Extract AQI Category name

# Save the result to a new CSV
output_file = 'processed_aqi_data_with_category.csv'
data_pivot.to_csv(output_file, index=False)

print(f"Processed data saved to {output_file}")
