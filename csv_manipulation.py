import pandas as pd

# Load the CSV file into a DataFrame
df = pd.read_csv('modified_file.csv')

# Ensure the 'DateObserved' column is in datetime format
df['date_local'] = pd.to_datetime(df['date_local'])

# Define the date range
start_date = '2020-01-01'
end_date = '2023-12-31'

# Find the rows between the date range
mask = (df['date_local'] >= start_date) & (df['date_local'] <= end_date)

# Move the values from 'Ozone' column to 'OZONE' column for the specified rows
df.loc[mask, 'Category_Name'] = df.loc[mask, 'AQI_Category']

# Optional: Drop the 'Ozone' column if it's no longer needed
# df.drop('Ozone', axis=1, inplace=True)

# Save the modified DataFrame to a new CSV file
df.to_csv('datasets/modified_file.csv', index=False)

print("Values transferred from 'AQI_Category' to 'Category_Name' for the specified date range.")
