import pandas as pd

# Load the dataset from the CSV file
df = pd.read_csv('merged_co_no2_data_2015_2023.csv')

# Drop the 'sample_duration_code' and 'sample_duration' columns
df = df.drop(columns=['sample_duration_code', 'sample_duration'])

# Set 'date_local' as the index
df['date_local'] = pd.to_datetime(df['date_local'])  # Ensure it's in datetime format
df.set_index('date_local', inplace=True)

# Pivot the table to get 'CO' and 'NO2' columns for counts and percentages
df_pivoted = df.pivot_table(
    index='date_local',
    columns='parameter',  # Pivot based on 'parameter' (CO or NO2)
    values=['observation_count', 'observation_percent'],  # Use counts and percentages
    aggfunc='first'  # Assume there's one value per date, or adjust aggregation if needed
)

# Rename the columns for clarity
df_pivoted.columns = ['CO', 'CO_percent', 'NO2', 'NO2_percent']

# Now swap the 'CO_percent' values with the 'NO2' column values
df_pivoted['CO_percent'], df_pivoted['NO2'] = df_pivoted['NO2'], df_pivoted['CO_percent']

# Reset the index (optional, if you want 'date_local' as a column rather than index)
df_pivoted.reset_index(inplace=True)

# Save the resulting DataFrame to a new CSV file
df_pivoted.to_csv('modified_data.csv', index=False)

print("Data has been modified and saved as 'modified_data.csv'")
