import pandas as pd

# Load the two CSV files into separate DataFrames
df1 = pd.read_csv('datasets/merged_data.csv')
df2 = pd.read_csv('datasets/processed_aqi_data.csv')

# Ensure that the 'date_local' column is in datetime format (optional)
df1['date_local'] = pd.to_datetime(df1['date_local'])
df2['date_local'] = pd.to_datetime(df2['date_local'])

# Merge the two DataFrames on the 'date_local' column
merged_df = pd.merge(df1, df2, on='date_local', how='inner')  # You can use 'outer', 'left', or 'right' as needed

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('merged_file.csv', index=False)

print("Merging complete and saved to 'merged_file.csv'.")