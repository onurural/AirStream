import pandas as pd

CO_20_data = pd.read_csv('epa_CO_data_20.csv')
CO_21_data = pd.read_csv('epa_CO_data_21.csv')
CO_22_data = pd.read_csv('epa_CO_data_22.csv')
CO_23_data = pd.read_csv('epa_CO_data_23.csv')

NO2_20_data = pd.read_csv('epa_NO2_data_20.csv')
NO2_21_data = pd.read_csv('epa_NO2_data_21.csv')
NO2_22_data = pd.read_csv('epa_NO2_data_22.csv')
NO2_23_data = pd.read_csv('epa_NO2_data_23.csv')

# Concatenating the dataframes
merged_CO_data = pd.concat([NO2_20_data, NO2_21_data, NO2_22_data, NO2_23_data])

# Convert 'date_local' to datetime format
merged_CO_data['date_local'] = pd.to_datetime(merged_CO_data['date_local'])

# Sort the data by 'date_local'
merged_CO_data = merged_CO_data.sort_values(by='date_local')

# Reset index for cleaner output
merged_CO_data.reset_index(drop=True, inplace=True)

merged_CO_data.to_csv('merged_NO2_data_2020_2023.csv', index=False)

# Display the merged and sorted dataframe
print(merged_CO_data.head())
