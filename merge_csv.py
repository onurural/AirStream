import pandas as pd

# CO_15_data = pd.read_csv('epa_CO_data_15.csv')
# CO_16_data = pd.read_csv('epa_CO_data_16.csv')
# CO_17_data = pd.read_csv('epa_CO_data_17.csv')
# CO_18_data = pd.read_csv('epa_CO_data_18.csv')
# CO_19_data = pd.read_csv('epa_CO_data_19.csv')

# NO2_20_data = pd.read_csv('epa_NO2_data_15.csv')
# NO2_21_data = pd.read_csv('epa_NO2_data_16.csv')
# NO2_22_data = pd.read_csv('epa_NO2_data_17.csv')
# NO2_23_data = pd.read_csv('epa_NO2_data_18.csv')
# NO2_24_data = pd.read_csv('epa_NO2_data_19.csv')

CO_15_19_data = pd.read_csv('co_no2_data_15_23.csv')
CO_20_23_data = pd.read_csv('sanfrancisco_15-24-weather.csv')
# Concatenating the dataframes
merged_CO_data = pd.concat([CO_15_19_data, CO_20_23_data])

# Convert 'date_local' to datetime format
merged_CO_data['date_local'] = pd.to_datetime(merged_CO_data['date_local'])

# Sort the data by 'date_local'
merged_CO_data = merged_CO_data.sort_values(by='date_local')

# Reset index for cleaner output
merged_CO_data.reset_index(drop=True, inplace=True)

merged_CO_data.to_csv('merged_data_2015_2023.csv', index=False)

# Display the merged and sorted dataframe
print(merged_CO_data.head())
