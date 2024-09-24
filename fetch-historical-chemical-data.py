import requests
import pandas as pd

# Define the API endpoint
url = "https://aqs.epa.gov/data/api/dailyData/byCounty"
params = {
    "email": "onurural57@gmail.com",  # Your email for authentication
    "key": "baywren87",                # Your API key
    "param": "42101",                  # Parameter (for NO2 in this case)
    "bdate": "20200101",               # Begin date (YYYYMMDD)
    "edate": "20201231",               # End date (YYYYMMDD)
    "county": "075",                   # County code (San Francisco)
    "state": "06"                      # State code (California)
}

# Send request to the API
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()
    
    # Check if 'Data' key exists in the response
    if 'Data' in data:
        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(data['Data'])
        
        # Save the DataFrame to a CSV file
        df.to_csv('epa_CO_data_20.csv', index=False)
        print("Data saved to 'epa_air_quality_data.csv'")
    else:
        print("No data found in the response.")
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")
