import requests
import pandas as pd

# Define the API endpoint
url = "https://aqs.epa.gov/data/api/dailyData/byCounty"
params = {
    "email": "onurural57@gmail.com",  # Your email for authentication
    "key": "",                # Your API key
    "param": "42602",                  # Parameter (for NO2 in this case)
    "bdate": "20190101",               # Begin date (YYYYMMDD)
    "edate": "20191231",               # End date (YYYYMMDD)
    "county": "075",                   # County code (San Francisco)
    "state": "06"                      # State code (California)
}


response = requests.get(url, params=params)


if response.status_code == 200:

    data = response.json()
    

    if 'Data' in data:
        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(data['Data'])
        
        # Save the DataFrame to a CSV file
        df.to_csv('epa_NO2_data_19.csv', index=False)
        print("Data saved to 'epa_NO2_data_19.csv'")
    else:
        print("No data found in the response.")
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")
