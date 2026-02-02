import requests
import json
import os
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

# Get API Key from credentials
api_key = os.getenv('DHS_API_KEY')
app_token = os.getenv('DHS_APP_TOKEN')


# DHS shelter census historical data
historical_api_endpoint = "https://data.cityofnewyork.us/api/v3/views/dwrg-kzni/query.json"
daily_api_endpoint = "https://data.cityofnewyork.us/api/v3/views/k46n-sa2m/query.json"

headers = {
    'X-App-Token': app_token
}

params = {
    '$limit': 10,
    '$$app_token': app_token
}

# Retrieve data from API
historical_response = requests.get(historical_api_endpoint, headers=headers, params=params)
daily_response = requests.get(daily_api_endpoint, headers=headers, params=params)

# Get status code from response and check for successful data retrieval
print(f"Historical Data API Status Code: {historical_response.status_code}")
print(f"\nDaily Data API Response Code: {daily_response.status_code}")
if historical_response.status_code == 200 and daily_response.status_code == 200:

    historical_data = historical_response.json()
    daily_data = daily_response.json()

    # Print out records from historical dataset
    print('='*80)
    print('\n HISTORICAL DATASET')
    print('\n' + '='*80)
    print('\n First record:')
    print(json.dumps(historical_data[0], indent=2))
    print('\n Second record:')
    print(json.dumps(historical_data[1], indent=2))
    print('\n Last record: ')
    print(json.dumps(historical_data[-1], indent=2))

    # Print out a few records from daily dataset
    print('='*80)
    print('\n DAILY DATASET')
    print('\n' + '='*80)
    print('\n First record:')
    print(json.dumps(daily_data[0], indent=2))
    print('\n Second record:')
    print(json.dumps(daily_data[1], indent=2))
    print('\n Last record: ')
    print(json.dumps(daily_data[-1], indent=2))

else:
    print(f"Error retrieving data from API")
    print(f"Historical API response: {historical_response.text}")
    print(f"Daily API response: {daily_response.text}")

# Read datasets into pandas DataFrame and run comparisons
try:
    historical_df = pd.DataFrame(historical_data)
    daily_df = pd.DataFrame(daily_data)

    # Compare data structures
    if len(historical_df.columns) == len(daily_df.columns):
        print(f"Same number of fields in both datasets: {len(historical_df.columns)}")
    else:
        print(f"Different number of fields between datasets")
        print(f"\n Historical Data: {len(historical_df.columns)} columns\n Fields: {historical_df.columns}")
        print(f"\n Daily Data: {len(daily_df.columns)} columns\n Fields: {daily_df.columns}")
    
    # Compare field names across datasets. Sort and normalize case for comparison
    historical_df_col_list = list(sorted([col.lower() for col in historical_df.columns]))
    daily_df_col_list = list(sorted([col.lower() for col in daily_df.columns]))

    print(type(historical_df_col_list))
    print(historical_df_col_list)

    if historical_df_col_list == daily_df_col_list:
        print("Column names are identical across datasets")
    else:
        print("Differences detected between the column sets")
        print(f"\n Historical Data Fields: {historical_df.columns}")
        print(f"\n Daily Data Fields: {daily_df.columns}")

except Exception as e:
    print(f"An error occurred while comparing the Historical & Daily Datasets: {str(e)}")