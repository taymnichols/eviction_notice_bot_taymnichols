from urllib.parse import quote
import pandas as pd
import requests

# Path to the local CSV file
local_csv_path = 'eviction_notices.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(local_csv_path)

# Convert all column names to lowercase and replace spaces with underscores
df.columns = df.columns.str.lower().str.replace(' ', '_')

# Function to get latitude, longitude, and ward for an address
def get_address_info(address):
    encoded_address = quote(address)
    url = f"https://citizenatlas.dc.gov/newwebservices/locationverifier.asmx/findLocation2?str={encoded_address}&f=json"
  
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data.get('returnDataset') and data['returnDataset'].get('Table1'):
            lat = data['returnDataset']['Table1'][0]['LATITUDE']
            lon = data['returnDataset']['Table1'][0]['LONGITUDE']
            ward_2002 = data['returnDataset']['Table1'][0].get('WARD_2002')
            ward_2012 = data['returnDataset']['Table1'][0].get('WARD_2012')
            ward = ward_2002 if ward_2002 else ward_2012 
            return lat, lon, ward

    print("Error: Failed to parse address:", address)
    return None, None, None

# Apply the get_address_info function to each address in the "full address" column
df[['latitude', 'longitude', 'ward']] = df['full_address'].apply(lambda x: pd.Series(get_address_info(x)))

# Rename columns from latitude and longitude to lat and lng
df = df.rename(columns={'latitude': 'lat', 'longitude': 'lng'})

# Convert 'eviction_date' to datetime format and add month and year columns
df['eviction_date'] = pd.to_datetime(df['eviction_date'])
df['month'] = df['eviction_date'].dt.month
df['year'] = df['eviction_date'].dt.year
df['month_name'] = df['eviction_date'].dt.strftime('%B')

# Save the updated DataFrame to a new CSV file
df.to_csv('eviction_data_ward.csv', index=False)