from urllib.parse import quote
import pandas as pd
import requests
import re

# Path to the local CSV file
local_csv_path = 'eviction_notices.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(local_csv_path)

# Convert all column names to lowercase and replace spaces with underscores
df.columns = df.columns.str.lower().str.replace(' ', '_')

# Initialize counters
total_addresses = 0
failed_geocoding = 0
successful_geocoding = 0

def clean_address(address):
    """Clean common address formatting issues"""
    if pd.isna(address):
        return address
    
    # Convert to string and strip whitespace
    address = str(address).strip()
    
    # Fix common typos
    address = address.replace('STEREET', 'STREET')
    address = address.replace('PLEASNT', 'PLEASANT')
    address = address.replace('BRENTOOWD', 'BRENTWOOD')
    address = address.replace('IRVINING', 'IRVINGTON')
    address = address.replace('21STSTREET', '21ST STREET')
    address = address.replace('CONNETICUT', 'CONNECTICUT')
    address = address.replace('MCARUTHUR', 'MACARTHUR')
    
    # Remove problematic patterns that contain dates and zip codes mixed in
    # Pattern: removes things like "NW 20010 1/29/2025 nan"
    address = re.sub(r'\s+(NE|NW|SE|SW)\s+\d{5}\s+\d{1,2}/\d{1,2}/\d{4}.*', '', address)
    address = re.sub(r'\s+(NE|NW|SE|SW)\s+\d{5}\s+\d{1,2}/.*', '', address)
    
    # Remove trailing date fragments (but keep multi-unit info intact)
    address = re.sub(r'\s+\d{1,2}/$', '', address)
    address = re.sub(r'\s+\d{1,2}/\d{1,2}/$', '', address)
    address = re.sub(r'\s+\d{1,2}/\d{1,2}/\d{2,4}.*$', '', address)
    
    # Clean up multiple spaces and commas
    address = re.sub(r'\s+', ' ', address)
    address = re.sub(r',\s*,', ',', address)
    address = address.strip()
        
    return address

# Function to get latitude, longitude, and ward for an address
def get_address_info(address):
    global total_addresses, failed_geocoding, successful_geocoding
    total_addresses += 1
    
    # Clean the address first
    cleaned_address = clean_address(address)
    
    encoded_address = quote(cleaned_address)
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
            successful_geocoding += 1
            return lat, lon, ward

    failed_geocoding += 1
    print(f"Failed to geocode (keeping in dataset): {cleaned_address}")
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

# Print final summary report
print("\n" + "="*50)
print("PROCESSING SUMMARY REPORT")
print("="*50)
print(f"Total addresses processed: {total_addresses:,}")
print(f"Successfully geocoded: {successful_geocoding:,}")
print(f"Failed to geocode (kept in dataset): {failed_geocoding:,}")
print(f"Success rate: {(successful_geocoding/total_addresses)*100:.1f}%")
print(f"\nAll {total_addresses:,} addresses were included in the final dataset.")
print("Addresses that failed geocoding have empty lat/lng/ward values.")
print("="*50)