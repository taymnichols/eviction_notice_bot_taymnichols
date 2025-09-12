from urllib.parse import quote 
import pandas as pd 
import requests 
import re 
from tqdm import tqdm 

# --- CONFIGURATION --- 
LOCAL_CSV_PATH = 'eviction_notices.csv' 
OUTPUT_CSV_PATH = 'eviction_data_ward.csv'
DC_GEOCODING_API_URL = "https://citizenatlas.dc.gov/newwebservices/locationverifier.asmx/findLocation2" 

# --- DATA LOADING AND PREPARATION --- 
df = pd.read_csv(LOCAL_CSV_PATH) 
df.columns = df.columns.str.lower().str.replace(' ', '_') 

# --- GLOBAL COUNTERS AND STORAGE --- 
stats = { 
    "total": 0, 
    "successful": 0, 
    "failed": 0, 
    "skipped": 0 
} 
failed_addresses = [] 
skipped_addresses = [] 

# --- ADDRESS PARSING AND CLEANING FUNCTIONS --- 
def parse_address_components(address): 
    if pd.isna(address): 
        return address, None, None 
    original_address = str(address).strip() 
    addr = original_address.upper()
    typo_fixes = { 
        'STEREET': 'STREET', 'PLEASNT': 'PLEASANT', 'AVE.': 'AVENUE', 
        'CONNETICUT': 'CONNECTICUT', 'MCARUTHUR': 'MACARTHUR' 
    } 
    for typo, fix in typo_fixes.items(): 
        addr = addr.replace(typo, fix) 
    street_suffixes = { 
        'STREET': 'ST', 'AVENUE': 'AVE', 'BOULEVARD': 'BLVD', 'CIRCLE': 'CIR', 
        'COURT': 'CT', 'DRIVE': 'DR', 'LANE': 'LN', 'ROAD': 'RD', 
        'PLACE': 'PL', 'TERRACE': 'TER', 'SQUARE': 'SQ' 
    } 
    for full, abbrev in street_suffixes.items(): 
        addr = re.sub(rf'\b{full}\b', abbrev, addr) 
    
    addr = re.sub(r'\s+\d+/\s*$', '', addr).strip()
    addr = re.sub(r'\s+(NE|NW|SE|SW)\s+\d{5}.*$', '', addr) 
    addr = re.sub(r'\s+\d{1,2}/\d{1,2}/\d{2,4}.*$', '', addr) 
    
    unit_patterns = [ 
        r'\s+(UNIT|APT\.?|#|STE\.?|SUITE)\s*([A-Z0-9\-]+)', 
        r'\s+([A-Z]\d+)(?=\s|,|$)',
    ] 
    units = [] 
    for pattern in unit_patterns: 
        matches = re.findall(pattern, addr) 
        for match in matches: 
            unit_str = "".join(match).strip() 
            if unit_str not in units: 
                units.append(unit_str) 
        addr = re.sub(pattern, '', addr) 
    unit_info = f"#{units[0]}" if units else None 
    base_addr = re.sub(r',\s*$', '', addr).strip()
    base_addr = re.sub(r'\s+', ' ', base_addr)
    base_addr = re.sub(r'^(\d+)-\d+\s', r'\1 ', base_addr) 
    quad_match = re.search(r'\b(NE|NW|SE|SW)\b', base_addr) 
    if quad_match: 
        quad = quad_match.group(1) 
        base_addr = re.sub(r'\s*\b(NE|NW|SE|SW)\b\s*', ' ', base_addr).strip() 
        base_addr = f"{base_addr} {quad}" 
    return original_address, base_addr, unit_info 

def should_attempt_geocoding(address): 
    if not address or pd.isna(address): 
        return False 
    if re.search(r'VACANT\s+LOT', address, re.IGNORECASE): 
        return False 
    if not re.match(r'^\d+', address):
        return False 
    return True 

# --- GEOGRAPHIC FUNCTIONS --- 
def geocode_address(address): 
    encoded_address = quote(address) 
    url = f"{DC_GEOCODING_API_URL}?str={encoded_address}&f=json" 
    try: 
        response = requests.get(url, timeout=10) 
        response.raise_for_status()
        data = response.json() 
        if data and data.get('returnDataset') and data['returnDataset'].get('Table1'): 
            result = data['returnDataset']['Table1'][0] 
            lat, lon = result.get('LATITUDE'), result.get('LONGITUDE') 
            if not (lat and lon and 38.8 < lat < 39.0 and -77.2 < lon < -76.9): 
                return None 
            return { 
                "lat": lat, 
                "lng": lon, 
                "ward": result.get('WARD_2012') or result.get('WARD_2002'), 
                "zipcode_api": result.get('ZIPCODE'), 
                "quad_api": result.get('QUADRANT') 
            } 
    except (requests.exceptions.RequestException, ValueError, requests.exceptions.JSONDecodeError): 
        return None 
    return None 

# --- MAIN PROCESSING LOGIC --- 
def process_row(full_address): 
    stats['total'] += 1 
    original, base_addr, unit = parse_address_components(full_address) 
    # **FIX**: Removed 'address_original' from here to prevent duplication
    result = { 
        'address_base': base_addr, 
        'unit': unit, 
        'lat': None, 'lng': None, 'ward': None, 'zipcode_api': None, 'quad_api': None 
    } 
    if should_attempt_geocoding(base_addr): 
        geo_data = geocode_address(base_addr) 
        if geo_data: 
            stats['successful'] += 1 
            result.update(geo_data) 
        else: 
            stats['failed'] += 1 
            failed_addresses.append({'original': original, 'base': base_addr}) 
    else: 
        stats['skipped'] += 1 
        skipped_addresses.append({'original': original, 'base': base_addr}) 
    return pd.Series(result) 


# --- SCRIPT EXECUTION --- 

print("Processing and geocoding addresses...") 
tqdm.pandas(desc="Geocoding") 
geocoded_data = df['full_address'].progress_apply(process_row) 

df = df.join(geocoded_data) 

df['zipcode'] = df['zipcode'].fillna(df['zipcode_api']) 
df['quad'] = df['quad'].fillna(df['quad_api']) 

df['zipcode'] = pd.to_numeric(df['zipcode'], errors='coerce').dropna().astype(int).astype(str)

def create_final_cleaned_address(row): 
    if pd.isna(row['address_base']): 
        return None 
    parts = [row['address_base']] 
    if pd.notna(row['unit']): 
        parts.append(row['unit']) 
    return " ".join(parts)

df['address_cleaned'] = df.apply(create_final_cleaned_address, axis=1) 

# Date processing 
df['eviction_date'] = pd.to_datetime(df['eviction_date'], errors='coerce') 
df['month'] = df['eviction_date'].dt.month 
df['year'] = df['eviction_date'].dt.year 
df['month_name'] = df['eviction_date'].dt.strftime('%B')

# **FIX**: Rename 'full_address' to 'address_original' just once at the end
if 'full_address' in df.columns:
    df = df.rename(columns={'full_address': 'address_original'})

cols_to_drop = ['defendant_address', 'zipcode_api', 'quad_api'] 
df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore') 

# Save the result 
df.to_csv(OUTPUT_CSV_PATH, index=False) 
print(f"\n✅ Processing complete. Data saved to {OUTPUT_CSV_PATH}") 

# --- REPORTING --- 
print("\n" + "="*60) 
print("PROCESSING SUMMARY REPORT") 
print("="*60) 
print(f"Total addresses processed: {stats['total']:,}") 
print(f"Geocoding attempted:     {stats['total'] - stats['skipped']:,}") 
print(f"  - Successfully geocoded: {stats['successful']:,}") 
print(f"  - Failed to geocode:     {stats['failed']:,}") 
print(f"Geocoding skipped:         {stats['skipped']:,} (poor address quality)") 

if (stats['total'] - stats['skipped']) > 0: 
    success_rate = (stats['successful'] / (stats['total'] - stats['skipped'])) * 100 
    print(f"\nSuccess Rate (of attempted): {success_rate:.1f}%") 

if failed_addresses: 
    print(f"\n--- Top 10 Failed Addresses ---") 
    for i, failed in enumerate(failed_addresses[:10]): 
        print(f"{i+1:2d}. Base: {failed.get('base', 'N/A')} (Original: {failed.get('original', 'N/A')})") 
print("="*60)