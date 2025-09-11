from urllib.parse import quote
import pandas as pd
import requests
import re
from tqdm import tqdm


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
failed_addresses = []  # Store failed addresses for reporting
skipped_addresses = []  # Store addresses we skip for quality reasons


def parse_address_components(address):
    """Parse address into original, cleaned full, base address, and unit components"""
    if pd.isna(address):
        return address, None, None, None, "missing_address"
   
    original_address = str(address).strip()
    address = original_address
    quality_flag = "clean"  # Track data quality issues
   
    # Check for obvious data quality issues (but don't filter out)
    if re.search(r'\bnan\b|VACANT\s+LOT', address, re.IGNORECASE):
        quality_flag = "poor_quality"
    
    # Fix common typos
    typo_fixes = {
        'STEREET': 'STREET',
        'PLEASNT': 'PLEASANT',
        'BRENTOOWD': 'BRENTWOOD',
        'IRVINING': 'IRVINGTON',
        '21STSTREET': '21ST STREET',
        'CONNETICUT': 'CONNECTICUT',
        'MCARUTHUR': 'MACARTHUR',
        'HAWTHRO': 'HAWTHORNE',
        'AVE.': 'AVENUE',  # Handle "MARTIN LUTHER KING JR AVE."
    }
   
    for typo, fix in typo_fixes.items():
        address = address.replace(typo, fix)
   
    # Enhanced street suffix standardization
    street_suffixes = {
        'STREET': 'ST', 'AVENUE': 'AVE', 'BOULEVARD': 'BLVD', 
        'CIRCLE': 'CIR', 'COURT': 'CT', 'DRIVE': 'DR',
        'LANE': 'LN', 'ROAD': 'RD', 'PLACE': 'PL', 
        'TERRACE': 'TER', 'HIGHWAY': 'HWY', 'PARKWAY': 'PKWY',
        'WAY': 'WY'
    }
    
    for full_suffix, abbrev in street_suffixes.items():
        address = re.sub(rf'\b{full_suffix}\b', abbrev, address, flags=re.IGNORECASE)
   
    # Remove corrupted patterns specific to your data
    # Pattern like "SE 20019 8/" or "SE 20020 8/"
    address = re.sub(r'\s+(NE|NW|SE|SW)\s+\d{5}\s+\d+/', '', address)
    address = re.sub(r'\s+\d{1,2}/$', '', address)
    address = re.sub(r'\s+\d{1,2}/\d{1,2}/$', '', address)
    address = re.sub(r'\s+\d{1,2}/\d{1,2}/\d{2,4}.*$', '', address)
   
    # Remove other corrupted data patterns
    address = re.sub(r'[A-Z]{2,}[0-9)]+$', '', address)
    address = re.sub(r'\s+\([^)]*BASEMEN[^)]*\)', '', address, flags=re.IGNORECASE)
    address = re.sub(r'^-[A-Z]\s+', '', address)
   
    # Clean up spaces
    address = re.sub(r'\s+', ' ', address).strip()
    cleaned_full_address = address
   
    # Enhanced unit information extraction
    unit_patterns = [
        r',\s*(#[A-Z0-9\-]+)',  # Handle ", #225" pattern
        r'\s+(#[A-Z0-9\-]+)',   # Handle " #1025" pattern
        r'\s+(UNIT\s+[A-Z0-9\-#]+)',  # Handle "UNIT 102" or "UNIT #307"
        r'\s+(SUITE[S]?\s+[A-Z0-9\-\s]+)',
        r'\s+(APT\.?\s+[A-Z0-9\-]+)',
        r'\s+(APARTMENT\s+[A-Z0-9\-]+)',  # Added from R code
        r'\s+(STE\.?\s+[A-Z0-9\-]+)',    # Added from R code
        r'\s+([A-Z]\d+)',  # T1, D2 patterns from R code
        r'\s+\(([^)]*#[A-Z0-9\-]+)\)',  # (D#202) patterns from R code
        r'\s+(BASEMENT\s+AND\s+FIRST\s+FLOOR)',
        r'\s+(LOWER\s+LEVEL\s+AND\s+MEZZANINE\s+LEVEL)',
        r'\s+(\(MAIN\s+UNIT.*?\))',
        r'\s+(AND\s+PARKING\s+UNIT\s+[A-Z0-9\-]+)',
    ]
   
    base_address = address
    units = []
   
    for pattern in unit_patterns:
        matches = re.findall(pattern, address, flags=re.IGNORECASE)
        if matches:
            for match in matches:
                units.append(match.strip().lstrip(',').strip())  # Remove leading comma
            base_address = re.sub(pattern, '', base_address, flags=re.IGNORECASE)
   
    unit_info = '; '.join(units) if units else None
   
    # Handle multiple addresses - take the first one with a house number
    if ' AND ' in base_address and re.search(r'\d+\s+\w+.*?\s+AND\s+\d+\s+\w+', base_address):
        parts = base_address.split(' AND ')
        for part in parts:
            if re.match(r'^\d+\s+', part.strip()):
                base_address = part.strip()
                break
   
    # Handle address ranges - use the first number
    base_address = re.sub(r'^(\d+)-\d+\s+', r'\1 ', base_address)
   
    # Fix obvious street number errors (5+ digits that should be 4 or fewer)
    if re.match(r'^[1-9]\d{4,}\s+', base_address):
        match = re.match(r'^(\d)(\d{3,4})\s+(.+)', base_address)
        if match:
            base_address = match.group(2) + ' ' + match.group(3)
   
    # Handle specific OCR errors
    base_address = re.sub(r'^41110\s+', '4110 ', base_address)
   
    # Clean up trailing commas and spaces
    base_address = re.sub(r',\s*$', '', base_address)
    base_address = re.sub(r'\s+', ' ', base_address).strip()
    base_address = re.sub(r',\s*,', ',', base_address)
   
    # Flag addresses without house numbers but DON'T set to None (preserve data)
    if not re.match(r'^\d+', base_address):
        quality_flag = "no_house_number"
   
    return original_address, cleaned_full_address, base_address, unit_info, quality_flag


def should_attempt_geocoding(address, quality_flag):
    """Determine if we should attempt geocoding (be very permissive)"""
    if not address:
        return False
    # Only skip if completely unusable
    if quality_flag == "missing_address":
        return False
    # Try to geocode even poor quality addresses - let the API decide
    return True


def geocode_address(address):
    """Geocode a single address using DC's API"""
    if not address:
        return None, None, None, None, None
       
    # Try variations: original, without city/state info
    variations = [address]
   
    # Remove Washington, DC references
    no_city = re.sub(r',\s*Washington,?\s*DC,?\s*\d{5}?', '', address)
    no_city = re.sub(r',\s*Washington,?\s*DC', '', no_city).strip()
    if no_city != address and no_city:
        variations.append(no_city)
   
    for addr_variation in variations:
        encoded_address = quote(addr_variation)
        url = f"https://citizenatlas.dc.gov/newwebservices/locationverifier.asmx/findLocation2?str={encoded_address}&f=json"
       
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('returnDataset') and data['returnDataset'].get('Table1'):
                    result = data['returnDataset']['Table1'][0]
                    lat = result['LATITUDE']
                    lon = result['LONGITUDE']
                    ward_2002 = result.get('WARD_2002')
                    ward_2012 = result.get('WARD_2012')
                    ward = ward_2002 if ward_2002 else ward_2012
                    zipcode = result.get('ZIPCODE')
                    quad = result.get('QUADRANT')
                   
                    return lat, lon, ward, zipcode, quad
        except requests.exceptions.RequestException:
            continue
   
    return None, None, None, None, None


def validate_coordinates(lat, lon):
    """Validate that coordinates are within reasonable DC bounds"""
    if lat is None or lon is None:
        return False, "missing_coordinates"
    
    # DC bounds (approximate)
    if lat < 38.8 or lat > 39.0 or lon < -77.2 or lon > -76.9:
        return False, "outside_dc_bounds"
    
    return True, "valid"


def process_address(address):
    """Process a single address: parse components and geocode"""
    global total_addresses, failed_geocoding, successful_geocoding, failed_addresses, skipped_addresses
    total_addresses += 1
   
    # Parse address components
    original, cleaned_full, base_addr, unit, quality_flag = parse_address_components(address)
   
    # Initialize geocoding results
    lat, lon, ward, zipcode_api, quad_api = None, None, None, None, None
    geocoding_attempted = False
    coordinate_quality = "not_attempted"
   
    # Decide whether to attempt geocoding
    if should_attempt_geocoding(base_addr, quality_flag):
        geocoding_attempted = True
        lat, lon, ward, zipcode_api, quad_api = geocode_address(base_addr)
        
        # Validate coordinates
        coords_valid, coord_quality_flag = validate_coordinates(lat, lon)
        coordinate_quality = coord_quality_flag
        
        if coords_valid and lat is not None:
            successful_geocoding += 1
        else:
            failed_geocoding += 1
            failed_addresses.append({
                'original': original,
                'cleaned_full': cleaned_full,
                'base_address': base_addr,
                'unit': unit,
                'quality_flag': quality_flag,
                'coordinate_quality': coordinate_quality,
                'geocoding_attempted': geocoding_attempted
            })
    else:
        skipped_addresses.append({
            'original': original,
            'cleaned_full': cleaned_full,
            'base_address': base_addr,
            'unit': unit,
            'quality_flag': quality_flag,
            'reason': 'poor_address_quality'
        })
   
    return original, cleaned_full, base_addr, unit, lat, lon, ward, zipcode_api, quad_api, quality_flag, coordinate_quality, geocoding_attempted


# Process all addresses with progress bar
print("Processing addresses...")
tqdm.pandas(desc="Geocoding addresses")
results = df['full_address'].progress_apply(lambda x: pd.Series(process_address(x)))


# Assign results to new columns
df['address_original'] = results[0]
df['address_cleaned'] = results[1]
df['address_base'] = results[2]
df['unit'] = results[3]
df['lat'] = results[4]
df['lng'] = results[5]
df['ward'] = results[6]
df['zipcode_api'] = results[7]
df['quad_api'] = results[8]
df['address_quality_flag'] = results[9]
df['coordinate_quality_flag'] = results[10]
df['geocoding_attempted'] = results[11]


# Fill in missing zipcode and quad with API data, preserving existing data
df['zipcode'] = df['zipcode'].fillna(df['zipcode_api'])
df['quad'] = df['quad'].fillna(df['quad_api'])


# Clean up temporary columns
df = df.drop(columns=['zipcode_api', 'quad_api'])


# Convert 'eviction_date' to datetime format and add month and year columns
df['eviction_date'] = pd.to_datetime(df['eviction_date'])
df['month'] = df['eviction_date'].dt.month
df['year'] = df['eviction_date'].dt.year
df['month_name'] = df['eviction_date'].dt.strftime('%B')


# Remove redundant columns
columns_to_drop = []
if 'defendant_address' in df.columns:
    columns_to_drop.append('defendant_address')
if 'full_address' in df.columns and 'address_cleaned' in df.columns:
    columns_to_drop.append('full_address')


if columns_to_drop:
    df = df.drop(columns=columns_to_drop)
    print(f"Removed redundant columns: {columns_to_drop}")


# Save the updated DataFrame
df.to_csv('eviction_data_ward.csv', index=False)


# Print detailed summary report
print("\n" + "="*60)
print("PROCESSING SUMMARY REPORT")
print("="*60)
print(f"Total addresses processed: {total_addresses:,}")
print(f"Geocoding attempted: {total_addresses - len(skipped_addresses):,}")
print(f"Geocoding skipped: {len(skipped_addresses):,}")
print(f"Successfully geocoded: {successful_geocoding:,}")
print(f"Failed to geocode: {failed_geocoding:,}")
if total_addresses - len(skipped_addresses) > 0:
    print(f"Success rate (of attempted): {(successful_geocoding/(total_addresses - len(skipped_addresses)))*100:.1f}%")
print(f"Overall success rate: {(successful_geocoding/total_addresses)*100:.1f}%")


# Show failed geocoding details
if failed_addresses:
    print(f"\nFAILED GEOCODING DETAILS ({len(failed_addresses)} addresses):")
    print("-" * 60)
    for i, failed in enumerate(failed_addresses[:20], 1):  # Show first 20
        print(f"{i:2d}. Original: {failed['original']}")
        print(f"    Base:     {failed['base_address']}")
        if failed['unit']:
            print(f"    Unit:     {failed['unit']}")
        print()
   
    if len(failed_addresses) > 20:
        print(f"... and {len(failed_addresses) - 20} more failed addresses")


# Show data coverage summary
total_rows = len(df)
geocoded_rows = len(df[df['lat'].notna()])
existing_quad = len(df[df['quad'].notna()])
existing_zip = len(df[df['zipcode'].notna()])


print(f"\nDATA COVERAGE SUMMARY:")
print("-" * 30)
print(f"Total records: {total_rows:,}")
print(f"Records with coordinates: {geocoded_rows:,} ({(geocoded_rows/total_rows)*100:.1f}%)")
print(f"Records with quad data: {existing_quad:,} ({(existing_quad/total_rows)*100:.1f}%)")
print(f"Records with zipcode data: {existing_zip:,} ({(existing_zip/total_rows)*100:.1f}%)")


print(f"\nFinal DataFrame Structure:")
print(f"  • address_original: Raw address from source data")
print(f"  • address_cleaned: Address with typos fixed")
print(f"  • address_base: Base address without unit info (used for geocoding)")
print(f"  • unit: Extracted unit/suite/apartment information")
print(f"  • lat/lng: Coordinates from geocoding")
print(f"  • ward: DC ward from geocoding")
print(f"  • zipcode: Existing zipcode filled with API data where missing")
print(f"  • quad: Existing quad filled with API data where missing")
print("="*60)