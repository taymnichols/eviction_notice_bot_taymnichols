import requests
from bs4 import BeautifulSoup
import os
import tabula
import pandas as pd
import re


total_records_saved = 0
total_skipped_with_data = 0
skipped_with_data = []  # Store skipped rows that have some data


# Step 1: Scrape the website and extract PDF URLs
url = "https://ota.dc.gov/page/scheduled-evictions"
response = requests.get(url)
pdf_urls = [a["href"] for a in BeautifulSoup(response.text, "html.parser").find_all("a", href=True) if a["href"].endswith(".pdf")]


# Step 2: Download PDF files
pdf_directory = "pdf_files"
os.makedirs(pdf_directory, exist_ok=True)
for pdf_url in pdf_urls:
  pdf_filename = pdf_url.split("/")[-1]
  if pdf_filename not in os.listdir(pdf_directory):
      with open(os.path.join(pdf_directory, pdf_filename), "wb") as f:
          f.write(requests.get(pdf_url).content)


# Step 3: Extract tables from PDF
csv_directory = "csv_files"
os.makedirs(csv_directory, exist_ok=True)
unique_rows = set()


def normalize_address(address):
   """
   Cleans and standardizes an address string to improve duplicate detection.
   """
   if not isinstance(address, str):
       return ''


   # 1. Convert to lowercase for reliable matching
   address = address.lower()
  
   # Remove "Also Known As" variations
   address = address.split('a/k/a')[0]


   # Standardize unit types
   replacements = {
       'apartment': 'apt',
       'apt': '#',
       'unit': '#',
       '#': '#' # Ensures consistency
   }
   for old, new in replacements.items():
       address = address.replace(old, new)


   # Standardize street types
   street_replacements = {
       'street': 'st',
       'avenue': 'ave',
       'road': 'rd',
       'drive': 'dr',
       'place': 'pl',
       'boulevard': 'blvd',
       'court': 'ct',
       'terrace': 'ter'
   }
   # Use word boundaries to avoid replacing parts of words
   for old, new in street_replacements.items():
       address = re.sub(r'\b' + old + r'\b', new, address)


   # Remove all punctuation and collapse whitespace
   address = re.sub(r'[^\w\s#]', '', address)
   address = re.sub(r'\s+', ' ', address).strip()


   # 2. Convert the final, clean string to Title Case
   address = address.title()


   return address


def process_and_split_rows(pdf_tables):
   """
   Processes tables, splits merged records, and now ACCEPTS rows without case numbers.
   """
   global total_skipped_with_data, skipped_with_data
   all_cleaned_rows = []


   case_pattern = re.compile(r'(\d+-[A-Z]+-\d+(?:-[A-Z])?|\b\d{2,}-\d{2,3}\b|LTB-\d+-\d+)')
   date_pattern = re.compile(r'\d{1,2}/\d{1,2}/\d{2,4}')
   zip_pattern = re.compile(r'20\d{3}')
   quad_pattern = re.compile(r'\b(NW|NE|SW|SE)\b')
   junk_pattern = re.compile(r'case number|defendant address|eviction date|page \d', re.IGNORECASE)


   if not pdf_tables:
       return pd.DataFrame()


   for table in pdf_tables:
       table.columns = range(table.shape[1])
       for _, row in table.iterrows():
           row_str = ' '.join(str(item) for item in row.dropna())


           if len(row_str.strip()) < 10 or junk_pattern.search(row_str):
               continue


           cases = case_pattern.findall(row_str)
           dates = date_pattern.findall(row_str)
           zips = zip_pattern.findall(row_str)
           quads = quad_pattern.findall(row_str)


           if cases and len(cases) == len(dates):
               # --- LOGIC FOR ROWS WITH CASE NUMBERS (EXISTING LOGIC) ---
               address_block = row_str
               for item in cases + dates + zips + quads:
                   address_block = address_block.replace(item, '')
               address_block = re.sub(r'\bnan\b', '', address_block, flags=re.IGNORECASE).strip()
               addresses = re.split(r'\s+(?=\d{3,})', address_block)


               if len(cases) == len(addresses):
                   for i in range(len(cases)):
                       all_cleaned_rows.append([cases[i], addresses[i].strip(" ,-"), quads[i] if i < len(quads) else '', zips[i] if i < len(zips) else '', dates[i]])
               else:
                   all_cleaned_rows.append([cases[0], address_block, quads[0] if quads else '', zips[0] if zips else '', dates[0]])
          
           # --- NEW LOGIC TO CAPTURE ROWS WITHOUT CASE NUMBERS ---
           elif not cases and dates:
               case_number = '' # Assign a blank case number
               eviction_date = dates[0]
               zipcode = zips[0] if zips else ''
               quad = quads[0] if quads else ''


               # Isolate address by removing other found data
               address = row_str.replace(eviction_date, '')
               if zipcode: address = address.replace(zipcode, '')
               address = re.sub(r'\bnan\b', '', address, flags=re.IGNORECASE).strip(" ,-")
              
               if len(address) > 5: # Make sure we have a real address
                   all_cleaned_rows.append([case_number, address, quad, zipcode, eviction_date])
          
           # --- Rows that are still skipped are truly junk ---
           else:
               total_skipped_with_data += 1
               skipped_with_data.append({'text': row_str[:100]})


   return pd.DataFrame(all_cleaned_rows, columns=['Case Number', 'Defendant Address', 'Quad', 'Zipcode', 'Eviction Date'])


# Process PDFs
for pdf_filename in os.listdir(pdf_directory):
  if not pdf_filename.endswith('.pdf'): continue
  pdf_path = os.path.join(pdf_directory, pdf_filename)
  print(f"Processing {pdf_filename}...")
  pdf_tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True, stream=True)
  cleaned_table = process_and_split_rows(pdf_tables)
  if not cleaned_table.empty:
      cleaned_rows_tuples = [tuple(row) for row in cleaned_table.values]
      unique_rows.update(cleaned_rows_tuples)


# Create DataFrame from all newly parsed unique rows
final_df = pd.DataFrame(list(unique_rows), columns=['Case Number', 'Defendant Address', 'Quad', 'Zipcode', 'Eviction Date'])


## Step 4: Load existing CSV and combine with new data
csv_path = "eviction_notices.csv"
if os.path.exists(csv_path):
  existing_df = pd.read_csv(csv_path)
else:
  existing_df = pd.DataFrame(columns=final_df.columns)


# Combine old and new data into one big dataframe
combined_df = pd.concat([existing_df, final_df], ignore_index=True)




# --- FINAL, MULTI-STAGE DEDUPLICATION LOGIC ---


# 1. Standardize and Pre-Clean all data
combined_df['Eviction Date'] = pd.to_datetime(combined_df['Eviction Date'], errors='coerce')
combined_df['Case Number'].fillna('', inplace=True)
combined_df['Defendant Address'].fillna('', inplace=True)
combined_df['Defendant Address'] = combined_df['Defendant Address'].astype(str).str.strip().str.rstrip(' ,.')
combined_df['Zipcode'] = combined_df['Zipcode'].astype(str).str.replace('.0', '', regex=False)
combined_df['Case Number'] = combined_df['Case Number'].astype(str).str.strip()
combined_df['Normalized Address'] = combined_df['Defendant Address'].apply(normalize_address)


# -- STAGE 1: Perfect the "Verified" Records (those with Case Numbers) --
verified_df = combined_df[combined_df['Case Number'] != ''].copy()
# Sort to bring the most complete records (with dates) to the top
verified_df.sort_values(by=['Case Number', 'Eviction Date'], ascending=[True, False], inplace=True)
# Drop duplicates, keeping only the best version of each verified record
verified_df.drop_duplicates(subset=['Case Number'], keep='first', inplace=True)




# -- STAGE 2: Filter the "Unverified" Records against the Verified ones --
unverified_df = combined_df[combined_df['Case Number'] == ''].copy()
# Use a merge to find any unverified records that are already covered by a verified one
merged = unverified_df.merge(
   verified_df[['Normalized Address', 'Eviction Date']],
   on=['Normalized Address', 'Eviction Date'],
   how='left',
   indicator=True
)
# Keep only the unverified records that did NOT find a match
new_unverified_df = merged[merged['_merge'] == 'left_only'].drop(columns='_merge')




# -- STAGE 3: Deduplicate the remaining Unverified Records --
# Sort to bring the most complete records (with dates) to the top
new_unverified_df.sort_values(by=['Normalized Address', 'Eviction Date'], ascending=[True, False], inplace=True)
# Drop duplicates within the unverified set
final_unverified_df = new_unverified_df.drop_duplicates(subset=['Normalized Address'], keep='first')




# -- STAGE 4: Combine the clean sets --
final_df = pd.concat([verified_df, final_unverified_df], ignore_index=True)




# --- Final Formatting and Save ---
final_df['City'] = 'Washington, DC'
final_df['Full Address'] = (
   final_df['Defendant Address'].astype(str) +
   final_df['Quad'].apply(lambda x: f', {x}' if x and pd.notna(x) and x != '' and x != 'nan' else '') +
   ', ' + final_df['City'] +
   final_df['Zipcode'].apply(lambda x: f', {x}' if x and pd.notna(x) and x != '' and x != 'nan' else '')
)
final_df['Eviction Date'] = final_df['Eviction Date'].dt.strftime('%Y-%m-%d').replace('NaT', '')


# Clean up temporary columns and save the final, clean CSV
final_df.drop(columns=['Normalized Address'], inplace=True)


# Separate deduplication for records without dates
dateless_records = final_df[final_df['Eviction Date'].isna() | (final_df['Eviction Date'] == '')]
dated_records = final_df[~(final_df['Eviction Date'].isna() | (final_df['Eviction Date'] == ''))]


if not dateless_records.empty:
   print(f"Processing {len(dateless_records)} dateless records for deduplication...")
  
   # For dateless records, dedupe on case number + normalized address only
   dateless_records['Normalized Address'] = dateless_records['Defendant Address'].apply(normalize_address)
  
   # Sort to prioritize records with case numbers
   dateless_records.sort_values(by=['Case Number', 'Normalized Address'],
                                ascending=[False, True], inplace=True)
  
   # Dedupe: if same address, keep the one with a case number (if any)
   dateless_deduped = dateless_records.drop_duplicates(
       subset=['Normalized Address'],
       keep='first'  # Keeps first occurrence (prioritized by sort above)
   )
  
   dateless_deduped.drop(columns=['Normalized Address'], inplace=True)
   print(f"Removed {len(dateless_records) - len(dateless_deduped)} duplicate dateless records")
  
   # Combine back with dated records
   final_df = pd.concat([dated_records, dateless_deduped], ignore_index=True)
else:
   print("No dateless records found.")
  
final_df.to_csv(csv_path, index=False)




def print_final_summary():
   # We'll use the final DataFrame for the summary
   global combined_df
   combined_df = final_df
  
   print("\n" + "="*60)
   print("PROCESSING SUMMARY")
   print("="*60)
   print(f"Records successfully saved: {len(combined_df):,}")
   print(f"Rows with partial data (skipped): {total_skipped_with_data:,}")
   if skipped_with_data:
       print(f"\nSKIPPED ROWS WITH DATA ({len(skipped_with_data)} total):")
       print("-" * 60)
       for i, row in enumerate(skipped_with_data[:20]):
           print(f"{i+1:2d}. {row['text']}")
       if len(skipped_with_data) > 20:
           print(f"... and {len(skipped_with_data) - 20} more rows")
   print("\n" + "="*60)


print_final_summary()

