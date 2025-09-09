import requests
from bs4 import BeautifulSoup
import os
import tabula
import pandas as pd
import re


# Step 1: Scrape the website and extract PDF URLs
url = "https://ota.dc.gov/page/scheduled-evictions"
response = requests.get(url)
pdf_urls = [a["href"] for a in BeautifulSoup(response.text, "html.parser").find_all("a", href=True) if a["href"].endswith(".pdf")]


# Step 2: Download PDF files if they are not already present in the pdf_files directory
pdf_directory = "pdf_files"
os.makedirs(pdf_directory, exist_ok=True)
new_pdfs = []  # List to store names of newly downloaded PDFs
for pdf_url in pdf_urls:
   pdf_filename = pdf_url.split("/")[-1]
   if pdf_filename not in os.listdir(pdf_directory):
       with open(os.path.join(pdf_directory, pdf_filename), "wb") as f:
           f.write(requests.get(pdf_url).content)
       new_pdfs.append(pdf_filename)  # Record newly downloaded PDFs


# Step 3: Extract tables from PDF and save as CSVs, ensuring unique rows
csv_directory = "csv_files"
os.makedirs(csv_directory, exist_ok=True)


unique_rows = set()  # Set to keep track of unique rows


for pdf_filename in os.listdir(pdf_directory):
   pdf_tables = tabula.read_pdf(
       os.path.join(pdf_directory, pdf_filename),
       pages='all',
       multiple_tables=True,
       lattice=True, 
       guess=False,  
       stream=True   
   )
   if pdf_tables:  # Check if tables exist in the PDF
       # Function to properly split address and zipcode
       def clean_address_data(df):
           # Convert all values to string and join
           text_data = df.astype(str).apply(' '.join, axis=1)
          
           print("\n=== Input Data Sample ===")
           print("First 5 rows of raw input:")
           print(df.head())
           print("\nFirst 5 rows after text joining:")
           print(text_data.head())


           cleaned_rows = []
           skipped_rows = []
          
           for text in text_data:
               try:
                   # Extract case number
                   case_match = re.search(r'(?:\d+-[A-Z]+-\d+|\d+-\d+|[A-Z]+-\d+)', text)
                   case_number = case_match.group() if case_match else ''
                  
                   # Extract zipcode
                   zipcode_match = re.search(r'20\d{3}(?:\.\d+)?', text)
                   zipcode = zipcode_match.group().split('.')[0] if zipcode_match else ''
                  
                   # Extract quadrant
                   quad_match = re.search(r'\b(NW|NE|SW|SE)\b', text)
                   quad = quad_match.group() if quad_match else ''
                  
                   # Extract date
                   date_match = re.search(r'\d{1,2}/\d{1,2}/\d{2,4}', text)
                   date = date_match.group() if date_match else ''
                  
                   # Extract address
                   address = text
                   if case_match:
                       address = address[case_match.end():].strip()
                   if zipcode_match:
                       address = address[:zipcode_match.start()].strip()
                   if date_match:
                       address = address[:date_match.start()].strip()
                  
                   # Detailed debugging for each skipped row
                   if not (case_number and address and zipcode and date):
                       print("\n=== Skipped Row Analysis ===")
                       print(f"Original text: {text}")
                       print("Extracted components:")
                       print(f"- Case Number: {case_number if case_number else 'MISSING'}")
                       print(f"- Address: {address if address else 'MISSING'}")
                       print(f"- Quad: {quad if quad else 'MISSING'}")
                       print(f"- Zipcode: {zipcode if zipcode else 'MISSING'}")
                       print(f"- Date: {date if date else 'MISSING'}")
                       print("Regex matches:")
                       print(f"- Case number match: {case_match.group() if case_match else 'NO MATCH'}")
                       print(f"- Zipcode match: {zipcode_match.group() if zipcode_match else 'NO MATCH'}")
                       print(f"- Quad match: {quad_match.group() if quad_match else 'NO MATCH'}")
                       print(f"- Date match: {date_match.group() if date_match else 'NO MATCH'}")
                      
                   if case_number and address and zipcode and date:
                       if zipcode.startswith('20'):
                           cleaned_rows.append([case_number, address, quad, zipcode, date])
                   else:
                       missing = []
                       if not case_number: missing.append('case_number')
                       if not address: missing.append('address')
                       if not zipcode: missing.append('zipcode')
                       if not date: missing.append('date')
                       skipped_rows.append({
                           'text': text,
                           'missing': missing
                       })
                              
               except Exception as e:
                   print(f"\n=== Error Processing Row ===")
                   print(f"Row text: {text[:100]}...")
                   print(f"Error: {str(e)}")
                   continue


           print(f"\nSummary:")
           print(f"Successfully cleaned {len(cleaned_rows)} rows")
           print(f"Skipped {len(skipped_rows)} rows")


           return pd.DataFrame(cleaned_rows, columns=['Case Number', 'Defendant Address', 'Quad', 'Zipcode', 'Eviction Date'])
                  


       for i, table in enumerate(pdf_tables):
           # Clean the data
           cleaned_table = clean_address_data(table)
           # Convert to tuples and add to unique rows
           cleaned_rows = [tuple(row) for row in cleaned_table.values]
           unique_rows.update(cleaned_rows)


old_data_csv = os.path.join(csv_directory, "evictions_jan_apr.csv")  # Path to the old data CSV file
#Add rows from old data CSV to unique_rows
if os.path.exists(old_data_csv):
   old_data_df = pd.read_csv(old_data_csv)
   old_data_rows = [tuple(row) for row in old_data_df.values]
   unique_rows.update(old_data_rows)


# Create DataFrame with unique rows and add a header row
final_df = pd.DataFrame(unique_rows)


#Remove columns with all NaN values
final_df = final_df.dropna(axis=1, how='all')


# If there are still columns with no data, remove them
final_df = final_df.loc[:, final_df.notna().any()]


# Drop duplicate rows based on all columns
final_df = final_df.drop_duplicates()


final_df.columns = ['Case Number', 'Defendant Address', 'Quad', 'Zipcode', 'Eviction Date']


# Drop empty rows from final_df
final_df.dropna(inplace=True)


## Step 4: Load existing CSV if present, otherwise create a new DataFrame
csv_path = "eviction_notices.csv"
if os.path.exists(csv_path):
   existing_df = pd.read_csv(csv_path)
else:
   # If the CSV doesn't exist, create a new DataFrame with the same column names as final_df
   existing_df = pd.DataFrame(columns=final_df.columns)


# Drop empty rows from existing_df
existing_df.dropna(inplace=True)


# Filter out columns with no data or with names like "Unnamed"
existing_df = existing_df.loc[:, ~existing_df.columns.str.startswith('Unnamed')].dropna(axis=1, how='all')


# Check if existing_df is empty
if not existing_df.empty:
   # Convert 'Eviction Date' columns to datetime in both DataFrames
   final_df['Eviction Date'] = pd.to_datetime(final_df['Eviction Date'], errors='coerce').dt.date
   existing_df['Eviction Date'] = pd.to_datetime(existing_df['Eviction Date'], errors='coerce').dt.date


   # Identify entries that could not be converted in final_df
   invalid_dates_final = final_df[final_df['Eviction Date'].isna()]
   print("Entries in final_df that could not be converted to datetime:")
   print(invalid_dates_final[['Case Number', 'Eviction Date']])


   # Identify entries that could not be converted in existing_df
   invalid_dates_existing = existing_df[existing_df['Eviction Date'].isna()]
   print("Entries in existing_df that could not be converted to datetime:")
   print(invalid_dates_existing[['Case Number', 'Eviction Date']])


   # Merge final_df with existing_df on the specific columns
   merged_df = final_df.merge(existing_df[['Case Number', 'Eviction Date']], on=['Case Number', 'Eviction Date'], how='left', indicator=True)


   # Filter only the rows that are not present in existing_df
   new_rows = merged_df[merged_df['_merge'] == 'left_only'].drop(columns='_merge')


   # Add only new rows to the existing DataFrame
   combined_df = pd.concat([existing_df, new_rows], ignore_index=True)

   # Ensure all zipcodes are strings and clean
   combined_df['Zipcode'] = combined_df['Zipcode'].astype(str).str.replace('.0', '')


   # Calculate the number of new rows added
   new_rows_added = new_rows.shape[0]
  
else:
   # If existing_df is empty, set combined_df to final_df
   combined_df = final_df.copy()

   # Optionally, print a message or perform any other actions
   print("No data found in the existing DataFrame. Skipping duplicate identification process.")


# Ensure all zipcodes are strings and clean
combined_df['Zipcode'] = combined_df['Zipcode'].astype(str).str.replace('.0', '')

# Convert "Eviction Date" column to datetime type, handling different date formats
combined_df['Eviction Date'] = pd.to_datetime(combined_df['Eviction Date'], errors='coerce').dt.date


# Check for any entries that could not be converted to datetime
invalid_dates = combined_df[combined_df['Eviction Date'].isna()]['Eviction Date']


# Print out the invalid dates
if not invalid_dates.empty:
   print("Invalid dates detected:")
   print(invalid_dates)


# Convert the 'Eviction Date' column to string format to exclude time
combined_df['Eviction Date'] = combined_df['Eviction Date'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else '')


# Add these debug statements right before the zipcode conversion line:
print("\nUnique values in Zipcode column:")
print(combined_df['Zipcode'].unique())


print("\nRows with problematic zipcodes:")
print(combined_df[combined_df['Zipcode'].astype(str).str.contains('[^0-9.-]', na=False)][['Case Number', 'Defendant Address', 'Zipcode']])


# Convert zipcode to numeric and validate data
# Convert zipcodes to strings and pad to 5 digits
combined_df['Zipcode'] = combined_df['Zipcode'].astype(str).str.replace('.0', '')  # Remove decimal points
combined_df = combined_df[combined_df['Zipcode'].str.match(r'^200\d{2}$')]  # Valid DC zipcodes
combined_df = combined_df[combined_df['Case Number'].notna()]  # Must have case number
combined_df = combined_df[combined_df['Eviction Date'].notna()]  # Must have date


# Add a column for the city
combined_df['City'] = 'Washington, DC'


# Create a new column 'full_address' by concatenating the existing columns
combined_df['Full Address'] = combined_df['Defendant Address'] + ', ' + combined_df['Quad'] + ', ' + combined_df['City'] + ', ' + combined_df['Zipcode']


# Assuming final_df is your DataFrame
combined_df.drop_duplicates(inplace=True)


# Save the combined DataFrame to CSV
try:
   combined_df.to_csv(csv_path, index=False)
except Exception as e:
   print(f"Error saving combined DataFrame to CSV: {e}")