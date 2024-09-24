import requests
from bs4 import BeautifulSoup
import os
import tabula
import pandas as pd

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
    pdf_tables = tabula.read_pdf(os.path.join(pdf_directory, pdf_filename), pages='all', multiple_tables=True)
    if pdf_tables:  # Check if tables exist in the PDF
        first_table = pdf_tables[0]  # Get the first table from the PDF
        header = list(first_table.columns)  # Extract the header row from the first table
        # Flatten and convert all tables to tuples of rows
        all_table_rows = [tuple(row) for table in pdf_tables for row in table.values]
        unique_rows.update(all_table_rows)  # Add unique rows to the set

        # Save each table as a CSV
        for i, table in enumerate(pdf_tables):
            csv_filename = f"{pdf_filename[:-4]}_table_{i+1}.csv"
            csv_path = os.path.join(csv_directory, csv_filename)
            try:
                table.to_csv(csv_path, index=False)
            except Exception as e:
                print(f"Error saving CSV file {csv_filename}: {e}")

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
    final_df['Eviction Date'] = pd.to_datetime(final_df['Eviction Date'], errors='coerce')
    existing_df['Eviction Date'] = pd.to_datetime(existing_df['Eviction Date'], errors='coerce')

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

    # Calculate the number of new rows added
    new_rows_added = new_rows.shape[0]
    
else:
    # If existing_df is empty, set combined_df to final_df
    combined_df = final_df.copy()

    # Optionally, print a message or perform any other actions
    print("No data found in the existing DataFrame. Skipping duplicate identification process.")

# Convert "Eviction Date" column to datetime type, handling different date formats
combined_df['Eviction Date'] = pd.to_datetime(combined_df['Eviction Date'], errors='coerce')

# Check for any entries that could not be converted to datetime
invalid_dates = combined_df[combined_df['Eviction Date'].isna()]['Eviction Date']

# Convert the remaining valid dates to the expected format
combined_df.loc[~combined_df['Eviction Date'].isna(), 'Eviction Date'] = combined_df.loc[~combined_df['Eviction Date'].isna(), 'Eviction Date'].dt.strftime('%m/%d/%Y')

# Print out the invalid dates
if not invalid_dates.empty:
    print("Invalid dates detected:")
    print(invalid_dates)

# Now convert the 'Eviction Date' column to datetime again
combined_df['Eviction Date'] = pd.to_datetime(combined_df['Eviction Date'], errors='coerce')

# Convert zipcode col to integer
combined_df['Zipcode'] = combined_df['Zipcode'].fillna(-1).astype(int)

# Add a column for the city
combined_df['City'] = 'Washington, DC'

# Create a new column 'full_address' by concatenating the existing columns
combined_df['Full Address'] = combined_df['Defendant Address'] + ', ' + combined_df['Quad'] + ', ' + combined_df['City'] + ', ' + combined_df['Zipcode'].astype(str)

# Assuming final_df is your DataFrame
combined_df.drop_duplicates(inplace=True)

# Save the combined DataFrame to CSV
try:
    combined_df.to_csv(csv_path, index=False)
except Exception as e:
    print(f"Error saving combined DataFrame to CSV: {e}")

