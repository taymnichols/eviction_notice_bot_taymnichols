import requests
from bs4 import BeautifulSoup
import os
import tabula
import pandas as pd

# Step 1: Scrape the website and extract PDF URLs
url = "https://ota.dc.gov/page/scheduled-evictions"
response = requests.get(url)
pdf_urls = [a["href"] for a in BeautifulSoup(response.text, "html.parser").find_all("a", href=True) if a["href"].endswith(".pdf")]

# Step 2: Download PDF files
pdf_directory = "pdf_files"
os.makedirs(pdf_directory, exist_ok=True)
[open(os.path.join(pdf_directory, pdf_url.split("/")[-1]), "wb").write(requests.get(pdf_url).content) for pdf_url in pdf_urls]

# Step 3: Extract tables from PDF and save as CSVs, ensuring unique rows
csv_directory = "csv_files"
os.makedirs(csv_directory, exist_ok=True)

header = None  # Initialize header to None
unique_rows = set()  # Set to keep track of unique rows

for pdf_filename in os.listdir(pdf_directory):
    pdf_tables = tabula.io.read_pdf(os.path.join(pdf_directory, pdf_filename), pages='all', multiple_tables=True)
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
            table.to_csv(csv_path, index=False)

# Step 4: Create DataFrame with unique rows and add a header row
final_df = pd.DataFrame(unique_rows, columns=header)

# Remove columns with all NaN values
final_df = final_df.dropna(axis=1, how='all')

# Drop duplicate rows based on all columns
final_df = final_df.drop_duplicates()

# Convert the 'Eviction Date' column to datetime format
final_df['Eviction Date'] = pd.to_datetime(final_df['Eviction Date'], errors='coerce')

# Sort the DataFrame by 'Eviction Date' and then by 'Case Number'
final_df = final_df.sort_values(by=['Eviction Date', 'Case Number'])

final_df.to_csv("eviction_notices.csv", index=False)
