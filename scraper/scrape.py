import requests
from bs4 import BeautifulSoup
import os
import tabula
import pandas as pd
from slack import WebClient
from slack.errors import SlackApiError

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

header = None  # Initialize header to None
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

# Create DataFrame with unique rows and add a header row
final_df = pd.DataFrame(unique_rows, columns=header)

# Remove columns with all NaN values
final_df = final_df.dropna(axis=1, how='all')

# Drop duplicate rows based on all columns
final_df = final_df.drop_duplicates()

## Step 4: Load existing CSV if present, otherwise create a new DataFrame
csv_path = "eviction_notices.csv"
if os.path.exists(csv_path):
    existing_df = pd.read_csv(csv_path)
else:
    # If the CSV doesn't exist, create a new DataFrame with the header row from the first PDF
    existing_df = pd.DataFrame(columns=header)

# Filter out columns with no data or with names like "Unnamed"
existing_df = existing_df.loc[:, ~existing_df.columns.str.startswith('Unnamed')].dropna(axis=1, how='all')

# Check if existing_df is empty
if not existing_df.empty:
    # Count distinct rows before removing duplicates
    distinct_rows_before = existing_df.shape[0]

    # Identify new rows by checking for duplicates based on "Case Number" and "Eviction Date" columns
    existing_case_numbers_dates = existing_df[['Case Number', 'Eviction Date']]
    new_case_numbers_dates = final_df[['Case Number', 'Eviction Date']]

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

# Save the combined DataFrame to CSV
try:
    combined_df.to_csv(csv_path, index=False)
except Exception as e:
    print(f"Error saving combined DataFrame to CSV: {e}")

# Convert "Eviction Date" column to datetime type
combined_df['Eviction Date'] = pd.to_datetime(combined_df['Eviction Date'], format='%m/%d/%Y')

latest_date = combined_df['Eviction Date'].max().strftime('%B %d, %Y')  # Get the latest date in eviction_notices.csv

print(f"Number of new rows added: {new_rows.shape[0]}")
print({latest_date})

if new_pdfs:
   slack_token = os.environ.get('SLACK_API_TOKEN')


   client = WebClient(token=slack_token)
   msg = f"There is new data available on scheduled evictions through {latest_date}. There were {new_rows_added} new scheduled evictions added to your dataset."


   try:
       response = client.chat_postMessage(
           channel="slack-bots",
           text=msg,
           unfurl_links=True,
           unfurl_media=True
       )
       print("Message sent successfully!")
   except SlackApiError as e:
       assert e.response["ok"] is False
       assert e.response["error"]
       print(f"Error sending message: {e.response['error']}")