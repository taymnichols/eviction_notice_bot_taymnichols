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

# Step 4: Load existing CSV if present, otherwise create a new DataFrame
csv_path = "eviction_notices.csv"
if os.path.exists(csv_path):
    existing_df = pd.read_csv(csv_path)
else:
    existing_df = pd.DataFrame()

# Create DataFrame with unique rows and add a header row
final_df = pd.DataFrame(unique_rows, columns=header)

# Split 'Defendant Address' column into 'Street Address' and 'Apartment Number'
final_df[['Street Address', 'Apartment Number']] = final_df['Defendant Address'].str.split('#', n=1, expand=True)

# Replace None with 'N/A' in 'Apartment Number' column
final_df['Apartment Number'] = final_df['Apartment Number'].fillna('N/A')

# Remove leading and trailing whitespace from both new columns
final_df['Street Address'] = final_df['Street Address'].str.strip()
final_df['Apartment Number'] = final_df['Apartment Number'].str.strip()

# Add new column 'City' with value 'Washington, D.C.'
final_df['City'] = 'WASHINGTON, D.C.'

# Remove original 'Defendant Address' column
final_df.drop(columns=['Defendant Address'], inplace=True)

# Remove columns with all NaN values
final_df = final_df.dropna(axis=1, how='all')

# Drop duplicate rows based on all columns
final_df = final_df.drop_duplicates()

# Rearrange columns in final_df
final_df = final_df[['Case Number', 'Eviction Date', 'Street Address', 'Apartment Number', 'Quad', 'City', 'Zipcode']]

# Count distinct rows before removing duplicates
distinct_rows_before = existing_df.shape[0]

# Combine existing DataFrame with final_df and drop duplicates
combined_df = pd.concat([existing_df, final_df], ignore_index=True)
combined_df.drop_duplicates(inplace=True)

# Calculate distinct rows after removing duplicates
distinct_rows_after = combined_df.shape[0]

# Save the combined DataFrame to CSV
try:
    combined_df.to_csv(csv_path, index=False)
except Exception as e:
    print(f"Error saving combined DataFrame to CSV: {e}")

# Print number of new distinct rows added
print(f"Number of new distinct rows added: {distinct_rows_after - distinct_rows_before}")

#if new_pdfs:
    #slack_token = os.environ.get('SLACK_API_TOKEN')

   # client = WebClient(token=slack_token)
    #msg = f"New PDFs downloaded: {', '.join(new_pdfs)}\nNumber of new scheduled evictions added: {distinct_rows_after - distinct_rows_before}."

    #try:
        #response = client.chat_postMessage(
           # channel="slack-bots",
           # text=msg,
          #  unfurl_links=True, 
           # unfurl_media=True
        #)
      #  print("Message sent successfully!")
   # except SlackApiError as e:
     #   assert e.response["ok"] is False
      #  assert e.response["error"]
      #  print(f"Error sending message: {e.response['error']}")
