import requests
from bs4 import BeautifulSoup
import os
import tabula
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Read SMTP configuration from environment variables
email_sender = os.environ.get('EMAIL_SENDER')
email_recipient = os.environ.get('EMAIL_RECIPIENT')
sendgrid_api_key = os.environ.get('SENDGRID_API_KEY')
smtp_server = 'smtp.sendgrid.net'
smtp_port = 587

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

# Remove columns with all NaN values
final_df = final_df.dropna(axis=1, how='all')

# Drop duplicate rows based on all columns
final_df = final_df.drop_duplicates()

# Count distinct rows before removing duplicates
distinct_rows_before = final_df.shape[0]

# Convert the 'Eviction Date' column to datetime format
final_df['Eviction Date'] = pd.to_datetime(final_df['Eviction Date'], errors='coerce')

# Sort the DataFrame by 'Eviction Date' and then by 'Case Number'
final_df = final_df.sort_values(by=['Eviction Date', 'Case Number'])

# Save the final DataFrame to CSV
try:
    final_df.to_csv(csv_path, index=False)
except Exception as e:
    print(f"Error saving final DataFrame to CSV: {e}")

if new_pdfs:
    email_subject = "New PDFs Downloaded"
    email_body = f"New PDFs downloaded: {', '.join(new_pdfs)}"

 # Count distinct rows after removing duplicates
distinct_rows_after = final_df.shape[0]

# Email Notification setup
if new_pdfs:
    email_subject = "New PDFs Downloaded"
    email_body = f"New PDFs downloaded: {', '.join(new_pdfs)}\nDistinct rows added: {distinct_rows_after - distinct_rows_before}"

    # Create message
    msg = MIMEMultipart()
    msg['From'] = email_sender
    msg['To'] = email_recipient
    msg['Subject'] = email_subject
    msg.attach(MIMEText(email_body, 'plain'))

    # Send email
    smtp_server.send_message(msg)
    smtp_server.quit()

