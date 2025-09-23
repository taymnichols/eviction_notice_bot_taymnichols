import requests
from bs4 import BeautifulSoup
import os
import pdfplumber
import pandas as pd
import re
import pytesseract
from PIL import Image
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

total_records_saved = 0
total_skipped_with_data = 0
skipped_with_data = []

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

# Step 3: Enhanced extraction and processing
csv_directory = "csv_files"
os.makedirs(csv_directory, exist_ok=True)
unique_rows = set()

def normalize_address(address):
    """Enhanced address normalization"""
    if not isinstance(address, str):
        return ''
    address = address.lower().split('a/k/a')[0]
    replacements = {'apartment': 'apt', 'apt': '#', 'unit': '#', 'suite': '#', 'ste': '#'}
    for old, new in replacements.items():
        address = address.replace(old, new)
    street_replacements = {'street': 'st', 'avenue': 'ave', 'road': 'rd', 'drive': 'dr', 'place': 'pl', 'boulevard': 'blvd', 'court': 'ct', 'terrace': 'ter', 'circle': 'cir', 'lane': 'ln'}
    for old, new in street_replacements.items():
        address = re.sub(r'\b' + old + r'\b', new, address)
    address = re.sub(r'[^\w\s#]', '', address)
    address = re.sub(r'\s+', ' ', address).strip()
    return address.title()

def find_and_rebuild_date(text):
    """
    Finds the components of a date anywhere in a string, rebuilds it,
    and returns the clean date and the rest of the string.
    """
    date_pattern = re.compile(r'(\d{1,2})\s*[/|-]\s*(\d{1,2})\s*[/|-]\s*(\d{2,4})')
    
    match = date_pattern.search(text)
    
    if not match:
        return None, text

    month, day, year = match.groups()

    if len(year) == 2:
        year = f"20{year}"
    
    rebuilt_date = f"{int(month):02d}/{int(day):02d}/{year}"

    try:
        pd.to_datetime(rebuilt_date, format='%m/%d/%Y')
        original_messy_text = match.group(0)
        remaining_text = text.replace(original_messy_text, '')
        return rebuilt_date, remaining_text
    except (ValueError, TypeError):
        return None, text

def enhanced_table_extraction(page):
    """Extracts tables or lines of text from a page."""
    tables = []
    try:
        settings = {"vertical_strategy": "text", "horizontal_strategy": "text"}
        page_tables = page.extract_tables(table_settings=settings)
        if page_tables:
            for table in page_tables:
                if table and len(table) > 1:
                    tables.append(table)
    except Exception as e:
        logger.warning(f"Standard table extraction failed: {e}")
    if not tables:
        try:
            text = page.extract_text()
            if text:
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                if lines:
                    tables.append(lines)
        except Exception as e:
            logger.warning(f"Text-based extraction failed: {e}")
    return tables

def enhanced_process_and_split_rows(pdf_tables):
    """Main processing function using the new date logic."""
    global total_skipped_with_data, skipped_with_data
    all_cleaned_rows = []

    case_patterns = [r'(\d+-[A-Z]+-\d+(?:-[A-Z])?)', r'(\b\d{2,}-\d{2,3}\b)', r'(LTB-\d+-\d+)', r'(\d{4,5}-\d{2})', r'(\d+-ADM-\d+)']
    zip_pattern = re.compile(r'(20\d{3})')
    quad_pattern = re.compile(r'\b(NW|NE|SW|SE)\b')
    junk_patterns = [re.compile(r'case number|defendant address|eviction date|page \d+', re.IGNORECASE), re.compile(r'scheduled evictions|total|sum', re.IGNORECASE), re.compile(r'^[\s\-_=]+$')]

    for table in pdf_tables:
        rows_to_process = []
        if table and isinstance(table[0], list):
            for row in table:
                row_text = ' '.join([str(item) if item is not None else '' for item in row])
                rows_to_process.append(row_text)
        elif table:
            rows_to_process = table

        for row_str in rows_to_process:
            row_str = row_str.strip()
            if len(row_str) < 10 or any(p.search(row_str) for p in junk_patterns):
                continue
            
            row_str = clean_row_text(row_str)

            found_date, remaining_str = find_and_rebuild_date(row_str)

            if not found_date:
                total_skipped_with_data += 1
                skipped_with_data.append({'text': f"NO VALID DATE in: {row_str[:100]}"})
                continue
            
            cases = [item for pattern in case_patterns for item in re.findall(pattern, remaining_str)]
            zips = zip_pattern.findall(remaining_str)
            quads = quad_pattern.findall(remaining_str)

            found_case = cases[0] if cases else ''
            found_zip = zips[0] if zips else ''
            found_quad = quads[0] if quads else ''

            address = remaining_str
            if found_case: address = re.sub(r'^' + re.escape(found_case), '', address).strip()
            if found_zip: address = address.replace(found_zip, '')
            if found_quad: address = address.replace(found_quad, '')
            
            address = re.sub(r'\s+', ' ', address).strip(' ,.')

            all_cleaned_rows.append([found_case, address, found_quad, found_zip, found_date])

    return pd.DataFrame(all_cleaned_rows, columns=['Case Number', 'Defendant Address', 'Quad', 'Zipcode', 'Eviction Date'])

def clean_row_text(text):
    """Cleans text from a row."""
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[|]{2,}|_{3,}|-{3,}|={3,}', '', text)
    text = re.sub(r'\bnan\b', '', text, flags=re.IGNORECASE)
    return text.strip()

def extract_with_enhanced_hybrid_approach(pdf_path):
    """Main extraction logic."""
    tables = []
    ocr_pages = 0
    
    logger.info(f"Processing {pdf_path}")
    
    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        for i, page in enumerate(pdf.pages):
            page_tables = enhanced_table_extraction(page)
            if page_tables:
                tables.extend(page_tables)
                continue
            
            text = page.extract_text()
            if not text or len(text.strip()) < 50:
                ocr_pages += 1
                logger.info(f"Using OCR for page {i+1}")
                try:
                    ocr_text = pytesseract.image_to_string(page.to_image().original)
                    lines = [line.strip() for line in ocr_text.split('\n') if line.strip()]
                    if lines:
                        tables.append(lines)
                except Exception as e:
                    logger.error(f"OCR failed for page {i+1}: {e}")

    logger.info(f"Processed {total_pages} pages, {ocr_pages} with OCR, found {len(tables)} potential tables")
    return tables

# Main processing loop
for pdf_filename in os.listdir(pdf_directory):
    if not pdf_filename.endswith('.pdf'): 
        continue
    pdf_path = os.path.join(pdf_directory, pdf_filename)
    logger.info(f"Processing {pdf_filename}...")
    
    try:
        pdf_tables = extract_with_enhanced_hybrid_approach(pdf_path)
        if pdf_tables:
            cleaned_table = enhanced_process_and_split_rows(pdf_tables)
            if not cleaned_table.empty:
                cleaned_rows_tuples = [tuple(row) for row in cleaned_table.values]
                unique_rows.update(cleaned_rows_tuples)
                logger.info(f"Extracted {len(cleaned_table)} records from {pdf_filename}")
            else:
                logger.warning(f"No valid data rows processed from {pdf_filename}")
        else:
            logger.warning(f"No tables found in {pdf_filename}")
    except Exception as e:
        logger.error(f"Failed to process {pdf_filename}: {e}", exc_info=True)

# Create DataFrame from all newly parsed unique rows
final_df = pd.DataFrame(list(unique_rows), columns=['Case Number', 'Defendant Address', 'Quad', 'Zipcode', 'Eviction Date'])

# Load existing CSV to merge and deduplicate
csv_path = "eviction_notices.csv"
if os.path.exists(csv_path):
    try:
        existing_df = pd.read_csv(csv_path)
        combined_df = pd.concat([existing_df, final_df], ignore_index=True)
    except pd.errors.EmptyDataError:
        combined_df = final_df
else:
    combined_df = final_df

# <-- MODIFIED: Deduplication logic updated to use both Case Number and Eviction Date.
combined_df['Eviction Date'] = pd.to_datetime(combined_df['Eviction Date'], errors='coerce').dt.strftime('%m/%d/%Y')
combined_df['Case Number'] = combined_df['Case Number'].astype(str).str.strip().fillna('')

# Remove duplicates based on BOTH Case Number and Eviction Date
combined_df.drop_duplicates(subset=['Case Number', 'Eviction Date'], keep='first', inplace=True)

# For remaining rows with no case number, deduplicate by address and date
no_case_df = combined_df[combined_df['Case Number'] == ''].copy()
no_case_df['Normalized Address'] = no_case_df['Defendant Address'].apply(normalize_address)
no_case_df.drop_duplicates(subset=['Normalized Address', 'Eviction Date'], keep='first', inplace=True)
no_case_df.drop(columns=['Normalized Address'], inplace=True)

# Recombine the dataframes
final_df = pd.concat([combined_df[combined_df['Case Number'] != ''], no_case_df], ignore_index=True)


# Final formatting
final_df['City'] = 'Washington, DC'
final_df['Full Address'] = (
    final_df['Defendant Address'].astype(str) +
    final_df['Quad'].apply(lambda x: f', {x}' if pd.notna(x) and x != '' and x != 'nan' else '') +
    ', ' + final_df['City'] +
    final_df['Zipcode'].apply(lambda x: f', {x}' if pd.notna(x) and x != '' and x != 'nan' else '')
)

final_df.to_csv(csv_path, index=False)

def print_final_summary():
    global total_skipped_with_data, skipped_with_data
    logger.info("\n" + "="*60)
    logger.info("PROCESSING SUMMARY")
    logger.info("="*60)
    logger.info(f"Records successfully saved: {len(final_df):,}")
    logger.info(f"Rows skipped due to no valid date: {total_skipped_with_data:,}")
    if skipped_with_data:
        logger.info(f"\nSKIPPED ROWS SAMPLES ({len(skipped_with_data)} total):")
        logger.info("-" * 60)
        for i, row in enumerate(skipped_with_data[:20]):
            logger.info(f"{i+1:2d}. {row['text']}")
        if len(skipped_with_data) > 20:
            logger.info(f"... and {len(skipped_with_data) - 20} more rows")
    logger.info("\n" + "="*60)

print_final_summary()