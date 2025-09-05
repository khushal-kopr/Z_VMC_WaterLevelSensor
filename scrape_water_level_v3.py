# Version: 3.1.0
# Changes: Enhanced logging for better debugging
import time
import os
import logging
import sys
import pandas as pd
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Define your output directory
OUTPUT_DIR = "data"

# Log version information
logging.info("VMC Water Level Scraper v3.1.0")
logging.info("Scraping from: https://vmc.gov.in/waterlevelsensor/WaterLevel.aspx")

def create_session_with_retries():
    """Create a requests session with retry capabilities"""
    logging.info("Creating session with retry capabilities")
    session = requests.Session()
    
    # Define retry strategy
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    # Mount HTTP and HTTPS adapters with retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    logging.info("Session created successfully")
    return session

def save_debug_html(html_content, filename="debug_page.html"):
    """Save HTML content to a file for debugging"""
    try:
        debug_file = os.path.join(OUTPUT_DIR, filename)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        logging.info(f"Saved debug HTML to {debug_file}")
        return debug_file
    except Exception as e:
        logging.error(f"Failed to save debug HTML: {str(e)}")
        return None

def analyze_page_structure(soup):
    """Analyze and log the structure of the page"""
    logging.info("Analyzing page structure...")
    
    # Log page title
    title = soup.find('title')
    if title:
        logging.info(f"Page title: {title.text.strip()}")
    
    # Count and log all tables
    tables = soup.find_all('table')
    logging.info(f"Found {len(tables)} tables on the page")
    
    # Log details for each table
    for i, table in enumerate(tables):
        table_id = table.get('id', 'No ID')
        table_class = table.get('class', 'No class')
        rows = table.find_all('tr')
        logging.info(f"Table {i+1}: ID='{table_id}', Class={table_class}, Rows={len(rows)}")
        
        # Log first few rows of each table for debugging
        if i < 3:  # Only log details for first 3 tables
            for j, row in enumerate(rows[:3]):  # Only log first 3 rows
                cols = row.find_all(['th', 'td'])
                col_texts = [col.text.strip()[:30] for col in cols]  # Limit to 30 chars
                logging.info(f"  Row {j+1}: {len(cols)} columns - {col_texts}")
    
    return tables

def scrape_with_requests(max_retries=3):
    """
    Scrape water level data using requests library.
    
    Args:
        max_retries (int): Maximum number of retry attempts
    
    Returns:
        list: A list of dictionaries containing water level data
    """
    url = "https://vmc.gov.in/waterlevelsensor/WaterLevel.aspx"
    
    # Headers to mimic a browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    for attempt in range(max_retries + 1):
        try:
            logging.info(f"=== Attempt {attempt + 1}/{max_retries + 1} ===")
            logging.info(f"Sending request to {url}")
            session = create_session_with_retries()
            
            # Try with increasing timeouts
            for timeout in [60, 120, 180]:
                try:
                    logging.info(f"Trying with timeout: {timeout} seconds")
                    response = session.get(url, headers=headers, timeout=timeout)
                    logging.info(f"Response status code: {response.status_code}")
                    logging.info(f"Response headers: {dict(response.headers)}")
                    
                    if response.status_code == 200:
                        logging.info("Request successful")
                        break
                    else:
                        logging.warning(f"Received status code {response.status_code}")
                        continue
                except requests.exceptions.Timeout:
                    logging.warning(f"Request timed out with {timeout} seconds")
                    continue
                except Exception as e:
                    logging.error(f"Request error: {str(e)}")
                    continue
            else:
                logging.error("All attempts to fetch the page timed out")
                continue
            
            # Parse HTML
            logging.info("Parsing HTML content")
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Save debug HTML if not in GitHub Actions
            if os.environ.get('GITHUB_ACTIONS') != 'true':
                save_debug_html(response.text)
            
            # Analyze page structure
            tables = analyze_page_structure(soup)
            
            # Try to find the target table with different approaches
            table = None
            approach_used = None
            
            # Approach 1: Try to find table with id 'GridView1'
            logging.info("Approach 1: Looking for table with id 'GridView1'")
            table = soup.find('table', {'id': 'GridView1'})
            if table:
                approach_used = "id='GridView1'"
                logging.info(f"Found table using {approach_used}")
            else:
                # Approach 2: Find any table with similar attributes
                logging.info("Approach 1 failed. Trying Approach 2: Looking for table with class containing 'table'")
                table = soup.find('table', {'class': lambda x: x and 'table' in x.lower()})
                if table:
                    approach_used = "class containing 'table'"
                    logging.info(f"Found table using {approach_used}")
                else:
                    # Approach 3: Find all tables and look for one with water level data
                    logging.info("Approach 2 failed. Trying Approach 3: Looking for table containing 'water level' text")
                    for i, t in enumerate(tables):
                        if t.find(text=lambda x: x and 'water level' in x.lower()):
                            table = t
                            approach_used = f"table containing 'water level' text (table {i+1})"
                            logging.info(f"Found table using {approach_used}")
                            break
                    
                    if not table:
                        # Approach 4: Look for any table with multiple rows
                        logging.info("Approach 3 failed. Trying Approach 4: Looking for table with multiple rows")
                        for i, t in enumerate(tables):
                            rows = t.find_all('tr')
                            if len(rows) > 5:  # Arbitrary threshold for "enough" rows
                                table = t
                                approach_used = f"table with {len(rows)} rows (table {i+1})"
                                logging.info(f"Found table using {approach_used}")
                                break
            
            if not table:
                logging.error("Could not find any suitable table with water level data")
                continue
            
            # Log table details
            table_id = table.get('id', 'No ID')
            table_class = table.get('class', 'No class')
            rows = table.find_all('tr')
            logging.info(f"Selected table: ID='{table_id}', Class={table_class}, Total rows={len(rows)}")
            
            # Extract data
            data = []
            data_rows = []
            
            # Find rows with actual data (skip headers)
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:  # Only consider rows with at least 2 columns
                    data_rows.append(row)
            
            logging.info(f"Found {len(data_rows)} potential data rows")
            
            if not data_rows:
                logging.warning("No data rows found in the table")
                continue
            
            # Get current date/time for fallback
            current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Process each data row
            for i, row in enumerate(data_rows):
                try:
                    cols = row.find_all('td')
                    logging.debug(f"Processing row {i+1}: Found {len(cols)} columns")
                    
                    if len(cols) < 2:
                        logging.warning(f"Row {i+1} doesn't have enough columns, skipping")
                        continue
                    
                    # Extract location
                    location = cols[0].text.strip()
                    logging.debug(f"Row {i+1}: Location='{location}'")
                    
                    # Extract water level (it might be in a nested structure)
                    water_level = ""
                    if cols[1].find('table'):
                        water_level_table = cols[1].find('table')
                        water_level_td = water_level_table.find('td')
                        if water_level_td:
                            water_level = water_level_td.text.strip()
                            logging.debug(f"Row {i+1}: Found nested water level table")
                    else:
                        water_level = cols[1].text.strip()
                        logging.debug(f"Row {i+1}: Extracted water level directly")
                    
                    logging.debug(f"Row {i+1}: Water Level='{water_level}'")
                    
                    # Extract date and time
                    date_time = ""
                    
                    # Check if there's a third column
                    if len(cols) > 2:
                        if cols[2].find('table'):
                            date_time_table = cols[2].find('table')
                            date_time_td = date_time_table.find('td')
                            if date_time_td:
                                date_time = date_time_td.text.strip()
                                logging.debug(f"Row {i+1}: Found nested date/time table")
                        else:
                            date_time = cols[2].text.strip()
                            logging.debug(f"Row {i+1}: Extracted date/time directly")
                        
                        # Check if the extracted value looks like a date/time
                        if re.match(r'^[\d.]+$', date_time):
                            logging.info(f"Row {i+1}: Third column appears to be numeric, using current datetime")
                            date_time = current_datetime
                    else:
                        logging.debug(f"Row {i+1}: No third column, using current datetime")
                        date_time = current_datetime
                    
                    # If we still don't have a date_time value, use current date and time
                    if not date_time:
                        logging.debug(f"Row {i+1}: No date/time found, using current datetime")
                        date_time = current_datetime
                    
                    logging.debug(f"Row {i+1}: Date/Time='{date_time}'")
                    
                    data.append({
                        'Location': location,
                        'Water Level (Feet)': water_level,
                        'Date & Time': date_time
                    })
                    
                except Exception as e:
                    logging.error(f"Error processing row {i+1}: {str(e)}")
                    continue
            
            logging.info(f"Successfully extracted data for {len(data)} locations using {approach_used}")
            return data
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error: {str(e)}")
            if attempt < max_retries:
                logging.info(f"Retrying in 5 seconds...")
                time.sleep(5)
            continue
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            if attempt < max_retries:
                logging.info(f"Retrying in 5 seconds...")
                time.sleep(5)
            continue
    
    # If we get here, all retries failed
    logging.error(f"All {max_retries + 1} attempts failed")
    return None

def save_to_csv(data, filename=None):
    """
    Save water level data to a CSV file.
    
    Args:
        data (list): List of dictionaries containing water level data
        filename (str, optional): Name of the CSV file. If None, generates based on current date.
    
    Returns:
        str: Path to the created CSV file
    """
    if not data:
        logging.warning("No data to save")
        return None
    
    if filename is None:
        # Generate filename with current date
        today = datetime.now().strftime('%Y-%m-%d')
        filename = f"water_level_data_{today}.csv"
    
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Save to CSV
    file_path = os.path.join(OUTPUT_DIR, filename)
    try:
        df = pd.DataFrame(data)
        logging.info(f"DataFrame shape: {df.shape}")
        logging.info(f"DataFrame columns: {list(df.columns)}")
        logging.info(f"First few rows:\n{df.head()}")
        
        df.to_csv(file_path, index=False)
        logging.info(f"Data saved to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Error saving data to CSV: {str(e)}")
        return None

def main():
    """Main function to scrape and save water level data."""
    logging.info("=== Starting water level data scraping process ===")
    
    # Try requests method first
    data = scrape_with_requests()
    
    if not data:
        logging.error("All scraping methods failed, exiting")
        sys.exit(1)
    
    filename = save_to_csv(data)
    if not filename:
        logging.error("Failed to save data, exiting")
        sys.exit(1)
    
    logging.info("=== Process completed successfully ===")

if __name__ == "__main__":
    main()
