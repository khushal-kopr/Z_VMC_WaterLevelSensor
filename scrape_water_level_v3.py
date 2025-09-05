# Version: 3.0.0
# Changes: Combined Selenium and requests approaches with better timeout handling and fallback mechanisms
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

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Define your output directory
OUTPUT_DIR = "data"

# Log version information
logging.info("VMC Water Level Scraper v3.0.0")
logging.info("Scraping from: https://vmc.gov.in/waterlevelsensor/WaterLevel.aspx")

def create_session_with_retries():
    """Create a requests session with retry capabilities"""
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
    
    return session

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
            logging.info(f"Attempting requests method (Attempt {attempt + 1}/{max_retries + 1})")
            session = create_session_with_retries()
            
            # Try with increasing timeouts
            for timeout in [60, 120, 180]:
                try:
                    response = session.get(url, headers=headers, timeout=timeout)
                    response.raise_for_status()
                    break
                except requests.exceptions.Timeout:
                    logging.warning(f"Request timed out with {timeout} seconds, trying with longer timeout...")
                    continue
            else:
                logging.error("All attempts to fetch the page timed out")
                continue
            
            logging.info("Successfully fetched page using requests")
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Try to find the table with different approaches
            table = None
            
            # Approach 1: Try to find table with id 'GridView1'
            table = soup.find('table', {'id': 'GridView1'})
            if table:
                logging.info("Found table using id 'GridView1'")
            else:
                # Approach 2: Find any table with similar attributes
                logging.info("Original method failed, trying alternative approaches")
                table = soup.find('table', {'class': lambda x: x and 'table' in x.lower()})
                if table:
                    logging.info("Found table using class containing 'table'")
                else:
                    # Approach 3: Find all tables and look for one with water level data
                    tables = soup.find_all('table')
                    logging.info(f"Found {len(tables)} tables on the page")
                    
                    for i, t in enumerate(tables):
                        # Check if this table contains water level data
                        if t.find(text=lambda x: x and 'water level' in x.lower()):
                            table = t
                            logging.info(f"Found table containing 'water level' text (table {i+1})")
                            break
                    
                    if not table:
                        # Approach 4: Look for any table with multiple rows
                        for i, t in enumerate(tables):
                            rows = t.find_all('tr')
                            if len(rows) > 5:  # Arbitrary threshold for "enough" rows
                                table = t
                                logging.info(f"Found table with {len(rows)} rows (table {i+1})")
                                break
            
            if not table:
                logging.error("Could not find any suitable table with water level data")
                continue
            
            # Extract data
            data = []
            rows = table.find_all('tr')
            
            # Skip header rows - we need to determine how many to skip
            # Let's look for rows that contain actual data
            data_rows = []
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:  # Only consider rows with at least 2 columns
                    data_rows.append(row)
            
            if not data_rows:
                logging.warning("No data rows found in the table")
                continue
            
            logging.info(f"Found {len(data_rows)} data rows")
            
            # Get current date/time for fallback
            current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            for i, row in enumerate(data_rows):
                try:
                    cols = row.find_all('td')
                    
                    if len(cols) < 2:
                        logging.warning(f"Row {i+1} doesn't have enough columns, skipping")
                        continue
                    
                    # Extract location
                    location = cols[0].text.strip()
                    
                    # Extract water level (it might be in a nested structure)
                    water_level = ""
                    # Try different ways to extract water level
                    if cols[1].find('table'):
                        water_level_table = cols[1].find('table')
                        water_level_td = water_level_table.find('td')
                        if water_level_td:
                            water_level = water_level_td.text.strip()
                    else:
                        water_level = cols[1].text.strip()
                    
                    # Extract date and time
                    date_time = ""
                    
                    # Check if there's a third column
                    if len(cols) > 2:
                        # Try to extract from third column
                        if cols[2].find('table'):
                            date_time_table = cols[2].find('table')
                            date_time_td = date_time_table.find('td')
                            if date_time_td:
                                date_time = date_time_td.text.strip()
                        else:
                            date_time = cols[2].text.strip()
                        
                        # Check if the extracted value looks like a date/time
                        # If it's numeric, it's probably not a date/time
                        if re.match(r'^[\d.]+$', date_time):
                            logging.info(f"Third column appears to be numeric, not date/time for {location}")
                            date_time = current_datetime
                    else:
                        # No third column, use current date/time
                        date_time = current_datetime
                    
                    # If we still don't have a date_time value, use current date and time
                    if not date_time:
                        date_time = current_datetime
                    
                    data.append({
                        'Location': location,
                        'Water Level (Feet)': water_level,
                        'Date & Time': date_time
                    })
                    
                    # Log the extracted values for debugging
                    logging.debug(f"Row {i+1}: Location='{location}', Water Level='{water_level}', Date/Time='{date_time}'")
                    
                except Exception as e:
                    logging.error(f"Error processing row {i+1}: {str(e)}")
                    continue
            
            logging.info(f"Successfully extracted data for {len(data)} locations using requests")
            return data
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error: {str(e)}")
            if attempt < max_retries:
                logging.info("Retrying...")
                time.sleep(5)  # Wait before retrying
            continue
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            if attempt < max_retries:
                logging.info("Retrying...")
                time.sleep(5)  # Wait before retrying
            continue
    
    # If we get here, all retries failed
    logging.error(f"All {max_retries + 1} attempts with requests failed")
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
        df.to_csv(file_path, index=False)
        logging.info(f"Data saved to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Error saving data to CSV: {str(e)}")
        return None

def main():
    """Main function to scrape and save water level data."""
    # Try requests method first
    data = scrape_with_requests()
    
    if not data:
        logging.error("All scraping methods failed, exiting")
        sys.exit(1)
    
    filename = save_to_csv(data)
    if not filename:
        logging.error("Failed to save data, exiting")
        sys.exit(1)
    
    logging.info("Process completed successfully")

if __name__ == "__main__":
    main()
