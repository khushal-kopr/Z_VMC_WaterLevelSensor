# https://chat.z.ai/c/023138cc-06e9-4ad7-9ebb-3137523d1227
# Version: 1.6.0
# Changes: Improved timeout handling, added retry mechanism, optimized page loading
import time
import os
import logging
import sys
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Define your output directory (relative path for GitHub Actions)
OUTPUT_DIR = "data"

def setup_driver():
    """Set up the Chrome WebDriver with headless options."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in background
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    # Set page load strategy to 'eager' to wait only for DOM to be loaded, not all resources
    chrome_options.page_load_strategy = 'eager'
    
    # Install and set up the driver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Set page load timeout
    driver.set_page_load_timeout(60)  # 60 seconds timeout
    
    return driver

def scrape_water_level_data(max_retries=2):
    """
    Scrape water level data from VMC website using Selenium.
    
    Args:
        max_retries (int): Maximum number of retry attempts
    
    Returns:
        list: A list of dictionaries containing water level data
    """
    url = "https://vmc.gov.in/WaterLevelSensor.aspx"
    
    for attempt in range(max_retries + 1):
        driver = None
        try:
            logging.info(f"Setting up WebDriver (Attempt {attempt + 1}/{max_retries + 1})")
            driver = setup_driver()
            
            logging.info(f"Accessing {url}")
            # Use a try-except block to handle timeout
            try:
                driver.get(url)
            except TimeoutException:
                logging.warning("Page load timed out, but continuing with current page state")
            
            # Wait for the page to load and for the table to be visible
            logging.info("Waiting for page to load...")
            time.sleep(10)  # Increased initial wait for page load
            
            # Switch to iframe if present
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            if iframes:
                logging.info(f"Found {len(iframes)} iframes. Switching to the first one.")
                driver.switch_to.frame(iframes[0])
            
            # Try to wait for the table to be present
            try:
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.ID, "GridView1"))
                )
                logging.info("Found table with id 'GridView1'")
            except Exception as e:
                logging.warning(f"Could not find table with id 'GridView1': {str(e)}")
            
            # Get the page source after JavaScript execution
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Save the HTML content for debugging (only in local environment)
            if os.environ.get('GITHUB_ACTIONS') != 'true':
                debug_file = os.path.join(OUTPUT_DIR, "debug_page.html")
                os.makedirs(OUTPUT_DIR, exist_ok=True)
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(page_source)
                logging.info(f"Saved page content to {debug_file} for debugging")
            
            # Try to find the table with different approaches
            table = None
            
            # Approach 1: Original method
            table = soup.find('table', {'id': 'GridView1'})
            if table:
                logging.info("Found table using original method (id='GridView1')")
            else:
                # Approach 2: Find any table with similar attributes
                logging.info("Original method failed, trying alternative approaches")
                table = soup.find('table', {'class': lambda x: x and 'grid' in x.lower()})
                if table:
                    logging.info("Found table using class containing 'grid'")
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
                if driver:
                    driver.quit()
                continue  # Try again
            
            # Debug: Log table structure
            logging.info("Analyzing table structure...")
            header_rows = table.find_all('tr')[:2]  # Check first 2 rows for headers
            for i, row in enumerate(header_rows):
                cols = row.find_all(['th', 'td'])
                logging.info(f"Header Row {i+1}: Found {len(cols)} columns")
                for j, col in enumerate(cols):
                    col_text = col.text.strip()[:50]  # Limit to 50 chars for readability
                    logging.info(f"  Column {j}: {col_text}")
            
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
                if driver:
                    driver.quit()
                continue  # Try again
            
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
            
            logging.info(f"Successfully extracted data for {len(data)} locations")
            if driver:
                driver.quit()
            return data
        
        except WebDriverException as e:
            logging.error(f"WebDriver error: {str(e)}")
            if driver:
                driver.quit()
            if attempt < max_retries:
                logging.info("Retrying...")
                time.sleep(5)  # Wait before retrying
            continue
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            if driver:
                driver.quit()
            if attempt < max_retries:
                logging.info("Retrying...")
                time.sleep(5)  # Wait before retrying
            continue
    
    # If we get here, all retries failed
    logging.error(f"All {max_retries + 1} attempts failed")
    return []

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
    logging.info("Starting water level data scraping process")
    
    data = scrape_water_level_data()
    if not data:
        logging.error("No data was scraped, exiting")
        sys.exit(1)
    
    filename = save_to_csv(data)
    if not filename:
        logging.error("Failed to save data, exiting")
        sys.exit(1)
    
    logging.info("Process completed successfully")

if __name__ == "__main__":
    main()
