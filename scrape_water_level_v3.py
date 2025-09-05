# Version: 4.0.0
# Changes: Added multiple scraping methods with enhanced error handling
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
import json
from urllib.parse import urljoin

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Define your output directory
OUTPUT_DIR = "data"

# Log version information
logging.info("VMC Water Level Scraper v4.0.0")
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

def try_alternative_methods():
    """Try alternative methods to fetch the data"""
    logging.info("Trying alternative methods to fetch data")
    
    # Method 1: Try with different user agents
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    ]
    
    url = "https://vmc.gov.in/waterlevelsensor/WaterLevel.aspx"
    
    for i, user_agent in enumerate(user_agents):
        try:
            logging.info(f"Trying with user agent {i+1}/{len(user_agents)}")
            headers = {
                'User-Agent': user_agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Referer': 'https://vmc.gov.in/',
            }
            
            session = create_session_with_retries()
            response = session.get(url, headers=headers, timeout=60)
            
            if response.status_code == 200:
                logging.info(f"Success with user agent {i+1}")
                return response.text
            else:
                logging.warning(f"Failed with user agent {i+1}, status code: {response.status_code}")
                
        except Exception as e:
            logging.error(f"Error with user agent {i+1}: {str(e)}")
            continue
    
    # Method 2: Try using a proxy service (if available)
    logging.info("Trying with a public proxy service")
    try:
        # Using a public proxy API (replace with your preferred service)
        proxy_url = "https://api.allorigins.win/get?url=" + url
        response = requests.get(proxy_url, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            if 'contents' in data:
                logging.info("Success with proxy service")
                return data['contents']
    except Exception as e:
        logging.error(f"Error with proxy service: {str(e)}")
    
    # Method 3: Try to find an API endpoint
    logging.info("Looking for API endpoints")
    try:
        # Try common API endpoints
        api_endpoints = [
            "/api/WaterLevel",
            "/waterlevelsensor/api/GetWaterLevel",
            "/waterlevelsensor/WaterLevel.aspx/GetData",
        ]
        
        for endpoint in api_endpoints:
            try:
                api_url = urljoin(url, endpoint)
                logging.info(f"Trying API endpoint: {api_url}")
                
                headers = {
                    'Content-Type': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                }
                
                response = requests.get(api_url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        logging.info(f"Found API endpoint: {endpoint}")
                        return json.dumps(data)  # Return as JSON string
                    except:
                        # If not JSON, return as text
                        return response.text
            except Exception as e:
                logging.error(f"Error with API endpoint {endpoint}: {str(e)}")
                continue
    except Exception as e:
        logging.error(f"Error looking for API endpoints: {str(e)}")
    
    # Method 4: Try with mobile user agent
    logging.info("Trying with mobile user agent")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        
        session = create_session_with_retries()
        response = session.get(url, headers=headers, timeout=60)
        
        if response.status_code == 200:
            logging.info("Success with mobile user agent")
            return response.text
    except Exception as e:
        logging.error(f"Error with mobile user agent: {str(e)}")
    
    logging.error("All alternative methods failed")
    return None

def scrape_water_level_data(max_retries=3):
    """
    Scrape water level data using multiple methods.
    
    Args:
        max_retries (int): Maximum number of retry attempts
    
    Returns:
        list: A list of dictionaries containing water level data
    """
    url = "https://vmc.gov.in/waterlevelsensor/WaterLevel.aspx"
    
    for attempt in range(max_retries + 1):
        try:
            logging.info(f"=== Attempt {attempt + 1}/{max_retries + 1} ===")
            
            # Try alternative methods first
            html_content = try_alternative_methods()
            
            if not html_content:
                # If alternative methods fail, try the standard method
                logging.info("Alternative methods failed, trying standard method")
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
                
                session = create_session_with_retries()
                
                # Try with increasing timeouts
                for timeout in [60, 120, 180]:
                    try:
                        logging.info(f"Trying with timeout: {timeout} seconds")
                        response = session.get(url, headers=headers, timeout=timeout)
                        
                        if response.status_code == 200:
                            html_content = response.text
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
            logging.info("Parsing content")
            
            # Check if content is JSON
            try:
                data = json.loads(html_content)
                logging.info("Content is JSON, processing as API response")
                
                # Handle JSON response
                if isinstance(data, dict) and 'data' in data:
                    # Expected format: {"data": [{"Location": "...", "WaterLevel": "...", "DateTime": "..."}, ...]}
                    return data['data']
                elif isinstance(data, list):
                    # Expected format: [{"Location": "...", "WaterLevel": "...", "DateTime": "..."}, ...]
                    return data
                else:
                    logging.error("Unexpected JSON format")
                    continue
            except:
                # Not JSON, parse as HTML
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Save debug HTML if not in GitHub Actions
                if os.environ.get('GITHUB_ACTIONS') != 'true':
                    save_debug_html(html_content)
                
                # Try to find the table with different approaches
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
                        tables = soup.find_all('table')
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
                
                # Extract data from table
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
                            if cols[2].find('table'):
                                date_time_table = cols[2].find('table')
                                date_time_td = date_time_table.find('td')
                                if date_time_td:
                                    date_time = date_time_td.text.strip()
                            else:
                                date_time = cols[2].text.strip()
                            
                            # Check if the extracted value looks like a date/time
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
                        
                    except Exception as e:
                        logging.error(f"Error processing row {i+1}: {str(e)}")
                        continue
                
                logging.info(f"Successfully extracted data for {len(data)} locations using {approach_used}")
                return data
        
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            if attempt < max_retries:
                logging.info("Retrying in 10 seconds...")
                time.sleep(10)
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
    
    # Try scraping with multiple methods
    data = scrape_water_level_data()
    
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
