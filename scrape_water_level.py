import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import logging
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def scrape_water_level_data():
    """
    Scrape water level data from VMC website.
    
    Returns:
        list: A list of dictionaries containing water level data
    """
    url = "https://vmc.gov.in/WaterLevelSensor.aspx"
    
    try:
        logging.info(f"Sending request to {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        logging.info("Parsing HTML content")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the table
        table = soup.find('table', {'id': 'GridView1'})
        if not table:
            logging.error("Could not find the target table with id 'GridView1'")
            return []
        
        # Extract data
        data = []
        rows = table.find_all('tr')[2:]  # Skip header rows
        
        if not rows:
            logging.warning("No data rows found in the table")
            return []
        
        logging.info(f"Found {len(rows)} data rows")
        
        for i, row in enumerate(rows):
            try:
                cols = row.find_all('td')
                
                if len(cols) < 3:
                    logging.warning(f"Row {i+1} doesn't have enough columns, skipping")
                    continue
                
                # Extract location
                location = cols[0].text.strip()
                
                # Extract water level (it's nested in another table)
                water_level_table = cols[1].find('table')
                if not water_level_table:
                    logging.warning(f"Could not find nested table in row {i+1}, skipping")
                    continue
                    
                water_level_td = water_level_table.find('td')
                if not water_level_td:
                    logging.warning(f"Could not find water level value in row {i+1}, skipping")
                    continue
                    
                water_level = water_level_td.text.strip()
                
                # Extract date and time
                date_time = cols[2].text.strip()
                
                data.append({
                    'Location': location,
                    'Water Level (Feet)': water_level,
                    'Date & Time': date_time
                })
            except Exception as e:
                logging.error(f"Error processing row {i+1}: {str(e)}")
                continue
        
        logging.info(f"Successfully extracted data for {len(data)} locations")
        return data
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {str(e)}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
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
    
    # Create directory if it doesn't exist
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)
    
    # Save to CSV
    file_path = os.path.join(data_dir, filename)
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
