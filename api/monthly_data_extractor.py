import time
import json
import requests
import os
from datetime import datetime, timedelta
import pandas as pd
import logging
from pathlib import Path

import warnings
from urllib3.exceptions import NotOpenSSLWarning

# Suppress only the specific warning
warnings.filterwarnings("ignore", category=NotOpenSSLWarning)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_extraction.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create data directories if they don't exist
DATA_DIR = Path('./data')
DATA_DIR.mkdir(exist_ok=True)

# Define the paths for combined datasets
PROPERTIES_FILE = DATA_DIR / 'properties_combined.json'
LATEST_AGGPRICES_FILE = DATA_DIR / 'agg_prices.csv'

def download_file(dataset_id, year=None, month=None, quarter=None):
    """
    Download data from data.gov.sg based on either month or quarter
    
    Args:
        dataset_id: The dataset ID to fetch
        year: The year (YYYY)
        month: The month (1-12), optional if quarter is provided
        quarter: The quarter (e.g., "Q2" or "2023-Q1"), optional if month is provided
        
    Returns:
        DataFrame with the data for the specified period
    """
    s = requests.Session()
    
    # Determine the filters based on provided parameters
    filters = []
    
    # Handle quarter parameter
    if quarter:
        # Check if quarter includes year (e.g., "2023-Q1")
        if isinstance(quarter, str) and "-Q" in quarter:
            logger.info(f"Downloading data for quarter {quarter} from dataset {dataset_id}")
            filters.append({"columnName": "quarter", "type": "EQ", "value": quarter})
        else:
            # If only quarter number/label provided, need year
            if not year:
                logger.error("Year must be provided when using quarter without year")
                return None
                
            quarter_str = f"{year}-Q{quarter}" if not quarter.startswith("Q") else f"{year}-{quarter}"
            logger.info(f"Downloading data for quarter {quarter_str} from dataset {dataset_id}")
            filters.append({"columnName": "quarter", "type": "EQ", "value": quarter_str})
    
    # Handle month parameter
    if month and year:
        try:
            # Handle month as string or int
            month_int = int(month)
            month_str = f"{year}-{month_int:02d}"
            logger.info(f"Downloading data for month {month_str} from dataset {dataset_id}")
            filters.append({"columnName": "month", "type": "EQ", "value": month_str})
            
            # Add year filter
            filters.append({"columnName": "year_completed", "type": "EQ", "value": str(year)})
        except (ValueError, TypeError):
            logger.error(f"Invalid month format: {month}. Must be a number between 1-12.")
            return None
    elif year and not quarter:
        # If only year provided (no month or quarter)
        logger.info(f"Downloading data for year {year} from dataset {dataset_id}")
        filters.append({"columnName": "year_completed", "type": "EQ", "value": str(year)})
    
    # Ensure we have at least one filter
    if not filters:
        logger.error("No valid time period specified (need year, month, or quarter)")
        return None
    
    # Construct query parameters
    query_params = {"filters": filters}

    try:
        # Initiate download with query parameters
        initiate_download_response = s.get(
            f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/initiate-download",
            headers={"Content-Type": "application/json"},
            json=query_params
        )
        
        response_data = initiate_download_response.json()
        if 'data' not in response_data or 'message' not in response_data['data']:
            logger.error(f"Unexpected response format: {response_data}")
            return None
            
        logger.info(response_data['data']['message'])

        # Poll download
        MAX_POLLS = 5
        for i in range(MAX_POLLS):
            poll_download_response = s.get(
                f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/poll-download",
                headers={"Content-Type": "application/json"},
                json=query_params
            )
            
            poll_data = poll_download_response.json()
            
            if "data" in poll_data and "url" in poll_data['data']:
                download_url = poll_data['data']['url']
                
                # Download and load the CSV
                df = pd.read_csv(download_url)
                logger.info(f"Downloaded {len(df)} rows of data")
                
                # Apply filters based on the input parameters
                if month and year and 'month' in df.columns:
                    df['month'] = pd.to_datetime(df['month'])
                    target_month = pd.to_datetime(f"{year}-{int(month):02d}-01")
                    month_end = (target_month + pd.offsets.MonthEnd(0)).date()
                    
                    # Filter to include only the target month
                    df = df[(df['month'] >= target_month) & 
                            (df['month'] <= pd.Timestamp(month_end))]
                    
                    logger.info(f"Filtered to {len(df)} rows for month {target_month.strftime('%Y-%m')}")
                
                # Add quarter information if not present
                if quarter and 'quarter' not in df.columns:
                    if isinstance(quarter, str) and "-Q" in quarter:
                        df['quarter'] = quarter
                    else:
                        quarter_str = f"{year}-Q{quarter}" if not str(quarter).startswith("Q") else f"{year}-{quarter}"
                        df['quarter'] = quarter_str
                
                return df
                
            if i == MAX_POLLS - 1:
                logger.error(f"{i+1}/{MAX_POLLS}: No result found, possible error with dataset")
            else:
                logger.info(f"{i+1}/{MAX_POLLS}: No result yet, continuing to poll")
            
            time.sleep(3)
        
    except Exception as e:
        logger.error(f"Error downloading data: {e}")
    
    return None

def tagging(d):
    if d['residential'] == "Y":
        return "Residential"
    elif d['commercial'] == "Y":
        return "Commercial"
    elif d['market_hawker'] == "Y":
        return "Market and hawker"
    elif d['miscellaneous'] == "Y":
        return "Miscellaneous"
    elif d['multistorey_carpark'] == "Y":
        return "Multi-storey carpark"
    elif d['precinct_pavilion'] == "Y":
        return "Miscellaneous"
    return None  # Return None if no conditions are met

def enrich_properties_data(df):
  blocks_coordinates = {}

  #-- counter
  ids = 0

  #-- for each block
  for i, j in df.iterrows():
      ids += 1
      #-- by default there is no location
      location = None
      #-- count how many features that are buildings are returned
      h = 0
      #-- construct the address for the geocoding API
      address = str(j['blk_no']) + ' ' + str(j['street'])
      #print(i, '\t', str(j['blk_no']) + ' ' + str(j['street']))
      if i in blocks_coordinates:
          print("\tAlready fetched, skipping")
          continue

      #-- geocoder query
      for attempt in range(10):
          #-- max. 250 requests per second are allowed, so let's pause every 10ms not to exceed 100 requests per second
          time.sleep(0.010)
          try:
              #-- fetch the location of the block
              #-- no authentication isneeded for this functionality of the API
              url = "https://www.onemap.gov.sg/api/common/elastic/search?searchVal=" + address + "&returnGeom=Y&getAddrDetails=Y&pageNum=1"
              headers = {"Authorization": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZTMxZDdlMmQ1ZGNkNzdiMGFiY2NlZWE1ZGNmMGE2NiIsImlzcyI6Imh0dHA6Ly9pbnRlcm5hbC1hbGItb20tcHJkZXppdC1pdC1uZXctMTYzMzc5OTU0Mi5hcC1zb3V0aGVhc3QtMS5lbGIuYW1hem9uYXdzLmNvbS9hcGkvdjIvdXNlci9wYXNzd29yZCIsImlhdCI6MTcyOTc0MzE1OCwiZXhwIjoxNzMwMDAyMzU4LCJuYmYiOjE3Mjk3NDMxNTgsImp0aSI6ImlValBjdU9KOWxkeTR6blIiLCJ1c2VyX2lkIjo0OTkwLCJmb3JldmVyIjpmYWxzZX0.sTI0T7yPameI7an_nJFCSi8yoygK20oEUIlrVEXK7Hw"}
              response = requests.request("GET", url, headers=headers)
              location = response.json()
              #-- thank you OneMap
          except:
              continue
          else:
              break
      else:
          print('10 attempts failed')

      if location:
          print('\tThere are', location['found'], 'result(s)')
          if location['found'] == 0:
              #-- if nothing is found (rarely happens)
              blocks_coordinates[i] = None
              continue
          #-- we are feeling lucky so we will just take the first result into consideration
          l = location['results'][0]
          #-- save the information
          blocks_coordinates[i] = j.to_dict()
          blocks_coordinates[i].update({
            'address': l['ADDRESS'],
            'latitude': l['LATITUDE'],
            'longitude': l['LONGITUDE'],
            'building': l['BUILDING']
          })
      else:
          print('Address not found')
          blocks_coordinates[i] = None

  legend = { 
    "AMK": "ANG MO KIO",
    "BB": "BUKIT BATOK",
    "BD": "BEDOK",
    "BH": "BISHAN",
    "BM": "BUKIT MERAH",
    "BP": "BUKIT PANJANG",
    "BT": "BUKIT TIMAH",
    "CCK": "CHOA CHU KANG",
    "CL": "CLEMENTI",
    "CT": "CENTRAL AREA",
    "GL": "GEYLANG",
    "HG": "HOUGANG",
    "JE": "JURONG EAST",
    "JW": "JURONG WEST",
    "KWN": "KALLANG",
    "MP": "MARINE PARADE",
    "PG": "PUNGGOL",
    "PRC": "PASIR RIS",
    "QT": "QUEENSTOWN",
    "SB": "SEMBAWANG",
    "SGN": "SERANGOON",
    "SK": "SENGKANG",
    "TAP": "TAMPINES",
    "TG": "TENGAH",
    "TP": "TOA PAYOH",
    "WL": "WOODLANDS",
    "YS": "YISHUN" 
  }

  data = []
  for key, d in blocks_coordinates.items():
    if d:
        data.append({
            'tag': tagging(d),
            'lat': float(d['latitude']),
            'lon': float(d['longitude']),
            'town': legend[d['bldg_contract_town']],  # You need to define `legend`
            'address': d['address'],
            'street': d['street'],
            'total_units': d.get('total_dwelling_units', 0),
            'year': d['year_completed'],
            'max_floor_lvl': d['max_floor_lvl']
        })

  return pd.DataFrame(data)

def update_latest_properties_dataset(year, month):
    """
    Update the properties dataset by combining existing data with new monthly data
    
    Args:
        year: The year to extract
        
    Returns:
        Boolean indicating success
    """
    dataset_id = "d_17f5382f26140b1fdae0ba2ef6239d2f"
    
    try:
        # First, check if there's an existing properties file
        existing_properties_df = None
        if PROPERTIES_FILE.exists():
            logger.info(f"Reading existing properties data from {PROPERTIES_FILE}")
            # Read JSON file instead of CSV
            with open(PROPERTIES_FILE, 'r') as f:
                existing_properties = json.load(f)
            
            # Convert JSON to DataFrame
            existing_properties_df = pd.DataFrame(existing_properties)
            logger.info(f"Loaded {len(existing_properties_df)} existing property records")
        
        # Download the new month's data
        logger.info(f"Downloading properties data for {year}")
        new_properties_df = download_file(dataset_id, year)
        
        if new_properties_df is None or len(new_properties_df) == 0:
            logger.warning(f"No new properties data found for {year}")
            return False
            
        logger.info(f"Downloaded {len(new_properties_df)} new property records")
        
        # Enrich the new data
        new_properties_df = enrich_properties_data(new_properties_df)
        
        # Combine with existing data if available
        if existing_properties_df is not None and len(existing_properties_df) > 0:
            # Check if existing data has 'address' column
            if 'address' in existing_properties_df.columns:
                # Create 'address' in new data if it doesn't exist but has the component columns
                if 'address' not in new_properties_df.columns and all(col in new_properties_df.columns for col in ['blk_no', 'street']):
                    # Construct address from components
                    new_properties_df['address'] = new_properties_df['blk_no'].astype(str) + ' ' + new_properties_df['street']
                    logger.info("Created 'address' column in new data by combining 'blk_no' and 'street'")
                
                if 'address' in new_properties_df.columns:
                    # Now we can compare addresses
                    existing_addresses = set(existing_properties_df['address'])
                    new_addresses = set(new_properties_df['address'])
                    duplicate_addresses = existing_addresses.intersection(new_addresses)
                    
                    if duplicate_addresses:
                        logger.info(f"Found {len(duplicate_addresses)} properties with matching addresses")
                        existing_properties_df = existing_properties_df[~existing_properties_df['address'].isin(duplicate_addresses)]
                        logger.info(f"Removed duplicates from existing data, {len(existing_properties_df)} records remain")
                else:
                    logger.warning("Could not create 'address' column in new data - unable to check for duplicates")
            
            # Combine the datasets
            combined_df = pd.concat([existing_properties_df, new_properties_df], ignore_index=True)
            logger.info(f"Combined dataset has {len(combined_df)} property records")
        else:
            # If no existing data, just use the new data
            if 'address' not in new_properties_df.columns and all(col in new_properties_df.columns for col in ['blk_no', 'street']):
                # Still create the address column for consistency
                new_properties_df['address'] = new_properties_df['blk_no'].astype(str) + ' ' + new_properties_df['street']
                logger.info("Created 'address' column in new data")
            
            combined_df = new_properties_df
        
        # Save the combined dataset as JSON
        with open(DATA_DIR / 'properties_combined_new.json', 'w') as f:
            json.dump(new_properties_df.to_dict(orient='records'), f)
            
        # Save the main properties file as JSON
        with open(PROPERTIES_FILE, 'w') as f:
            json.dump(combined_df.to_dict(orient='records'), f)
            
        logger.info(f"Saved {len(combined_df)} property records to {PROPERTIES_FILE}")
        
        # Save metadata
        metadata = {
            'last_updated': datetime.now().isoformat(),
            'last_month_added': f"{year}-{month:02d}",
            'record_count': len(combined_df)
        }
        
        with open(DATA_DIR / 'properties_metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
        
        return True
        
    except Exception as e:
        logger.error(f"Error updating properties dataset: {e}")
        return False

def update_latest_aggprices_dataset(year, month):
    """
    Update the latest aggregated prices dataset with new monthly data
    
    Args:
        year: The year to extract
        month: The month to extract
        
    Returns:
        Boolean indicating success
    """
    dataset_id = "d_b51323a474ba789fb4cc3db58a3116d4"
    
    if not dataset_id:
        logger.error(f"No dataset ID found for year {year}")
        return False

    # Calculate quarter (1-4)
    quarter = (month - 1) // 3 + 1
    quarter_str = f"Q{quarter}"

    try:
        # Download the data for this month
        quarter_df = download_file(dataset_id, year=year, quarter=quarter_str)
        
        if quarter_df is None or len(quarter_df) == 0:
            logger.warning(f"No data found for {year}-{quarter_str}")
            return False
        
        quarter_df['flat_type'] = quarter_df['flat_type'].str.replace('-', ' ').str.upper()
        quarter_df['price'] = quarter_df['price'].fillna('-')
        quarter_df['town'] = quarter_df['town'].str.upper()
        quarter_df['town'] = quarter_df['town'].apply(lambda x: 'CENTRAL AREA' if x == 'CENTRAL' else x)

        # Load existing dataset if it exists
        if LATEST_AGGPRICES_FILE.exists():
            existing_df = pd.read_csv(LATEST_AGGPRICES_FILE)
            
            logger.info(f"Loaded existing dataset with {len(existing_df)} records")
            
            # Remove any existing data for this month to avoid duplicates
            if 'quarter' in existing_df.columns:
                existing_df = existing_df[existing_df['quarter'] != quarter_str]
                
                logger.info(f"Removed existing data for {year}-{quarter_str}, {len(existing_df)} records remain")
            
            # Combine with new data
            combined_df = pd.concat([existing_df, quarter_df], ignore_index=True)
            logger.info(f"Combined dataset now has {len(combined_df)} records")
        else:
            # If no existing file, just use the new data
            combined_df = quarter_df
            logger.info(f"Created new dataset with {len(combined_df)} records")
        
        # Save the combined dataset
        combined_df.to_csv(LATEST_AGGPRICES_FILE, index=False)
        logger.info(f"Saved updated dataset to {LATEST_AGGPRICES_FILE}")
        
        # Save metadata
        metadata = {
            'last_updated': datetime.now().isoformat(),
            'last_month_added': f"{year}-{quarter_str}",
            'record_count': len(combined_df)
        }
        
        with open(DATA_DIR / 'agg_prices_latest_metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
        
        return True
        
    except Exception as e:
        logger.error(f"Error updating latest prices dataset: {e}")
        return False
 
def run(year=None, month=None):
    """
    Extract data for a specific month and update the combined datasets
    
    Args:
        year: The year to extract (defaults to current year)
        month: The month to extract (defaults to current month)
    """
    if year is None or month is None:
        today = datetime.now()
        first_of_month = datetime(today.year, today.month, 1)

        year = year or first_of_month.year
        month = month or first_of_month.month
    
    logger.info(f"Extracting data for {year}-{month:02d}")
    
    # 1. Update properties dataset (replace with the latest full dataset)
    properties_updated = update_latest_properties_dataset(year, month)
    logger.info(f"Properties dataset updated: {properties_updated}")
    
    # 2. Update aggregated prices dataset (append new month data to the latest segment)
    agg_prices_updated = update_latest_aggprices_dataset(year, month)
    logger.info(f"Latest prices dataset updated for {year}-{month:02d}: {agg_prices_updated}")

    return True

def extract_multiple_months(start_year, start_month, end_year, end_month):
    """
    Extract data for multiple months in a loop
    
    Args:
        start_year: Starting year (YYYY)
        start_month: Starting month (1-12)
        end_year: Ending year (YYYY)
        end_month: Ending month (1-12)
    """
    from datetime import datetime
    
    # Create start and end dates
    start_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 1)
    
    # Ensure start is before end
    if start_date > end_date:
        raise ValueError("Start date must be before end date")
    
    current = start_date
    while current <= end_date:
        year = current.year
        month = current.month
        
        print(f"Extracting data for {year}-{month:02d}")
        run(year, month)
        
        # Move to next month
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
        current = datetime(year, month, 1)
        
    print(f"Completed extracting data from {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
   
if __name__ == "__main__":    
    run()

        