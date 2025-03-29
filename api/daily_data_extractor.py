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
LATEST_PRICES_FILE = DATA_DIR / 'prices_2017_onwards.csv'  # Prices from 2017 onwards
PRICES_SEGMENTS = {
    '1990-1999': DATA_DIR / 'prices_1990_1999.csv',
    '2000-2012': DATA_DIR / 'prices_2000_2012.csv',
    '2012-2014': DATA_DIR / 'prices_2012_2014.csv',
    '2015-2016': DATA_DIR / 'prices_2015_2016.csv',
    '2017-2025': LATEST_PRICES_FILE
}

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
        except (ValueError, TypeError):
            logger.error(f"Invalid month format: {month}. Must be a number between 1-12.")
            return None
    
    # Ensure we have at least one filter
    if not filters:
        logger.error("No valid time period specified (need year, month, or quarter)")
        return None
    
    # Construct query parameters
    query_params = {"filters": filters}
    print(query_params)
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
                json={}  # No filters needed for polling
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

def initialize_price_segments():
    """
    Initialize the price segment files if they don't exist.
    This should be run once to set up the historical data segments.
    """
    for period, file_path in PRICES_SEGMENTS.items():
        if not file_path.exists():
            logger.info(f"Initializing price segment for {period}")
            
            # Extract year range
            years = period.split('-')
            start_year = int(years[0])
            end_year = int(years[1])
            
            # Get appropriate dataset ID
            dataset_id = get_dataset_id_for_period(start_year)
            
            if not dataset_id:
                logger.error(f"No dataset ID found for period {period}")
                continue
            
            try:
                # Download full dataset for this period
                s = requests.Session()
                
                # initiate download
                initiate_download_response = s.get(
                    f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/initiate-download",
                    headers={"Content-Type": "application/json"},
                    json={}
                )
                
                logger.info(f"Initiated download for {period} (dataset ID: {dataset_id})")
                
                # poll download
                MAX_POLLS = 5
                for i in range(MAX_POLLS):
                    poll_download_response = s.get(
                        f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/poll-download",
                        headers={"Content-Type": "application/json"},
                        json={}
                    )
                    
                    poll_data = poll_download_response.json()
                    
                    if "data" in poll_data and "url" in poll_data['data']:
                        download_url = poll_data['data']['url']
                        logger.info(f"Download URL for {period}: {download_url}")
                        
                        # Download and load the CSV
                        df = pd.read_csv(download_url)
                        
                        # Convert date columns if needed
                        if 'month' in df.columns:
                            df['month'] = pd.to_datetime(df['month'])
                        
                        # Filter to the specific year range
                        if 'month' in df.columns:
                            df = df[
                                (df['month'].dt.year >= start_year) & 
                                (df['month'].dt.year <= end_year)
                            ]
                        
                        # Enrich the data
                        df = enrich_prices_data(df)
                        
                        # Save to CSV
                        df.to_csv(file_path, index=False)
                        logger.info(f"Saved {len(df)} records for {period} to {file_path}")
                        
                        # Save metadata
                        metadata = {
                            'period': period,
                            'created_date': datetime.now().isoformat(),
                            'record_count': len(df)
                        }
                        
                        metadata_path = DATA_DIR / f"prices_{start_year}_{end_year}_metadata.json"
                        with open(metadata_path, 'w') as f:
                            json.dump(metadata, f, indent=2)
                        
                        break
                        
                    if i == MAX_POLLS - 1:
                        logger.error(f"{i+1}/{MAX_POLLS}: No result found for {period}")
                    else:
                        logger.info(f"{i+1}/{MAX_POLLS}: No result yet for {period}, continuing to poll")
                    
                    time.sleep(3)
                
            except Exception as e:
                logger.error(f"Error initializing price segment for {period}: {e}")

def get_dataset_id_for_period(year):
    """
    Get the appropriate dataset ID based on the year
    """
    if 1990 <= year <= 1999:
        return "d_ebc5ab87086db484f88045b47411ebc5"
    elif 2000 <= year <= 2012:
        return "d_43f493c6c50d54243cc1eab0df142d6a"
    elif 2012 <= year <= 2014:
        return "d_2d5ff9ea31397b66239f245f57751537"
    elif 2015 <= year <= 2016:
        return "d_ea9ed51da2787afaf8e51f827c304208"
    elif year >= 2017:
        return "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
    else:
        return None

def enrich_prices_data(df):
    """
    Perform any enrichment operations on the prices data
    
    Args:
        df: DataFrame with the prices data
        
    Returns:
        Enriched DataFrame
    """
    if df is None or len(df) == 0:
        return df
        
    # Example enrichment operations:
    # 1. Calculate price per square meter
    if 'resale_price' in df.columns and 'floor_area_sqm' in df.columns:
        df['price_per_sqm'] = df['resale_price'] / df['floor_area_sqm']
    
    # # 2. Add month and year columns for easier filtering
    # if 'month' in df.columns:
    #     df['year'] = df['month'].dt.year
    #     df['month_num'] = df['month'].dt.month
    
    #3 Relabel columns
    df.rename(columns={'month': 'date'}, inplace=True)
    df.rename(columns={'resale_price': 'price'}, inplace=True)
    
    return df

def update_latest_prices_dataset(year, month):
    """
    Update the latest prices dataset (2017 onwards) with new monthly data
    
    Args:
        year: The year to extract
        month: The month to extract
        
    Returns:
        Boolean indicating success
    """
    # Only applicable for data from 2017 onwards
    if year < 2017:
        logger.warning(f"Year {year} is before 2017, not updating latest prices dataset")
        return False
    
    dataset_id = get_dataset_id_for_period(year)
    
    if not dataset_id:
        logger.error(f"No dataset ID found for year {year}")
        return False
    
    try:
        # Download the data for this month
        month_df = download_file(dataset_id, year=year, month=month)
        
        if month_df is None or len(month_df) == 0:
            logger.warning(f"No data found for {year}-{month:02d}")
            return False
        
        # Enrich the data
        month_df = enrich_prices_data(month_df)
        
        # Load existing dataset if it exists
        if LATEST_PRICES_FILE.exists():
            existing_df = pd.read_csv(LATEST_PRICES_FILE)
            
            # Convert date columns to datetime if needed
            if 'date' in existing_df.columns and not pd.api.types.is_datetime64_dtype(existing_df['date']):
                existing_df['date'] = pd.to_datetime(existing_df['date'])
            
            logger.info(f"Loaded existing dataset with {len(existing_df)} records")
            
            # Create a date range for the month we're adding
            target_month = pd.to_datetime(f"{year}-{month:02d}-01")
            month_end = (target_month + pd.offsets.MonthEnd(0)).date()
            
            # Remove any existing data for this month to avoid duplicates
            if 'date' in existing_df.columns:
                existing_df = existing_df[
                    ~((existing_df['date'] >= target_month) & 
                      (existing_df['date'] <= pd.Timestamp(month_end)))
                ]
                
                logger.info(f"Removed existing data for {year}-{month:02d}, {len(existing_df)} records remain")
            
            # Combine with new data
            combined_df = pd.concat([existing_df, month_df], ignore_index=True)

            # Convert datetime column to YYYY-MM-DD strings
            combined_df['date'] = combined_df['date'].dt.strftime('%Y-%m-%d')

            logger.info(f"Combined dataset now has {len(combined_df)} records")
        else:
            # If no existing file, just use the new data
            combined_df = month_df
            logger.info(f"Created new dataset with {len(combined_df)} records")
        
        # Save the combined dataset
        combined_df.to_csv(LATEST_PRICES_FILE, index=False)
        logger.info(f"Saved updated dataset to {LATEST_PRICES_FILE}")
        
        # Save metadata
        metadata = {
            'last_updated': datetime.now().isoformat(),
            'last_month_added': f"{year}-{month:02d}",
            'record_count': len(combined_df)
        }
        
        with open(DATA_DIR / 'prices_latest_metadata.json', 'w') as f:
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
        month: The month to extract (defaults to previous month)
    """
    # Default to previous month if not specified
    if year is None or month is None:
        today = datetime.now()
      
        first_of_month = datetime(today.year, today.month, 1)

        year = year or first_of_month.year
        month = month or first_of_month.month
    
    logger.info(f"Extracting data for {year}-{month:02d}")
    
    prices_updated = update_latest_prices_dataset(year, month)
    logger.info(f"Latest prices dataset updated for {year}-{month:02d}: {prices_updated}")

    return True

if __name__ == "__main__":    
    run()