from pathlib import Path
import json
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from datetime import datetime
import pandas as pd
import logging

app = Flask(__name__)
CORS(app)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        #logging.FileHandler("data_extraction.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define data directories
DATA_DIR = Path('./data')
PRICES_DIR = DATA_DIR / 'prices'
PROPERTIES_DIR = DATA_DIR / 'properties'

# Define the paths for combined datasets
PROPERTIES_FILE = DATA_DIR / 'properties_combined.json'
LATEST_PRICES_FILE = DATA_DIR / 'prices_2017_onwards.csv'  # Prices from 2017 onwards
LATEST_AGGPRICES_FILE = DATA_DIR / 'agg_prices.csv'
PRICES_SEGMENTS = {
    '1990-1999': DATA_DIR / 'prices_1990_1999.csv',
    '2000-2012': DATA_DIR / 'prices_2000_2012.csv',
    '2012-2014': DATA_DIR / 'prices_2012_2014.csv',
    '2015-2016': DATA_DIR / 'prices_2015_2016.csv',
    '2017-2025': LATEST_PRICES_FILE
}

def load_prices_data_for_period(start_date, end_date):
    """
    Load prices data for a specific period from the segmented files
    
    Args:
        start_date: Start date (YYYY-MM-DD or YYYY-MM or YYYY)
        end_date: End date (YYYY-MM-DD or YYYY-MM or YYYY)
        
    Returns:
        DataFrame with the combined data for the specified period
    """
    # Convert dates to datetime objects for comparison
    if isinstance(start_date, str):
        if len(start_date) == 4:  # YYYY
            start_date = pd.to_datetime(f"{start_date}-01-01")
        elif len(start_date) == 7:  # YYYY-MM
            start_date = pd.to_datetime(f"{start_date}-01")
        else:
            start_date = pd.to_datetime(start_date)
    
    if isinstance(end_date, str):
        if len(end_date) == 4:  # YYYY
            end_date = pd.to_datetime(f"{end_date}-12-31")
        elif len(end_date) == 7:  # YYYY-MM
            end_date = pd.to_datetime(end_date) + pd.offsets.MonthEnd(0)
        else:
            end_date = pd.to_datetime(end_date)
    
    start_year = start_date.year
    end_year = end_date.year
    
    logger.info(f"Loading prices data for period {start_date.date()} to {end_date.date()}")
    
    # Determine which segments we need
    segments_to_load = []
    
    if start_year <= 1999:
        segments_to_load.append(PRICES_SEGMENTS['1990-1999'])
    
    if start_year <= 2012 and end_year >= 2000:
        segments_to_load.append(PRICES_SEGMENTS['2000-2012'])
    
    if start_year <= 2014 and end_year >= 2012:
        segments_to_load.append(PRICES_SEGMENTS['2012-2014'])
    
    if start_year <= 2016 and end_year >= 2015:
        segments_to_load.append(PRICES_SEGMENTS['2015-2016'])
    
    if end_year >= 2017:
        segments_to_load.append(LATEST_PRICES_FILE)
    
    all_dfs = []
    
    # Load each segment
    for segment_file in segments_to_load:
        if segment_file.exists():
            try:
                logger.info(f"Loading segment from {segment_file}")
                df = pd.read_csv(segment_file)
                
                # Convert date columns if needed
                if 'date' in df.columns and not pd.api.types.is_datetime64_dtype(df['date']):
                    df['date'] = pd.to_datetime(df['date'])
                
                # Filter to the requested date range
                if 'date' in df.columns:
                    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
                
                all_dfs.append(df)
                logger.info(f"Loaded {len(df)} records from {segment_file}")
                
            except Exception as e:
                logger.error(f"Error loading segment {segment_file}: {e}")
        else:
            logger.warning(f"Segment file {segment_file} does not exist")
    
    if not all_dfs:
        logger.warning(f"No data found for period {start_date} to {end_date}")
        return pd.DataFrame()
    
    # Combine all segments
    combined_df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Combined dataset has {len(combined_df)} records")
    
    return combined_df

@app.route('/')
def root():
    return send_from_directory('./client/dist', 'index.html')

# Path for the rest of the static files (JS/CSS)
@app.route('/<path:path>')
def assets(path):
    return send_from_directory('./client/dist', path)

@app.route('/api/geojson', methods=['GET'])
def get_geosjon():
    try:
        with open('./data/PlanningBoundaryArea.geojson', 'r') as file:
            data = json.load(file)
    
        return jsonify({
            'geojson': data
        })
            
    except Exception as e:
        print(f"Error in API: {e}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/properties', methods=['GET'])
def get_properties_local():
    """
    API endpoint to get properties data from local files
    """
    try:
        with open('./data/properties_combined.json', 'r') as file:
            data = json.load(file)
    
        return jsonify({
            'properties': data
        })
            
    except Exception as e:
        print(f"Error in API: {e}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/agg_prices', methods=['GET'])
def get_agg_prices_local():
    """
    API endpoint to get aggregated data from local files
    """
    try:
        df = pd.read_csv('./data/agg_prices.csv')

        df['price'] = pd.to_numeric(df['price'], errors='coerce')

        return jsonify({
            'prices': df.to_json(orient='records', date_format='iso')
        })
            
    except Exception as e:
        print(f"Error in API: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/agg_address_prices', methods=['GET'])
def get_address_prices_local():
    """
    API endpoint to get prices data from local files
    Query parameters:
    - start_date: (YYYY-MM-DD or YYYY-MM or YYYY) defaults to 1990
    - end_date: (YYYY-MM-DD or YYYY-MM or YYYY) defaults to current date
    - town: filter by town (optional)
    - towns: comma-separated list of towns to filter by (optional)
    - aggregation: 'monthly', 'quarterly', or 'yearly'
    """
    try:
        # Get current date
        current_date = datetime.now()

        # Calculate date 6 months ago
        year = current_date.year
        month = current_date.month

        # Adjust year and month for 6 months ago
        month -= 6
        if month <= 0:
            month += 12
            year -= 1

        # Format as 'YYYY-MM'
        default_start_date = f"{year}-{month:02d}"

        # Use in your request args
        start_date = request.args.get('start_date', default_start_date)
        end_date = request.args.get('end_date', current_date.strftime('%Y-%m'))

        print(f"Loading data for period {start_date} to {end_date}")

        df = load_prices_data_for_period(start_date, end_date)

        # Check if we have any data
        if df.empty:
            return jsonify({
                'prices': '[]',
                'message': 'No data found for the specified period'
            })
        
        df['block_street'] = df['block'] + ' ' + df['street']

        # Convert the price column to numeric, forcing non-numeric values to become NaN
        df['price'] = pd.to_numeric(df['price'], errors='coerce')

        # Filter out rows with NaN prices before aggregation
        df_filtered = df[df['price'].notna()]

        # Then aggregate
        df_aggregated = df_filtered.groupby(['block_street', 'flat_type']).agg(price=('price', 'median')).reset_index()

        print(f"Returning {len(df_aggregated)} records")

        return jsonify({
            'prices': df_aggregated.to_json(orient='records', date_format='iso')
        })
            
    except Exception as e:
        print(f"Error in API: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/prices', methods=['GET'])
def get_prices_local():
    """
    API endpoint to get prices data from local files
    Query parameters:
    - start_date: (YYYY-MM-DD or YYYY-MM or YYYY) defaults to 1990
    - end_date: (YYYY-MM-DD or YYYY-MM or YYYY) defaults to current date
    - town: filter by town (optional)
    - towns: comma-separated list of towns to filter by (optional)
    """
    try:
        # Get query parameters
        start_date = request.args.get('start_date', '2022-01')
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m'))

        # Handle either single town or multiple towns
        town = request.args.get('town')
        towns_param = request.args.get('towns')
        
        towns = []
        if town:
            towns = [town]
        elif towns_param:
            towns = [t.strip() for t in towns_param.split(',')]
        
        print(f"Loading data for period {start_date} to {end_date}")
        if towns:
            print(f"Filtering by towns: {towns}")
        
        df = load_prices_data_for_period(start_date, end_date)

        # Check if we have any data
        if df.empty:
            return jsonify({
                'prices': '[]',
                'message': 'No data found for the specified period'
            })
        
        # Filter by towns if specified
        if towns:
            df = df[df['town'].isin(towns)]
            print(f"DataFrame size after town filter: {len(df)}")
    
        # Cnvert the price column to numeric, forcing non-numeric values to become NaN
        df['price'] = pd.to_numeric(df['price'], errors='coerce')

        df_aggregated = df.groupby(['date', 'street', 'flat_type']).agg(price=('price', 'median')).reset_index()
        #df_aggregated = df

        print(f"Returning {len(df_aggregated)} records")
   
        return jsonify({
            'prices': df_aggregated.to_json(orient='records', date_format='iso')
        })
            
    except Exception as e:
        print(f"Error in API: {e}")
        return jsonify({'error': str(e)}), 500

# Vercel requires the app to be named 'app'
app = app

