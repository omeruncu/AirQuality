import requests
import json
import logging
from datetime import datetime
import time
from functools import lru_cache
import os
from dotenv import load_dotenv

load_dotenv()

# Dosyanın kaydedileceği dizini belirleyin
save_directory = r"C:\Users\ST27\Desktop\AQI"
filename = "sakarya_aqi_data.json"
full_path = os.path.join(save_directory, filename)

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API information
API_URL = os.getenv("API_URL")
API_TOKEN = os.getenv("API_TOKEN")

# Wait 5 minutes between each request
REQUEST_INTERVAL = 30  # in seconds
# Total data collection time (e.g. 1 hours)
TOTAL_COLLECTION_TIME = 1 * 10 * 60  # in seconds

@lru_cache(maxsize=1)
def fetch_air_quality_data():
    """Retrieves air quality data from API for Sakarya."""
    params = {
        "token": API_TOKEN
    }
    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()  # Check for HTTP errors
        return response.json()
    except requests.RequestException as e:
        logger.error(f"API request failed: {e}")
        return None

def process_data(data):
    """Processes the received data."""
    if not data or data.get('status') != 'ok':
        logger.error(f"Data not received or incorrect: {data}")
        return None
    
    result = data.get('data', {})
    return {
        "city": "Sakarya",
        "aqi": result.get('aqi'),
        "timestamp": datetime.now().isoformat(),
        "iaqi": result.get('iaqi', {})
    }

def save_data(data, filename="sakarya_aqi_data.json"):
    """Saves the processed data to file."""
    try:
        with open(filename, "a") as f:
            json.dump(data, f)
            f.write("\n")
        logger.info(f"Data saved successfully:{filename}")
    except IOError as e:
        logger.error(f"Failed to save data:{e}")

def main():
    all_data = []
    start_time = time.time()
    
    while time.time() - start_time < TOTAL_COLLECTION_TIME:
        raw_data = fetch_air_quality_data()
        if raw_data:
            processed_data = process_data(raw_data)
            if processed_data:
                all_data.append(processed_data)
                print(json.dumps(processed_data, indent=2))
            else:
                logger.warning("Data processing failed.")
        else:
            logger.warning("Data could not be retrieved.")
        
        time_elapsed = time.time() - start_time
        time_remaining = TOTAL_COLLECTION_TIME - time_elapsed
        if time_remaining > REQUEST_INTERVAL:
            logger.info(f"{REQUEST_INTERVAL} waiting for seconds...")
            time.sleep(REQUEST_INTERVAL)
        else:
            break

    try:
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Total {len(all_data)} data points were collected and recorded.")
    except IOError as e:
        logger.error(f"Failed to save data to {full_path}: {e}")
    
    
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("The program was stopped by the user.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")