import requests
import logging
from functools import lru_cache
import os
from dotenv import load_dotenv
import json

load_dotenv()

logger = logging.getLogger(__name__)

API_URL = os.getenv("API_URL")
API_TOKEN = os.getenv("API_TOKEN")
CARBON_API_URL = os.getenv("CARBON_API_URL")
CARBON_API_TOKEN = os.getenv("CARBON_API_TOKEN")
CITY_LAT = float(os.getenv("CITY_LAT"))
CITY_LON = float(os.getenv("CITY_LON"))

def fetch_data(url, params=None, headers=None):
    """Generic function to fetch data from an API."""
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"API request failed: {e}")
        return None

@lru_cache(maxsize=1)
def fetch_air_quality_data():
    """Retrieves air quality data from API for Sakarya."""
    params = {"token": API_TOKEN}
    return fetch_data(API_URL, params=params)
    
@lru_cache(maxsize=1)
def fetch_carbon_intensity_data():
    """Retrieves carbon intensity data from API for Sakarya."""
    headers = {"auth-token": CARBON_API_TOKEN}
    params = {"lat": CITY_LAT, "lon": CITY_LON}
    data = fetch_data(CARBON_API_URL, params=params, headers=headers)
    if data:
        logger.info("Carbon Intensity API Response: %s", json.dumps(data, indent=2))
    return data