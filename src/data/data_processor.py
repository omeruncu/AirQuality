import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def process_all_data(aqi_data, carbon_data):
    """Processes both AQI and carbon intensity data."""
    processed_aqi = process_aqi_data(aqi_data)
    processed_carbon = process_carbon_data(carbon_data)
    
    if processed_aqi and processed_carbon:
        processed_aqi['carbon_intensity'] = processed_carbon['carbonIntensity']
        return processed_aqi
    return None

def process_carbon_data(data):
    """Processes the received carbon intensity data."""
    if not validate_data(data, ['carbonIntensity', 'datetime', 'updatedAt']):
        logger.error("Invalid carbon intensity data received")
        return None
    
    return {
        "city": "Sakarya",
        "carbonIntensity": data.get('carbonIntensity'),
        "datetime": data.get('datetime'),
        "updatedAt": data.get('updatedAt')
    }
    
def process_aqi_data(data):
    """Processes the received AQI data."""
    if not validate_data(data, ['status', 'data'], nested_keys=['data', 'aqi']):
        logger.error(f"Invalid AQI data received: {data}")
        return None
    
    result = data.get('data', {})
    aqi = result.get('aqi')
    return {
        "city": "Sakarya",
        "aqi": aqi if aqi != "-" else None,
        "timestamp": datetime.now().isoformat(),
        "iaqi": result.get('iaqi', {})
    }

def validate_data(data, required_keys, nested_keys=None):
    """Validates the raw data."""
    if not isinstance(data, dict):
        return False
    if not all(key in data for key in required_keys):
        return False
    if nested_keys:
        nested_data = data
        for key in nested_keys[:-1]:
            nested_data = nested_data.get(key, {})
        if not isinstance(nested_data, dict) or nested_keys[-1] not in nested_data:
            return False
    return True