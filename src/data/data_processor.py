from datetime import datetime
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from src.utils.logger import LoggerFactory

class DataValidator(ABC):
    def __init__(self):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

    @abstractmethod
    def validate(self, data: Dict[str, Any]) -> bool:
        pass

class BasicDataValidator(DataValidator):
    def __init__(self, required_keys: List[str], nested_keys: Optional[List[str]] = None):
        super().__init__()
        self.required_keys = required_keys
        self.nested_keys = nested_keys

    def validate(self, data: Dict[str, Any]) -> bool:
        if not isinstance(data, dict):
            self.logger.error("Data is not a dictionary")
            return False
        if not all(key in data for key in self.required_keys):
            self.logger.error(f"Missing required keys: {set(self.required_keys) - set(data.keys())}")
            return False
        if self.nested_keys:
            nested_data = data
            for key in self.nested_keys[:-1]:
                nested_data = nested_data.get(key, {})
            if not isinstance(nested_data, dict) or self.nested_keys[-1] not in nested_data:
                self.logger.error(f"Missing or invalid nested key: {self.nested_keys}")
                return False
        return True

class DataProcessor(ABC):
    def __init__(self):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

    @abstractmethod
    def process(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pass

class AQIDataProcessor(DataProcessor):
    def __init__(self, validator: DataValidator):
        super().__init__()
        self.validator = validator

    def process(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.validator.validate(data):
            self.logger.error(f"Invalid AQI data received: {data}")
            return None
        
        result = data.get('data', {})
        aqi = result.get('aqi')
        return {
            "city": "Sakarya",
            "aqi": aqi if aqi != "-" else None,
            "timestamp": datetime.now().isoformat(),
            "iaqi": result.get('iaqi', {})
        }

class CarbonIntensityDataProcessor(DataProcessor):
    def __init__(self, validator: DataValidator):
        super().__init__()
        self.validator = validator

    def process(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.validator.validate(data):
            self.logger.error("Invalid carbon intensity data received")
            return None
        
        return {
            "city": "Sakarya",
            "carbonIntensity": data.get('carbonIntensity'),
            "datetime": data.get('datetime'),
            "updatedAt": data.get('updatedAt')
        }

class CombinedDataProcessor:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CombinedDataProcessor, cls).__new__(cls)
            cls._instance.logger = LoggerFactory.get_logger(cls.__name__)
            cls._instance.aqi_processor = None
            cls._instance.carbon_processor = None
        return cls._instance

    def initialize(self, aqi_processor: AQIDataProcessor, carbon_processor: CarbonIntensityDataProcessor):
        self.aqi_processor = aqi_processor
        self.carbon_processor = carbon_processor

    @staticmethod
    def process_all_data(aqi_data: Dict[str, Any], carbon_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        instance = CombinedDataProcessor()
        if not instance.aqi_processor or not instance.carbon_processor:
            instance.logger.error("Processors not initialized")
            return None

        processed_aqi = instance.aqi_processor.process(aqi_data)
        processed_carbon = instance.carbon_processor.process(carbon_data)
        
        if processed_aqi and processed_carbon:
            processed_aqi['carbon_intensity'] = processed_carbon['carbonIntensity']
            instance.logger.info("Successfully processed and combined AQI and Carbon Intensity data")
            return processed_aqi
        instance.logger.error("Failed to process either AQI or Carbon Intensity data")
        return None

def create_data_processor() -> CombinedDataProcessor:
    aqi_validator = BasicDataValidator(['status', 'data'], nested_keys=['data', 'aqi'])
    carbon_validator = BasicDataValidator(['carbonIntensity', 'datetime', 'updatedAt'])
    
    aqi_processor = AQIDataProcessor(aqi_validator)
    carbon_processor = CarbonIntensityDataProcessor(carbon_validator)
    
    processor = CombinedDataProcessor()
    processor.initialize(aqi_processor, carbon_processor)
    return processor