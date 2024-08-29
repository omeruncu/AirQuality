import time
import json
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from src.data.data_processor import create_data_processor
from src.data.hopsworks_uploader import create_hopsworks_uploader
from src.utils.error_handler import ErrorHandler
from src.utils.logger import LoggerFactory
from src.data.data_fetcher import DataFetcher
from src.data.postgresql_connector import PostgreSQLConnector
from src.config.config_manager import ConfigManager

class DataSaver(ABC):
    def __init__(self):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

    @abstractmethod
    def save(self, data: List[Dict[str, Any]], path: str):
        pass

class JsonDataSaver(DataSaver):
    def save(self, data: List[Dict[str, Any]], path: str):
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Total {len(data)} data points were collected and recorded.")
        except IOError as e:
            self.logger.error(f"Failed to save data to {path}: {e}")

class DataUploader(ABC):
    def __init__(self):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

    @abstractmethod
    def upload(self, data: Dict[str, Any]):
        pass

class HopsworksDataUploader(DataUploader):
    def __init__(self, uploader: create_hopsworks_uploader):
        super().__init__()
        self.uploader = uploader

    def upload(self, data: Dict[str, Any]):
        try:
            self.uploader.upload_data_from_postgresql(data)
        except Exception as e:
            self.logger.error(f"Hopsworks'e veri yüklenemedi: {e}")

class DataCollector:
    def __init__(self, config: ConfigManager, data_saver: DataSaver, data_uploader: DataUploader):
        self.config = config
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        self.data_saver = data_saver
        self.data_uploader = data_uploader
        self.data_fetcher = DataFetcher()
        self.data_processor = create_data_processor()
        self.postgresql_connector = PostgreSQLConnector(config.get('db_url'))

    @ErrorHandler.handle_errors("DataCollector")
    def collect_data(self) -> List[Dict[str, Any]]:
        all_data = []
        start_time = time.time()
        collection_time = self.config.get('data_collection', {}).get('total_collection_time', 3600)
        interval = self.config.get('data_collection', {}).get('request_interval', 300)
        
        while time.time() - start_time < collection_time:
            raw_aqi_data = self.data_fetcher.fetch_air_quality_data()
            carbon_data = self.data_fetcher.fetch_carbon_intensity_data()
            
            processed_data = self.data_processor.process_all_data(raw_aqi_data, carbon_data)
            if processed_data:
                all_data.append(processed_data)
                self.logger.info("Data processed successfully: %s", json.dumps(processed_data, indent=2))

                # PostgreSQL'e veri ekleme
                self.postgresql_connector.insert_data(processed_data)
                
                # Hopsworks'e veri yükleme
                self.data_uploader.upload(processed_data)
                    
            time_elapsed = time.time() - start_time
            time_remaining = collection_time - time_elapsed
            if time_remaining > interval:
                self.logger.info("Waiting for %s seconds...", interval)
                time.sleep(interval)
            else:
                break
        
        return all_data

    @ErrorHandler.handle_errors("DataCollector")
    def save_data(self, data: List[Dict[str, Any]], path: str):
        self.data_saver.save(data, path)

def create_data_collector(config: ConfigManager) -> DataCollector:
    data_saver = JsonDataSaver()
    hopsworks_uploader = create_hopsworks_uploader(config)
    data_uploader = HopsworksDataUploader(hopsworks_uploader)
    return DataCollector(config, data_saver, data_uploader)