import time
import json
from src.data import data_fetcher, data_processor

class DataCollector:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def collect_data(self):
        all_data = []
        start_time = time.time()
        collection_time = self.config.get('data_collection', {}).get('total_collection_time', 3600)
        interval = self.config.get('data_collection', {}).get('request_interval', 300)
        
        while time.time() - start_time < collection_time:
            raw_aqi_data = data_fetcher.fetch_air_quality_data()
            carbon_data = data_fetcher.fetch_carbon_intensity_data()
            
            processed_data = data_processor.process_all_data(raw_aqi_data, carbon_data)
            if processed_data:
                all_data.append(processed_data)
                self.logger.info(f"Processed data: {json.dumps(processed_data, indent=2)}")
            
            time_elapsed = time.time() - start_time
            time_remaining = collection_time - time_elapsed
            if time_remaining > interval:
                self.logger.info(f"Waiting for {interval} seconds...")
                time.sleep(interval)
            else:
                break
        
        return all_data

    def save_data(self, data, path):
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Total {len(data)} data points were collected and recorded.")
        except IOError as e:
            self.logger.error(f"Failed to save data to {path}: {e}")