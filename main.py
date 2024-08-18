import os
import logging
from dotenv import load_dotenv
from src.config.config_manager import ConfigManager
from src.utils.logger import Logger
from src.data.data_collector import DataCollector
from src.features.feature_processor import FeatureProcessor

class MainApplication:
    def __init__(self):
        load_dotenv()
        self.config = ConfigManager()
        self.logger = Logger.setup()
        self.data_collector = DataCollector(self.config, self.logger)
        self.feature_processor = FeatureProcessor(self.logger)

    def run(self):
        try:
            save_directory = self.config.get('output', {}).get('save_directory', 'output')
            filename = self.config.get('output', {}).get('filename', 'sakarya_data.json')
            full_path = os.path.join(save_directory, filename)
            
            # Collect and save data
            all_data = self.data_collector.collect_data()
            self.data_collector.save_data(all_data, full_path)
            
            # Process and save features
            self.feature_processor.process_and_save_features(all_data, save_directory)
            
            self.logger.info("Data collection and feature processing completed successfully.")
        except KeyboardInterrupt:
            self.logger.info("The program was stopped by the user.")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}", exc_info=True)

def main():
    app = MainApplication()
    app.run()

if __name__ == "__main__":
    main()