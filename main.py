import os
from dotenv import load_dotenv
from src.config.config_manager import ConfigManagerFactory
from src.utils.logger import LoggerFactory
from src.data.data_collector import create_data_collector
from src.features.feature_engineering import FeatureProcessorFactory
from src.data.hopsworks_uploader import create_hopsworks_uploader
from datetime import datetime
from src.data.postgresql_connector import PostgreSQLConnector

class DataCollectionOrchestrator:
    def __init__(self, config, logger, data_collector):
        self.config = config
        self.logger = logger
        self.data_collector = data_collector

    def collect_and_save_data(self):
        self.logger.info("Starting data collection...")
        all_data = self.data_collector.collect_data()
        self.logger.info(f"Collected {len(all_data)} data points.")

        save_directory = self.config.get('output.save_directory', 'output')
        os.makedirs(save_directory, exist_ok=True)
        self.logger.info(f"Data will be saved in: {save_directory}")

        filename = f"sakarya_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        full_path = os.path.join(save_directory, filename)
        self.data_collector.save_data(all_data, full_path)
        self.logger.info(f"Raw data saved to: {full_path}")

        return all_data, save_directory

class FeatureEngineeringOrchestrator:
    def __init__(self, logger, feature_processor):
        self.logger = logger
        self.feature_processor = feature_processor

    def process_features(self, all_data, save_directory):
        self.logger.info("Processing and extracting features...")
        processed_data = self.feature_processor.process_and_save_features(all_data, save_directory)
        self.logger.info(f"Processed data shape: {processed_data.shape}")
        return processed_data

class DataUploadOrchestrator:
    def __init__(self, logger, data_uploader, batch_size=100):
        self.logger = logger
        self.data_uploader = data_uploader
        self.batch_size = batch_size

    def upload_data(self):
        self.logger.info(f"Uploading data from PostgreSQL to Hopsworks (batch size: {self.batch_size})...")
        try:
            self.data_uploader.upload_data_from_postgresql(batch_size=self.batch_size)
            self.logger.info("Data uploaded successfully.")
        except Exception as e:
            self.logger.error(f"Error uploading data to Hopsworks: {str(e)}")
            raise

class Application:
    def __init__(self, config, logger, data_collection_orchestrator, feature_engineering_orchestrator, data_upload_orchestrator):
        self.config = config
        self.logger = logger
        self.data_collection_orchestrator = data_collection_orchestrator
        self.feature_engineering_orchestrator = feature_engineering_orchestrator
        self.data_upload_orchestrator = data_upload_orchestrator

    def run(self):
        try:
            self.logger.info("Starting the application...")
            
            if not self.data_upload_orchestrator.data_uploader.check_connection():
                self.logger.error("Hopsworks connection failed. Terminating the process.")
                return

            # Veri toplama
            self.data_collection_orchestrator.collect_and_save_data()
            
            # Veri yükleme
            self.data_upload_orchestrator.upload_data()
            
            self.logger.info("Data collection and uploading to Hopsworks completed successfully.")
        except KeyboardInterrupt:
            self.logger.info("Program interrupted by user.")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)

class ApplicationFactory:
    @staticmethod
    def create() -> Application:
        load_dotenv()

        # Manually construct the db_url from environment variables
        db_url = f"postgresql://{os.getenv('db_username')}:{os.getenv('db_password')}@{os.getenv('db_host')}:{os.getenv('db_port')}/{os.getenv('db_name')}"

        config = ConfigManagerFactory.create()
        config.set('db_url', db_url)
        
        logger = LoggerFactory.get_logger("Application")
        
        # When creating PostgreSQLConnector, use:
        postgresql_connector = PostgreSQLConnector(config.get('db_url'))

        if not postgresql_connector.test_connection():
            logger.error("Database connection failed. Exiting...")
            return None
        
        data_collector = create_data_collector(config)
        feature_processor = FeatureProcessorFactory.create()
        data_uploader = create_hopsworks_uploader(config)
        
        data_collection_orchestrator = DataCollectionOrchestrator(config, logger, data_collector)
        feature_engineering_orchestrator = FeatureEngineeringOrchestrator(logger, feature_processor)
        data_upload_orchestrator = DataUploadOrchestrator(logger, data_uploader)
        
        logger.info("Application components initialized successfully.")
        
        return Application(config, logger, data_collection_orchestrator, feature_engineering_orchestrator, data_upload_orchestrator)
    
def main():
    logger = LoggerFactory.get_logger("Main")
    try:
        logger.info("Creating application...")
        app = ApplicationFactory.create()
        logger.info("Running application...")
        app.run()
    except Exception as e:
        logger.error(f"Failed to create or run application: {e}", exc_info=True)

if __name__ == "__main__":
    main()