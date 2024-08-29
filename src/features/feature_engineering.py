import pandas as pd
from typing import List, Dict, Any, Protocol
from abc import ABC, abstractmethod
from datetime import datetime
from src.utils.logger import LoggerFactory
from src.utils.error_handler import ErrorHandler
import os

class FeatureEngineer(ABC):
    @abstractmethod
    def extract_features(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        pass

    @abstractmethod
    def add_rolling_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        pass

class BasicFeatureEngineer(FeatureEngineer):
    def __init__(self):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

    @ErrorHandler.handle_errors("BasicFeatureEngineer")
    def extract_features(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        self.logger.info("Starting feature extraction")
        df = pd.DataFrame(data)
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        
        for pollutant in ['pm25', 'pm10', 'o3', 'no2', 'so2', 'co']:
            df[pollutant] = df['iaqi'].apply(lambda x: x.get(pollutant, {}).get('v', None))
        
        self.logger.info(f"Feature extraction completed. DataFrame shape: {df.shape}")
        return df

    @ErrorHandler.handle_errors("BasicFeatureEngineer")
    def add_rolling_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        self.logger.info("Adding rolling averages")
        
        df = df.sort_values('timestamp')
        
        for column in ['aqi', 'pm25', 'pm10', 'o3', 'no2', 'so2', 'co']:
            df[f'{column}_3h_avg'] = df[column].rolling(window=3, min_periods=1).mean()
            df[f'{column}_24h_avg'] = df[column].rolling(window=24, min_periods=1).mean()
        
        self.logger.info(f"Rolling averages added. New DataFrame shape: {df.shape}")
        return df

class FeatureSaver(ABC):
    @abstractmethod
    def save(self, df: pd.DataFrame, path: str) -> None:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        pass

class CSVFeatureSaver(FeatureSaver):
    def __init__(self):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

    @ErrorHandler.handle_errors("CSVFeatureSaver")
    def save(self, df: pd.DataFrame, path: str) -> None:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        df.to_csv(path, index=False, float_format='%.2f')
        self.logger.info(f"Features saved successfully to {path}")
        self.logger.debug(f"CSV file size: {os.path.getsize(path)} bytes")

class FeatureProcessor:
    def __init__(self, engineer: FeatureEngineer, saver: FeatureSaver):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        self.engineer = engineer
        self.saver = saver

    @ErrorHandler.handle_errors("FeatureProcessor")
    def process_and_save_features(self, all_data: List[Dict[str, Any]], save_directory: str) -> pd.DataFrame:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        
        if not all_data:
            self.logger.warning("No data collected, cannot extract features.")
            return pd.DataFrame()

        self.logger.info(f"Processing {len(all_data)} data points.")
        
        df = self.engineer.extract_features(all_data)
        df = self.engineer.add_rolling_averages(df)
        
        output_csv = os.path.join(save_directory, f"sakarya_features_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        self.saver.save(df, output_csv)
        
        return df

class FeatureProcessorFactory:
    @staticmethod
    def create() -> FeatureProcessor:
        engineer = BasicFeatureEngineer()
        saver = CSVFeatureSaver()
        return FeatureProcessor(engineer, saver)