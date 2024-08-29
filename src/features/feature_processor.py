import os
import json
from typing import List, Dict, Any, Protocol
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime
from src.utils.logger import LoggerFactory

class DataFrameProcessor(Protocol):
    def extract_features(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        ...
    
    def add_rolling_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        ...

class FeatureSaver(ABC):
    @abstractmethod
    def save(self, df: pd.DataFrame, path: str) -> None:
        pass

class CSVFeatureSaver(FeatureSaver):
    def __init__(self):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

    def save(self, df: pd.DataFrame, path: str) -> None:
        try:
            df.to_csv(path, index=False, float_format='%.2f')
            self.logger.info(f"Features saved successfully to {path}")
            self.logger.debug(f"CSV file size: {os.path.getsize(path)} bytes")
        except PermissionError:
            self.logger.warning(f"Permission denied when trying to save to {path}")
            alternative_path = os.path.join(os.path.expanduser("~"), f"sakarya_features_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            self.save(df, alternative_path)
        except Exception as e:
            self.logger.error(f"Failed to save features to {path}: {str(e)}", exc_info=True)

class FeatureExtractor(DataFrameProcessor):
    def __init__(self):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

    def extract_features(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        self.logger.info("Starting feature extraction")
        df = pd.DataFrame(data)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Extract hour and day of week
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        
        # Extract individual pollutant values
        for pollutant in ['pm25', 'pm10', 'o3', 'no2', 'so2', 'co']:
            df[pollutant] = df['iaqi'].apply(lambda x: x.get(pollutant, {}).get('v', None))
        
        self.logger.info(f"Feature extraction completed. DataFrame shape: {df.shape}")
        return df

    def add_rolling_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        self.logger.info("Adding rolling averages")
        
        # Sort by timestamp to ensure correct rolling average calculation
        df = df.sort_values('timestamp')
        
        # Calculate rolling averages for AQI and each pollutant
        for column in ['aqi', 'pm25', 'pm10', 'o3', 'no2', 'so2', 'co']:
            df[f'{column}_3h_avg'] = df[column].rolling(window=3, min_periods=1).mean()
            df[f'{column}_24h_avg'] = df[column].rolling(window=24, min_periods=1).mean()
        
        self.logger.info(f"Rolling averages added. New DataFrame shape: {df.shape}")
        return df

class FeatureProcessor:
    def __init__(self, extractor: DataFrameProcessor, saver: FeatureSaver):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        self.extractor = extractor
        self.saver = saver

    def process_and_save_features(self, all_data: List[Dict[str, Any]], save_directory: str) -> pd.DataFrame:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        
        if not all_data:
            self.logger.warning("No data collected, cannot extract features.")
            return pd.DataFrame()

        self.logger.info(f"Processing {len(all_data)} data points.")
        self.logger.debug(f"Sample data point: {json.dumps(all_data[0], indent=2)}")
        
        try:
            df = self.extractor.extract_features(all_data)
            self.logger.info(f"Features extracted. Shape of DataFrame: {df.shape}")
            self.logger.debug(f"Columns after extraction: {df.columns.tolist()}")
            
            df = self.extractor.add_rolling_averages(df)
            self.logger.info(f"Rolling averages added. New shape of DataFrame: {df.shape}")
            self.logger.debug(f"Columns after adding rolling averages: {df.columns.tolist()}")
            
            output_csv = os.path.join(save_directory, f"sakarya_features_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            self.saver.save(df, output_csv)
            
            return df
        except Exception as e:
            self.logger.error(f"An error occurred during feature processing: {str(e)}", exc_info=True)
            return pd.DataFrame()

class FeatureProcessorFactory:
    @staticmethod
    def create() -> FeatureProcessor:
        extractor = FeatureExtractor()
        saver = CSVFeatureSaver()
        return FeatureProcessor(extractor, saver)

# Usage
if __name__ == "__main__":
    # This is just an example of how to use the FeatureProcessor
    sample_data = [
        {
            "timestamp": "2024-05-01T12:00:00",
            "aqi": 50,
            "iaqi": {
                "pm25": {"v": 20},
                "pm10": {"v": 30},
                "o3": {"v": 40},
                "no2": {"v": 15},
                "so2": {"v": 5},
                "co": {"v": 0.8}
            },
            "carbon_intensity": 200
        }
        # ... more data points ...
    ]

    processor = FeatureProcessorFactory.create()
    result_df = processor.process_and_save_features(sample_data, ".")
    print(result_df.head())