import os
import hsfs
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List
from functools import wraps
import logging
import time
from abc import ABC, abstractmethod
from src.utils.logger import LoggerFactory
from src.utils.error_handler import ErrorHandler
from hsfs.feature import Feature
from src.data.postgresql_connector import PostgreSQLConnector
from src.config.config_manager import ConfigManager

class DataValidator(ABC):
    @abstractmethod
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        pass

class SchemaValidator(DataValidator):
    def __init__(self, expected_columns: List[str]):
        self._expected_columns = expected_columns

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        missing_columns = set(self._expected_columns) - set(data.keys())
        if missing_columns:
            if 'iaqi' in missing_columns:
                # 'iaqi' eksikse, diğer iaqi_* sütunlarının varlığını kontrol et
                iaqi_columns = [col for col in data.keys() if col.startswith('iaqi_')]
                if iaqi_columns:
                    missing_columns.remove('iaqi')
            if missing_columns:
                raise ValueError(f"Eksik sütunlar: {missing_columns}")
        return data

class TypeValidator(DataValidator):
    def __init__(self, expected_types: Dict[str, type]):
        self._expected_types = expected_types

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        for column, expected_type in self._expected_types.items():
            if column in data:
                if data[column] is None:
                    if expected_type == float:
                        data[column] = float('nan')  # veya başka bir uygun değer, örneğin 0.0
                    elif expected_type == int:
                        data[column] = -1  # veya başka bir uygun değer
                    else:
                        data[column] = "N/A"  # veya başka bir uygun değer
                elif not isinstance(data[column], expected_type):
                    try:
                        data[column] = expected_type(data[column])
                    except ValueError:
                        raise ValueError(f"Geçersiz veri tipi: {column} sütunu {expected_type} olmalı")
        return data

class OutlierDetector(DataValidator):
    def __init__(self, value_ranges: Dict[str, tuple]):
        self._value_ranges = value_ranges

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        for column, (min_val, max_val) in self._value_ranges.items():
            if column in data and data[column] is not None:
                if data[column] < min_val or data[column] > max_val:
                    data[column] = np.clip(data[column], min_val, max_val)
        return data

class MissingValueHandler(DataValidator):
    def __init__(self, numeric_columns: List[str]):
        self._numeric_columns = numeric_columns

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        for column in self._numeric_columns:
            if data[column] is None:
                data[column] = -1  # veya başka bir uygun değer
        return data

class ConsistencyChecker(DataValidator):
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if 'timestamp' in data:
            current_time = datetime.now()
            data_time = datetime.fromisoformat(data['timestamp'])
            if data_time > current_time or data_time < current_time - timedelta(days=1):
                data['timestamp'] = current_time.isoformat()
        return data

class IntegrityEnsurer(DataValidator):
    def __init__(self, expected_columns: List[str], expected_types: Dict[str, type]):
        self._expected_columns = expected_columns
        self._expected_types = expected_types

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        for field in self._expected_columns:
            if field not in data or data[field] is None:
                if field == 'timestamp':
                    data[field] = datetime.now().isoformat()
                elif self._expected_types[field] in (int, float):
                    data[field] = -1  # veya başka bir uygun değer
                else:
                    data[field] = "N/A"  # veya başka bir uygun değer
        return data

class DataPreprocessor:
    def __init__(self, validators: List[DataValidator]):
        self._validators = validators

    def preprocess(self, data: Dict[str, Any]) -> Dict[str, Any]:
        for validator in self._validators:
            data = validator.validate(data)
        return data
class DataPreprocessorFactory:
    @staticmethod
    def create() -> DataPreprocessor:
        validators = [
            SchemaValidator(['city', 'aqi', 'timestamp', 'iaqi', 'carbon_intensity']),
            TypeValidator({'aqi': float, 'carbon_intensity': int}),
            OutlierDetector({'aqi': (0, 500), 'carbon_intensity': (0, 1000)}),
            MissingValueHandler(['aqi', 'carbon_intensity']),
            ConsistencyChecker(),
            IntegrityEnsurer(['city', 'aqi', 'timestamp', 'iaqi', 'carbon_intensity'], 
                             {'aqi': float, 'carbon_intensity': int})
        ]
        return DataPreprocessor(validators)
    
class HopsworksConnector:
    def __init__(self, host: str, project_name: str, api_key: str):
        self._host = host
        self._project_name = project_name
        self._api_key = api_key
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        self._connection = None
        self._fs = None

    def connect(self):
        if self._connection is None:
            try:
                self.logger.info(f"Hopsworks'e bağlanmaya çalışılıyor. Host: {self._host}, Project: {self._project_name}")
                self._connection = hsfs.connection(
                    host=self._host,
                    project=self._project_name,
                    api_key_value=self._api_key,
                    engine="python"
                )
                self._fs = self._connection.get_feature_store()
                self.logger.info("Hopsworks'e başarıyla bağlanıldı.")
            except Exception as e:
                self.logger.error(f"Hopsworks'e bağlanılamadı: {str(e)}")
                raise

    def check_connection(self) -> bool:
        try:
            self.connect()
            return True
        except Exception as e:
            self.logger.error(f"Bağlantı hatası: {e}")
            return False

    def get_feature_store(self):
        if self._fs is None:
            self.connect()
        return self._fs

class HopsworksUploader:
    def __init__(self, config: ConfigManager):
        self._config = config
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        self._feature_group_name = os.getenv('HW_FEATURE_GN')
        self._batch_size = config.get('hopsworks.batch_size', 1000)
        self._retry_attempts = config.get('hopsworks.retry_attempts', 3)
        self._retry_delay = config.get('hopsworks.retry_delay', 5)
        self.postgresql_connector = PostgreSQLConnector(config.get('db_url'))
        self._connector = self._create_connector()
        self._preprocessor = self._create_preprocessor()

    def check_connection(self) -> bool:
        try:
            self._connector.get_feature_store()
            return True
        except Exception as e:
            self.logger.error(f"Hopsworks connection check failed: {str(e)}")
            return False
        
    @ErrorHandler.handle_errors("HopsworksUploader")
    @ErrorHandler.retry(max_attempts=3, delay=5)
    def upload_data_from_postgresql(self, batch_size=None):
        if batch_size is None:
            batch_size = self._batch_size

        fs = self._connector.get_feature_store()
        
        try:
            # PostgreSQL'den veri çekme
            data_batch = self.postgresql_connector.fetch_data(limit=batch_size)
            
            if not data_batch:
                self.logger.info("No new data to upload to Hopsworks.")
                return

            # Veriyi pandas DataFrame'e dönüştür
            df = pd.DataFrame(data_batch)
            
            # Veri tiplerini düzelt
            df['city'] = 1  # varchar(25) için
            df['city'] = df['city'].astype('int')  # int için
            df['carbon_intensity'] = df['carbon_intensity'].astype('int')  # int için
            # timestamp sütununu datetime objesine çevir ve formatı düzenle
            #df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # timestamp sütununu datetime objesine çevir ve UTC'ye dönüştür
            #df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC')
            # timestamp sütununu null yap
            df.drop('timestamp', axis=1, inplace=True)

            df['aqi'] = df['aqi'].fillna(-1)

            # Veri özeti ve son birkaç satırı logla
            self.logger.info(f"Yüklenecek veri özeti:\n{df.describe()}")
            self.logger.info(f"Son 5 satır:\n{df.head()}")

            # Feature Group'u al veya oluştur
            fg = fs.get_or_create_feature_group(
                name=self._feature_group_name,
                version=4
            )

            # DataFrame'i Hopsworks'e yükle
            fg.insert(df)
            self.logger.info(f"{len(df)} veri noktası başarıyla Hopsworks'e yüklendi.")

        except Exception as e:
            self.logger.error(f"Veri Hopsworks'e yüklenemedi: {e}", exc_info=True)
            raise

    def get_feature_group_schema(self):
        fs = self._connector.get_feature_store()
        fg = fs.get_feature_group(name=self._feature_group_name, version=1)
        return fg.schema
    
    def _create_connector(self) -> HopsworksConnector:
        return HopsworksConnector(
            os.getenv('HOPSWORKS_HOST'),
            os.getenv('HW_PROJECT_NAME'),
            os.getenv('HOPSWORKS_API_KEY')
        )

    def _create_preprocessor(self) -> DataPreprocessor:
        return DataPreprocessorFactory.create()

    
    
    def error_handler(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                self.logger.error(f"{func.__name__} metodunda hata oluştu: {str(e)}")
                raise
        return wrapper
    
    @error_handler
    def _prepare_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            processed_data = self._preprocessor.preprocess(data)
            if processed_data['aqi'] is None:
                processed_data['aqi'] = -1
            return processed_data
        except Exception as e:
            self.logger.error(f"_prepare_data metodunda hata oluştu: {str(e)}")
            raise
    
    def _flatten_iaqi(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if 'iaqi' not in data:
            self.logger.warning(f"'iaqi' anahtarı veride bulunamadı. Veri zaten düzleştirilmiş olabilir.")
            return data
        
        flattened = data.copy()
        for key, value in data['iaqi'].items():
            flattened[f'iaqi_{key}'] = float(value['v']) if isinstance(value, dict) and 'v' in value else value
        del flattened['iaqi']
        return flattened
    
    

    def set_log_level(self, level: str):
        """Log seviyesini ayarlar."""
        numeric_level = getattr(logging, level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f'Geçersiz log seviyesi: {level}')
        self._logger.setLevel(numeric_level)

class HopsworksUploaderFactory:
    @staticmethod
    def create(config: ConfigManager) -> HopsworksUploader:
        return HopsworksUploader(config)

def create_hopsworks_uploader(config: ConfigManager) -> HopsworksUploader:
    return HopsworksUploaderFactory.create(config)

# Bu yeni yapı, OOP ve SOLID prensiplerine daha uygun hale getirilmiştir:
# 1. Single Responsibility Principle (SRP): Her sınıf ve metod tek bir sorumluluğa sahip.
# Veri doğrulama işlemleri ayrı sınıflara bölünmüştür.
# Hopsworks bağlantısı HopsworksConnector sınıfına taşınmıştır.
# Open-Closed Principle (OCP): Yeni özellikler eklemek için mevcut kodu değiştirmek yerine yeni sınıflar ekleyebiliriz.
# Yeni bir veri doğrulama stratejisi eklemek için yeni bir DataValidator alt sınıfı oluşturup DataPreprocessor'a ekleyebiliriz.
# Liskov Substitution Principle (LSP): Tüm DataValidator alt sınıfları, temel sınıfın yerine kullanılabilir.
# Interface Segregation Principle (ISP): DataValidator arayüzü minimal ve odaklıdır.
# Dependency Inversion Principle (DIP): Yüksek seviye modüller (HopsworksUploader), düşük seviye modüllere (DataValidator alt sınıfları) doğrudan bağımlı değildir.