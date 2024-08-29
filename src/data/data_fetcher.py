import requests
from functools import lru_cache
import os
from dotenv import load_dotenv
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from src.utils.logger import LoggerFactory

load_dotenv()

class APIFetcher(ABC):
    def __init__(self):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

    @abstractmethod
    def fetch(self) -> Optional[Dict[str, Any]]:
        pass

class AirQualityFetcher(APIFetcher):
    def __init__(self, api_url: str, api_token: str):
        super().__init__()
        self.api_url = api_url
        self.api_token = api_token

    @lru_cache(maxsize=1)
    def fetch(self) -> Optional[Dict[str, Any]]:
        params = {"token": self.api_token}
        return self._fetch_data(self.api_url, params=params)

    def _fetch_data(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error("API request failed: %s", e)
            return None

class CarbonIntensityFetcher(APIFetcher):
    def __init__(self, api_url: str, api_token: str, city_lat: float, city_lon: float):
        super().__init__()
        self.api_url = api_url
        self.api_token = api_token
        self.city_lat = city_lat
        self.city_lon = city_lon

    @lru_cache(maxsize=1)
    def fetch(self) -> Optional[Dict[str, Any]]:
        headers = {"auth-token": self.api_token}
        params = {"lat": self.city_lat, "lon": self.city_lon}
        data = self._fetch_data(self.api_url, params=params, headers=headers)
        if data:
            self.logger.info("Carbon Intensity API Response: %s", json.dumps(data, indent=2))
        return data

    def _fetch_data(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error("API request failed: %s", e)
            return None

class DataFetcher:
    def __init__(self):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        self.air_quality_fetcher = AirQualityFetcher(
            api_url=os.getenv("API_URL"),
            api_token=os.getenv("API_TOKEN")
        )
        self.carbon_intensity_fetcher = CarbonIntensityFetcher(
            api_url=os.getenv("CARBON_API_URL"),
            api_token=os.getenv("CARBON_API_TOKEN"),
            city_lat=float(os.getenv("CITY_LAT")),
            city_lon=float(os.getenv("CITY_LON"))
        )

    def fetch_air_quality_data(self) -> Optional[Dict[str, Any]]:
        data = self.air_quality_fetcher.fetch()
        if data:
            self.logger.info("Air Quality data fetched successfully")
        else:
            self.logger.warning("Failed to fetch Air Quality data")
        return data

    def fetch_carbon_intensity_data(self) -> Optional[Dict[str, Any]]:
        data = self.carbon_intensity_fetcher.fetch()
        if data:
            self.logger.info("Carbon Intensity data fetched successfully")
        else:
            self.logger.warning("Failed to fetch Carbon Intensity data")
        return data