import unittest
from unittest.mock import patch
from src.data import fetch_data

class TestDataFetcher(unittest.TestCase):

    @patch('src.data.fetch_data.requests.get')
    def test_fetch_air_quality_data(self, mock_get):
        # Mock the API response
        mock_get.return_value.json.return_value = {
            "status": "ok",
            "data": {
                "aqi": 50,
                "iaqi": {
                    "pm25": {"v": 20},
                    "pm10": {"v": 30}
                }
            }
        }
        
        result = fetch_data.fetch_air_quality_data()
        
        self.assertIsNotNone(result)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['data']['aqi'], 50)

    @patch('src.data.fetch_data.requests.get')
    def test_fetch_carbon_intensity_data(self, mock_get):
        # Mock the API response
        mock_get.return_value.json.return_value = {
            "carbonIntensity": 100,
            "datetime": "2023-04-20T12:00:00Z",
            "updatedAt": "2023-04-20T12:05:00Z"
        }
        
        result = fetch_data.fetch_carbon_intensity_data()
        
        self.assertIsNotNone(result)
        self.assertEqual(result['carbonIntensity'], 100)

if __name__ == '__main__':
    unittest.main()