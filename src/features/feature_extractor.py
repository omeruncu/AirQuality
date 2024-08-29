import pytest
import pandas as pd
from datetime import datetime, timedelta
from src.features.feature_processor import FeatureExtractor

@pytest.fixture
def sample_data():
    base_time = datetime(2023, 5, 1, 12, 0)
    return [
        {
            "timestamp": (base_time + timedelta(hours=i)).isoformat(),
            "aqi": 50 + i,
            "iaqi": {
                "pm25": {"v": 20 + i},
                "pm10": {"v": 30 + i},
                "o3": {"v": 40 + i},
                "no2": {"v": 15 + i},
                "so2": {"v": 5 + i},
                "co": {"v": 0.8 + i * 0.1}
            },
            "carbon_intensity": 200 + i * 10
        } for i in range(48)  # 2 days of hourly data
    ]

@pytest.fixture
def feature_extractor():
    return FeatureExtractor()

def test_extract_features(feature_extractor, sample_data):
    df = feature_extractor.extract_features(sample_data)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(sample_data)
    assert set(df.columns) == {'timestamp', 'aqi', 'carbon_intensity', 'hour', 'day_of_week', 'pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'iaqi'}
    
    assert df['timestamp'].dtype == 'datetime64[ns]'
    assert df['hour'].dtype == 'int64'
    assert df['day_of_week'].dtype == 'int64'
    
    assert df['hour'].min() == 0
    assert df['hour'].max() == 23
    assert df['day_of_week'].min() == 0
    assert df['day_of_week'].max() == 1  # 0 for Monday, 1 for Tuesday

def test_add_rolling_averages(feature_extractor, sample_data):
    df = feature_extractor.extract_features(sample_data)
    df_with_averages = feature_extractor.add_rolling_averages(df)
    
    expected_columns = [
        'timestamp', 'aqi', 'carbon_intensity', 'hour', 'day_of_week', 
        'pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'iaqi',
        'aqi_3h_avg', 'aqi_24h_avg',
        'pm25_3h_avg', 'pm25_24h_avg',
        'pm10_3h_avg', 'pm10_24h_avg',
        'o3_3h_avg', 'o3_24h_avg',
        'no2_3h_avg', 'no2_24h_avg',
        'so2_3h_avg', 'so2_24h_avg',
        'co_3h_avg', 'co_24h_avg'
    ]
    
    assert set(df_with_averages.columns) == set(expected_columns)
    assert len(df_with_averages) == len(df)
    
    # Check if rolling averages are calculated correctly
    for col in ['aqi', 'pm25', 'pm10', 'o3', 'no2', 'so2', 'co']:
        assert (df_with_averages[f'{col}_3h_avg'].iloc[2] == 
                df_with_averages[col].iloc[:3].mean())
        assert (df_with_averages[f'{col}_24h_avg'].iloc[23] == 
                df_with_averages[col].iloc[:24].mean())

def test_extract_features_empty_data(feature_extractor):
    df = feature_extractor.extract_features([])
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0

def test_add_rolling_averages_empty_data(feature_extractor):
    empty_df = pd.DataFrame(columns=['timestamp', 'aqi', 'pm25', 'pm10', 'o3', 'no2', 'so2', 'co'])
    df_with_averages = feature_extractor.add_rolling_averages(empty_df)
    assert isinstance(df_with_averages, pd.DataFrame)
    assert len(df_with_averages) == 0

def test_extract_features_missing_data(feature_extractor):
    incomplete_data = [
        {
            "timestamp": "2023-05-01T12:00:00",
            "aqi": 50,
            "iaqi": {
                "pm25": {"v": 20},
                "pm10": {"v": 30},
                # Missing o3, no2, so2, co
            },
            "carbon_intensity": 200
        }
    ]
    df = feature_extractor.extract_features(incomplete_data)
    assert pd.isna(df['o3'].iloc[0])
    assert pd.isna(df['no2'].iloc[0])
    assert pd.isna(df['so2'].iloc[0])
    assert pd.isna(df['co'].iloc[0])

if __name__ == "__main__":
    pytest.main()