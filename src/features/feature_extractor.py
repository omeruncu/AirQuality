import pandas as pd
import numpy as np

class FeatureExtractor:
    def __init__(self):
        self.pollutants = ['co', 'dew', 'h', 'no2', 'o3', 'p', 'pm10', 'pm25', 'so2', 't', 'w']
        self.category_mappings = {
            'aqi_category': ('aqi', [-np.inf, 50, 100, 150, 200, 300, np.inf],
                             ['Good', 'Moderate', 'Unhealthy for Sensitive Groups', 'Unhealthy', 'Very Unhealthy', 'Hazardous']),
            'carbon_intensity_category': ('carbon_intensity', [-np.inf, 100, 200, 300, 400, np.inf],
                                          ['Very Low', 'Low', 'Moderate', 'High', 'Very High'])
        }
        self.month_to_season = {12:0, 1:0, 2:0, 3:1, 4:1, 5:1, 6:2, 7:2, 8:2, 9:3, 10:3, 11:3}
        self.season_names = {0:'Winter', 1:'Spring', 2:'Summer', 3:'Autumn'}

    def extract_features(self, data):
        df = pd.DataFrame(data)
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        
        numeric_columns = ['aqi', 'carbon_intensity']
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
        
        for col, (source_col, bins, labels) in self.category_mappings.items():
            df[col] = pd.cut(df[source_col], bins=bins, labels=labels)
        
        df[[f'{p}_aqi' for p in self.pollutants]] = df['iaqi'].apply(lambda x: pd.Series({f'{p}_aqi': x.get(p, {}).get('v') for p in self.pollutants}))
        
        df['dominant_pollutant'] = df['iaqi'].apply(self._get_dominant_pollutant)
        
        df['season'] = df['timestamp'].dt.month.map(self.month_to_season).map(self.season_names)
        
        float_columns = ['carbon_intensity'] + [f'{p}_aqi' for p in self.pollutants]
        df[float_columns] = df[float_columns].apply(pd.to_numeric, errors='coerce')
        
        return df.drop('iaqi', axis=1)

    def add_rolling_averages(self, df, windows=[3, 6, 12, 24]):
        df = df.sort_values('timestamp')
        columns_to_roll = ['aqi', 'carbon_intensity'] + [f'{p}_aqi' for p in self.pollutants]
        
        for window in windows:
            df[[f'{col}_rolling_{window}h' for col in columns_to_roll]] = df[columns_to_roll].rolling(window=window, center=False, min_periods=1).mean()
        
        return df

    @staticmethod
    def _get_dominant_pollutant(iaqi):
        if isinstance(iaqi, dict):
            return max(iaqi, key=lambda k: iaqi[k].get('v', 0) if isinstance(iaqi[k], dict) else 0)
        return None