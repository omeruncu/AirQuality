import os
import json
from .feature_extractor import FeatureExtractor

class FeatureProcessor:
    def __init__(self, logger):
        self.logger = logger
        self.feature_extractor = FeatureExtractor()

    def process_and_save_features(self, all_data, save_directory):
        if not all_data:
            self.logger.warning("No data collected, cannot extract features.")
            return

        self.logger.info(f"Sample data point: {json.dumps(all_data[0], indent=2)}")
        
        df = self.feature_extractor.extract_features(all_data)
        df = self.feature_extractor.add_rolling_averages(df)
        
        output_csv = os.path.join(save_directory, "sakarya_features.csv")
        try:
            df.to_csv(output_csv, index=False, float_format='%.2f')
            self.logger.info(f"Features extracted and saved to {output_csv}")
        except PermissionError:
            alternative_output = os.path.join(os.path.expanduser("~"), "sakarya_features.csv")
            df.to_csv(alternative_output, index=False, float_format='%.2f')
            self.logger.warning(f"Could not save to {output_csv}. Saved to {alternative_output} instead.")