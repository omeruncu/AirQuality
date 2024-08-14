import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
from datetime import datetime
import os
from statsmodels.tsa.seasonal import seasonal_decompose

# Sonuçlar için klasör oluşturma
results_folder = "analysis_results"
if not os.path.exists(results_folder):
    os.makedirs(results_folder)

def load_data(filename="sakarya_aqi_data.json"):
    with open(filename, 'r') as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
         
    # iaqi sütunlarını açalım
    if 'iaqi' in df.columns:
        for col in df['iaqi'].iloc[0].keys():
            df[f'iaqi_{col}'] = df['iaqi'].apply(lambda x: x.get(col, {}).get('v', None))
         
    df = df.drop('iaqi', axis=1)
    return df

def calculate_basic_stats(df):
    return df['aqi'].describe()

def analyze_aqi_trends(df):
    plt.figure(figsize=(12, 6))
    plt.plot(df.index, df['aqi'])
    plt.title('Sakarya AQI Trends')
    plt.xlabel('Date')
    plt.ylabel('AQI')
    plt.grid(True)
    plt.savefig(os.path.join(results_folder, 'sakarya_aqi_trends.png'))
    plt.close()

def analyze_pollutants(df):
    pollutants = ['pm25', 'pm10', 'o3', 'no2', 'so2', 'co']
    pollutant_data = {p: df[f'iaqi.{p}.v'] for p in pollutants if f'iaqi.{p}.v' in df.columns}
    
    if pollutant_data:
        plt.figure(figsize=(12, 6))
        for pollutant, data in pollutant_data.items():
            plt.plot(df.index, data, label=pollutant)
        plt.title('Pollutant Levels in Sakarya')
        plt.xlabel('Date')
        plt.ylabel('Concentration')
        plt.legend()
        plt.grid(True)
        plt.savefig(os.path.join(results_folder, 'sakarya_pollutants.png'))
        plt.close()

def create_markdown_report(stats, filename="analysis_report.md"):
    with open(os.path.join(results_folder, filename), "w") as f:
        f.write("# Sakarya Air Quality Analysis Report\n\n")
        f.write("## Basic AQI Statistics\n")
        f.write("```\n")
        f.write(str(stats))
        f.write("\n```\n\n")
        f.write("## Visualizations\n")
        f.write("### AQI Trends\n")
        f.write("![AQI Trends](sakarya_aqi_trends.png)\n\n")
        f.write("### Pollutant Levels\n")
        f.write("![Pollutant Levels](sakarya_pollutants.png)\n")
        f.write("### AQI Decomposition\n")
        f.write("![AQI Decomposition](aqi_decomposition.png)\n")
        f.write("### Pollutant Correlation\n")
        f.write("![Pollutant Correlation](pollutant_correlation.png)\n")

def perform_time_series_decomposition(df):
    if len(df) < 40:
        print("Not enough data for time series decomposition. Skipping this analysis.")
        return
    period = len(df)
    result = seasonal_decompose(df['aqi'], model='additive', period=period) 
    plt.figure(figsize=(12, 10))
    result.plot()
    plt.tight_layout()
    plt.savefig(os.path.join(results_folder, 'aqi_decomposition.png'))
    plt.close()

def perform_correlation_analysis(df):
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    # Remove columns with no variation
    numeric_columns = [col for col in numeric_columns if df[col].std() != 0]
    
    if len(numeric_columns) < 2:
        print("Not enough varying numeric columns for correlation analysis. Skipping this analysis.")
        return
    
    corr_matrix = df[numeric_columns].corr()
         
    if corr_matrix.empty:
        print("Correlation matrix is empty. Not enough valid data for analysis.")
        return
         
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1, center=0)
    plt.title('Correlation between Numeric Variables')
    plt.tight_layout()
    plt.savefig(os.path.join(results_folder, 'correlation_heatmap.png'))
    plt.close()
         
    print("Correlation Matrix:")
    print(corr_matrix)

def create_markdown_report(stats, df, filename="analysis_report.md"):
    with open(os.path.join(results_folder, filename), "w") as f:
        f.write("# Sakarya Air Quality Analysis Report\n\n")
        f.write("## Basic AQI Statistics\n")
        f.write("```\n")
        f.write(str(stats))
        f.write("\n```\n\n")
        f.write("## Raw Data\n")
        f.write("```\n")
        f.write(str(df))
        f.write("\n```\n\n")
        f.write("## Visualizations\n")
        f.write("### AQI Values\n")
        f.write("![AQI Values](sakarya_aqi_values.png)\n\n")
        f.write("### Pollutant Levels\n")
        f.write("![Pollutant Levels](sakarya_pollutants.png)\n")
        f.write("### AQI Decomposition\n")
        f.write("![AQI Decomposition](aqi_decomposition.png)\n")
        f.write("### Pollutant Correlation\n")
        f.write("![Pollutant Correlation](pollutant_correlation.png)\n")

def main():
    df = load_data()
    print("DataFrame columns:")
    print(df.columns)
    print("\nDataFrame head:")
    print(df.head())
    print("\nDataFrame info:")
    print(df.describe())
    
    stats = calculate_basic_stats(df)
    print("Basic AQI Statistics:")
    print(stats)
    
    if df['aqi'].std() == 0:
        print("Warning: AQI values are constant. Some analyses may not be meaningful.")
        
    analyze_aqi_trends(df)
    analyze_pollutants(df)
    try:
        perform_time_series_decomposition(df)
    except Exception as e:
        print(f"Time series decomposition failed: {e}")
    
    try:
        perform_correlation_analysis(df)
    except Exception as e:
        print(f"Correlation analysis failed: {e}")
    
    create_markdown_report(stats, df)
    
    print("Analysis complete. Graphs and report saved in the results folder.")

if __name__ == "__main__":
    main()