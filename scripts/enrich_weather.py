"""
Weather Enrichment Script - NYC Taxi Trip Duration
Fetches historical weather data from Open-Meteo API and joins with taxi data.
"""
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import time
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
PROCESSED_DATA_PATH = '/opt/airflow/data/processed/cleaned_taxi_data.csv'
ENRICHED_DATA_PATH = '/opt/airflow/data/processed/enriched_taxi_data.csv'

# NYC Central coordinates (for weather data)
NYC_LAT = 40.7128
NYC_LON = -74.0060

# Open-Meteo Historical Weather API (FREE, no API key needed)
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"


def fetch_weather_data(start_date, end_date):
    """
    Fetch historical hourly weather data from Open-Meteo API.
    Returns DataFrame with hourly weather conditions.
    """
    logger.info(f"Fetching weather data from {start_date} to {end_date}")
    
    params = {
        "latitude": NYC_LAT,
        "longitude": NYC_LON,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m", 
            "precipitation",
            "rain",
            "snowfall",
            "wind_speed_10m",
            "weather_code"
        ],
        "timezone": "America/New_York"
    }
    
    try:
        response = requests.get(OPEN_METEO_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        # Parse hourly data
        hourly_data = data.get('hourly', {})
        weather_df = pd.DataFrame({
            'datetime': pd.to_datetime(hourly_data.get('time', [])),
            'temperature_c': hourly_data.get('temperature_2m', []),
            'humidity_pct': hourly_data.get('relative_humidity_2m', []),
            'precipitation_mm': hourly_data.get('precipitation', []),
            'rain_mm': hourly_data.get('rain', []),
            'snowfall_mm': hourly_data.get('snowfall', []),
            'wind_speed_kmh': hourly_data.get('wind_speed_10m', []),
            'weather_code': hourly_data.get('weather_code', [])
        })
        
        logger.info(f"Fetched {len(weather_df)} hourly weather records")
        return weather_df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch weather data: {e}")
        raise


def decode_weather_condition(code):
    """
    Decode WMO weather code to human-readable condition.
    See: https://open-meteo.com/en/docs
    """
    weather_codes = {
        0: 'Clear',
        1: 'Mainly Clear', 2: 'Partly Cloudy', 3: 'Overcast',
        45: 'Fog', 48: 'Depositing Rime Fog',
        51: 'Light Drizzle', 53: 'Moderate Drizzle', 55: 'Dense Drizzle',
        61: 'Slight Rain', 63: 'Moderate Rain', 65: 'Heavy Rain',
        66: 'Light Freezing Rain', 67: 'Heavy Freezing Rain',
        71: 'Slight Snow', 73: 'Moderate Snow', 75: 'Heavy Snow',
        77: 'Snow Grains',
        80: 'Slight Rain Showers', 81: 'Moderate Rain Showers', 82: 'Violent Rain Showers',
        85: 'Slight Snow Showers', 86: 'Heavy Snow Showers',
        95: 'Thunderstorm', 96: 'Thunderstorm with Hail', 99: 'Thunderstorm with Heavy Hail'
    }
    return weather_codes.get(code, 'Unknown')


def enrich_with_weather():
    """Main weather enrichment function."""
    logger.info(f"Loading processed data from {PROCESSED_DATA_PATH}")
    
    # Load processed taxi data
    df = pd.read_csv(PROCESSED_DATA_PATH)
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['pickup_date'] = pd.to_datetime(df['pickup_date'])
    
    logger.info(f"Loaded {len(df):,} taxi records")
    
    # Determine date range for weather data
    min_date = df['pickup_date'].min().strftime('%Y-%m-%d')
    max_date = df['pickup_date'].max().strftime('%Y-%m-%d')
    
    # Fetch weather data
    weather_df = fetch_weather_data(min_date, max_date)
    
    # Create a merge key (rounded to nearest hour)
    df['weather_hour'] = df['pickup_datetime'].dt.floor('H')
    weather_df['weather_hour'] = weather_df['datetime'].dt.floor('H')
    
    # Merge weather data with taxi data
    logger.info("Merging weather data with taxi trips...")
    df = df.merge(
        weather_df[['weather_hour', 'temperature_c', 'humidity_pct', 
                    'precipitation_mm', 'rain_mm', 'snowfall_mm', 
                    'wind_speed_kmh', 'weather_code']],
        on='weather_hour',
        how='left'
    )
    
    # Add derived weather features
    df['is_raining'] = (df['rain_mm'] > 0).astype(int)
    df['is_snowing'] = (df['snowfall_mm'] > 0).astype(int)
    df['is_bad_weather'] = (
        (df['is_raining'] == 1) | 
        (df['is_snowing'] == 1) | 
        (df['weather_code'].isin([45, 48, 95, 96, 99]))
    ).astype(int)
    
    # Decode weather condition
    df['weather_condition'] = df['weather_code'].apply(decode_weather_condition)
    
    # Temperature categories
    df['temp_category'] = pd.cut(
        df['temperature_c'],
        bins=[-np.inf, 0, 10, 20, 30, np.inf],
        labels=['Freezing', 'Cold', 'Mild', 'Warm', 'Hot']
    )
    
    # Fill any missing weather data with defaults
    weather_cols = ['temperature_c', 'humidity_pct', 'precipitation_mm', 
                    'rain_mm', 'snowfall_mm', 'wind_speed_kmh']
    for col in weather_cols:
        df[col] = df[col].fillna(df[col].median())
    
    df['weather_code'] = df['weather_code'].fillna(0)
    df['weather_condition'] = df['weather_condition'].fillna('Unknown')
    
    # Drop temporary columns
    df = df.drop(columns=['weather_hour'])
    
    # Log enrichment summary
    rain_trips = df['is_raining'].sum()
    rain_pct = (rain_trips / len(df)) * 100
    logger.info(f"Weather enrichment complete:")
    logger.info(f"  - Trips during rain: {rain_trips:,} ({rain_pct:.1f}%)")
    logger.info(f"  - Temperature range: {df['temperature_c'].min():.1f}°C to {df['temperature_c'].max():.1f}°C")
    
    # Save enriched data
    df.to_csv(ENRICHED_DATA_PATH, index=False)
    logger.info(f"Saved enriched data to {ENRICHED_DATA_PATH}")
    
    return len(df)


if __name__ == '__main__':
    enrich_with_weather()
