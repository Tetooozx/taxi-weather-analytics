"""
Data Processing Script - NYC Taxi Trip Duration
Cleans raw data, removes invalid trips, and extracts features.
"""
import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
RAW_DATA_PATH = '/opt/airflow/data/raw/train.csv'
PROCESSED_DATA_PATH = '/opt/airflow/data/processed/cleaned_taxi_data.csv'


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points on Earth.
    Returns distance in kilometers.
    """
    R = 6371  # Earth's radius in kilometers
    
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    return R * c


def process_data():
    """Main data processing function."""
    logger.info(f"Loading raw data from {RAW_DATA_PATH}")
    
    # Load data
    df = pd.read_csv(RAW_DATA_PATH)
    initial_count = len(df)
    logger.info(f"Loaded {initial_count:,} records")
    
    # Convert datetime columns
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    if 'dropoff_datetime' in df.columns:
        df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
    
    # =====================
    # DATA QUALITY FILTERS
    # =====================
    
    # Filter 1: Remove trips shorter than 60 seconds (bad data / test trips)
    df = df[df['trip_duration'] >= 60]
    logger.info(f"After removing trips < 60 seconds: {len(df):,} records")
    
    # Filter 2: Remove trips longer than 24 hours (likely errors)
    df = df[df['trip_duration'] <= 86400]
    logger.info(f"After removing trips > 24 hours: {len(df):,} records")
    
    # Filter 3: Remove invalid coordinates (outside NYC area)
    nyc_bounds = {
        'lat_min': 40.4, 'lat_max': 41.0,
        'lon_min': -74.3, 'lon_max': -73.7
    }
    df = df[
        (df['pickup_latitude'] >= nyc_bounds['lat_min']) & 
        (df['pickup_latitude'] <= nyc_bounds['lat_max']) &
        (df['pickup_longitude'] >= nyc_bounds['lon_min']) & 
        (df['pickup_longitude'] <= nyc_bounds['lon_max']) &
        (df['dropoff_latitude'] >= nyc_bounds['lat_min']) & 
        (df['dropoff_latitude'] <= nyc_bounds['lat_max']) &
        (df['dropoff_longitude'] >= nyc_bounds['lon_min']) & 
        (df['dropoff_longitude'] <= nyc_bounds['lon_max'])
    ]
    logger.info(f"After removing invalid coordinates: {len(df):,} records")
    
    # Filter 4: Remove zero passenger trips
    df = df[df['passenger_count'] > 0]
    logger.info(f"After removing zero passengers: {len(df):,} records")
    
    # =====================
    # FEATURE ENGINEERING
    # =====================
    
    # Time-based features
    df['pickup_hour'] = df['pickup_datetime'].dt.hour
    df['pickup_day'] = df['pickup_datetime'].dt.day
    df['pickup_month'] = df['pickup_datetime'].dt.month
    df['pickup_dayofweek'] = df['pickup_datetime'].dt.dayofweek
    df['pickup_date'] = df['pickup_datetime'].dt.date
    df['is_weekend'] = df['pickup_dayofweek'].isin([5, 6]).astype(int)
    
    # Rush hour indicator (7-9 AM or 4-7 PM on weekdays)
    df['is_rush_hour'] = (
        ((df['pickup_hour'].between(7, 9)) | (df['pickup_hour'].between(16, 19))) &
        (~df['is_weekend'].astype(bool))
    ).astype(int)
    
    # Calculate trip distance using Haversine formula
    df['trip_distance_km'] = haversine_distance(
        df['pickup_latitude'], df['pickup_longitude'],
        df['dropoff_latitude'], df['dropoff_longitude']
    )
    
    # Average speed (km/h)
    df['avg_speed_kmh'] = (df['trip_distance_km'] / df['trip_duration']) * 3600
    
    # Trip duration in minutes (for easier interpretation)
    df['trip_duration_min'] = df['trip_duration'] / 60
    
    # =====================
    # FINAL CLEANUP
    # =====================
    
    # Remove extreme outliers in speed (likely GPS errors)
    df = df[df['avg_speed_kmh'] <= 100]  # Max 100 km/h average
    df = df[df['avg_speed_kmh'] >= 0.5]  # Min 0.5 km/h (standing still)
    logger.info(f"After removing speed outliers: {len(df):,} records")
    
    # Calculate data quality metrics
    records_removed = initial_count - len(df)
    pct_removed = (records_removed / initial_count) * 100
    logger.info(f"Data Quality Summary: Removed {records_removed:,} records ({pct_removed:.1f}%)")
    
    # Save processed data
    os.makedirs(os.path.dirname(PROCESSED_DATA_PATH), exist_ok=True)
    df.to_csv(PROCESSED_DATA_PATH, index=False)
    logger.info(f"Saved processed data to {PROCESSED_DATA_PATH}")
    
    return len(df)


if __name__ == '__main__':
    process_data()
