"""
PostgreSQL Data Loader - NYC Taxi Trip Duration
Loads enriched taxi data into PostgreSQL for Power BI visualization.
"""
import pandas as pd
import os
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Float, String, DateTime, Date
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
ENRICHED_DATA_PATH = '/opt/airflow/data/processed/enriched_taxi_data.csv'

# Database connection from environment variables
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'airflow')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'airflow')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'airflow')

TABLE_NAME = 'taxi_trips'


def get_db_engine():
    """Create SQLAlchemy database engine."""
    connection_string = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(connection_string)


def load_to_postgres():
    """Load enriched data to PostgreSQL."""
    logger.info(f"Loading enriched data from {ENRICHED_DATA_PATH}")
    
    # Create database connection
    engine = get_db_engine()
    
    # Drop existing table first
    logger.info(f"Dropping existing table '{TABLE_NAME}' if exists...")
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))
    
    # Load data to PostgreSQL in chunks to save memory
    logger.info("Loading data to PostgreSQL in chunks (this may take a few minutes)...")
    
    # Read and load in chunks of 50,000 rows
    chunk_size = 50000
    total_rows = 0
    first_chunk = True
    
    for chunk_num, chunk in enumerate(pd.read_csv(ENRICHED_DATA_PATH, chunksize=chunk_size)):
        # Convert datetime columns
        chunk['pickup_datetime'] = pd.to_datetime(chunk['pickup_datetime'])
        if 'dropoff_datetime' in chunk.columns:
            chunk['dropoff_datetime'] = pd.to_datetime(chunk['dropoff_datetime'])
        chunk['pickup_date'] = pd.to_datetime(chunk['pickup_date'])
        
        # Define column types for PostgreSQL (only for columns that exist)
        dtype_mapping = {
            'id': String(50),
            'vendor_id': Integer,
            'pickup_datetime': DateTime,
            'dropoff_datetime': DateTime,
            'passenger_count': Integer,
            'pickup_longitude': Float,
            'pickup_latitude': Float,
            'dropoff_longitude': Float,
            'dropoff_latitude': Float,
            'store_and_fwd_flag': String(5),
            'trip_duration': Integer,
            'pickup_hour': Integer,
            'pickup_day': Integer,
            'pickup_month': Integer,
            'pickup_dayofweek': Integer,
            'pickup_date': Date,
            'is_weekend': Integer,
            'is_rush_hour': Integer,
            'trip_distance_km': Float,
            'avg_speed_kmh': Float,
            'trip_duration_min': Float,
            'temperature_c': Float,
            'humidity_pct': Float,
            'precipitation_mm': Float,
            'rain_mm': Float,
            'snowfall_mm': Float,
            'wind_speed_kmh': Float,
            'weather_code': Integer,
            'is_raining': Integer,
            'is_snowing': Integer,
            'is_bad_weather': Integer,
            'weather_condition': String(50),
            'temp_category': String(20)
        }
        final_dtypes = {k: v for k, v in dtype_mapping.items() if k in chunk.columns}
        
        # Write chunk to database
        if_exists_mode = 'replace' if first_chunk else 'append'
        chunk.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists=if_exists_mode,
            index=False,
            dtype=final_dtypes if first_chunk else None,
            chunksize=5000
        )
        
        total_rows += len(chunk)
        first_chunk = False
        logger.info(f"  Loaded chunk {chunk_num + 1}: {total_rows:,} total rows so far...")
    
    logger.info(f"Finished loading {total_rows:,} records to PostgreSQL")
    
    # Create indexes for better query performance
    logger.info("Creating indexes for better query performance...")
    
    with engine.begin() as conn:
        # Index on pickup datetime for time-series queries
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_pickup_datetime ON {TABLE_NAME} (pickup_datetime)"))
        
        # Index on date for daily aggregations
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_pickup_date ON {TABLE_NAME} (pickup_date)"))
        
        # Index on weather condition for weather analysis
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_weather ON {TABLE_NAME} (is_raining, is_bad_weather)"))
    
    # Verify data was loaded
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
        count = result.fetchone()[0]
    
    logger.info("=" * 50)
    logger.info("POSTGRESQL LOAD COMPLETE")
    logger.info("=" * 50)
    logger.info(f"Table: {TABLE_NAME}")
    logger.info(f"Records loaded: {count:,}")
    logger.info(f"Connection: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    logger.info("=" * 50)
    logger.info("")
    logger.info("POWER BI CONNECTION SETTINGS:")
    logger.info(f"  Server: localhost (or host.docker.internal)")
    logger.info(f"  Port: 5432")
    logger.info(f"  Database: {POSTGRES_DB}")
    logger.info(f"  Username: {POSTGRES_USER}")
    logger.info(f"  Table: {TABLE_NAME}")
    logger.info("=" * 50)
    
    return count


if __name__ == '__main__':
    load_to_postgres()
