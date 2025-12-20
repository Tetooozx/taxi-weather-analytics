"""
Machine Learning Model Training - NYC Taxi Trip Duration Prediction
Trains a Random Forest model to predict trip duration.
"""
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import pickle
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
ENRICHED_DATA_PATH = '/opt/airflow/data/processed/enriched_taxi_data.csv'
MODEL_PATH = '/opt/airflow/models/trip_duration_model.pkl'
METRICS_PATH = '/opt/airflow/models/model_metrics.txt'


def train_model():
    """Train trip duration prediction model."""
    logger.info(f"Loading enriched data from {ENRICHED_DATA_PATH}")
    
    # Load data
    df = pd.read_csv(ENRICHED_DATA_PATH)
    logger.info(f"Loaded {len(df):,} records for training")
    
    # Sample if dataset is too large (for faster training)
    if len(df) > 500000:
        df = df.sample(n=500000, random_state=42)
        logger.info(f"Sampled down to {len(df):,} records")
    
    # =====================
    # FEATURE SELECTION
    # =====================
    
    feature_columns = [
        # Geographic features
        'pickup_latitude', 'pickup_longitude',
        'dropoff_latitude', 'dropoff_longitude',
        'trip_distance_km',
        
        # Time features
        'pickup_hour', 'pickup_dayofweek', 'pickup_month',
        'is_weekend', 'is_rush_hour',
        
        # Passenger info
        'passenger_count', 'vendor_id',
        
        # Weather features
        'temperature_c', 'humidity_pct', 'precipitation_mm',
        'wind_speed_kmh', 'is_raining', 'is_bad_weather'
    ]
    
    # Ensure all features exist
    available_features = [col for col in feature_columns if col in df.columns]
    logger.info(f"Using {len(available_features)} features: {available_features}")
    
    X = df[available_features].copy()
    y = df['trip_duration'].copy()  # Target in seconds
    
    # Handle any remaining missing values
    X = X.fillna(X.median())
    
    # =====================
    # TRAIN/TEST SPLIT
    # =====================
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    logger.info(f"Training set: {len(X_train):,} samples")
    logger.info(f"Test set: {len(X_test):,} samples")
    
    # =====================
    # MODEL TRAINING
    # =====================
    
    logger.info("Training Random Forest model...")
    
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=15,
        min_samples_split=10,
        min_samples_leaf=5,
        n_jobs=-1,
        random_state=42,
        verbose=1
    )
    
    model.fit(X_train, y_train)
    logger.info("Model training complete!")
    
    # =====================
    # EVALUATION
    # =====================
    
    logger.info("Evaluating model performance...")
    
    y_pred = model.predict(X_test)
    
    # Metrics
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    
    # Convert to minutes for interpretability
    mae_min = mae / 60
    rmse_min = rmse / 60
    
    logger.info("=" * 50)
    logger.info("MODEL PERFORMANCE METRICS")
    logger.info("=" * 50)
    logger.info(f"Mean Absolute Error: {mae_min:.2f} minutes")
    logger.info(f"Root Mean Squared Error: {rmse_min:.2f} minutes")
    logger.info(f"R² Score: {r2:.4f}")
    logger.info("=" * 50)
    
    # Feature importance
    feature_importance = pd.DataFrame({
        'feature': available_features,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    logger.info("\nTop 10 Feature Importances:")
    for _, row in feature_importance.head(10).iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.4f}")
    
    # =====================
    # SAVE MODEL & METRICS
    # =====================
    
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    
    # Save model
    model_data = {
        'model': model,
        'features': available_features,
        'metrics': {
            'mae_seconds': mae,
            'mae_minutes': mae_min,
            'rmse_seconds': rmse,
            'rmse_minutes': rmse_min,
            'r2_score': r2
        },
        'feature_importance': feature_importance.to_dict()
    }
    
    with open(MODEL_PATH, 'wb') as f:
        pickle.dump(model_data, f)
    
    logger.info(f"Model saved to {MODEL_PATH}")
    
    # Save metrics as text file
    with open(METRICS_PATH, 'w') as f:
        f.write("NYC Taxi Trip Duration Prediction Model\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Training Date: {pd.Timestamp.now()}\n")
        f.write(f"Training Samples: {len(X_train):,}\n")
        f.write(f"Test Samples: {len(X_test):,}\n\n")
        f.write("METRICS:\n")
        f.write(f"  Mean Absolute Error: {mae_min:.2f} minutes\n")
        f.write(f"  Root Mean Squared Error: {rmse_min:.2f} minutes\n")
        f.write(f"  R² Score: {r2:.4f}\n\n")
        f.write("TOP FEATURES:\n")
        for _, row in feature_importance.head(10).iterrows():
            f.write(f"  {row['feature']}: {row['importance']:.4f}\n")
    
    logger.info(f"Metrics saved to {METRICS_PATH}")
    
    return r2


if __name__ == '__main__':
    train_model()
