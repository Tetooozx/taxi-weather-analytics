"""
NYC Taxi ETL Pipeline - Apache Airflow DAG
Orchestrates the complete data processing, enrichment, ML training, and reporting pipeline.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
import sys
import os

# Add scripts directory to path
sys.path.insert(0, '/opt/airflow/scripts')

# Import our processing functions
from process_data import process_data
from enrich_weather import enrich_with_weather
from train_model import train_model
from load_to_postgres import load_to_postgres
from generate_report import generate_report
from notify_slack import send_slack_notification

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Define the DAG
with DAG(
    dag_id='nyc_taxi_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for NYC Taxi data with weather enrichment, ML, and Power BI integration',
    schedule_interval=None,  # Manual trigger or set to '@daily' for scheduled runs
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'taxi', 'ml', 'analytics'],
    doc_md="""
    # NYC Taxi ETL Pipeline
    
    This DAG processes NYC taxi trip data through the following stages:
    
    1. **Data Arrival Check**: Monitors for new data files
    2. **Data Processing**: Cleans and filters invalid trips
    3. **Weather Enrichment**: Adds weather data from Open-Meteo API
    4. **ML Training**: Trains trip duration prediction model
    5. **PostgreSQL Load**: Stores data for Power BI dashboards
    6. **Report Generation**: Creates PDF analytics report
    7. **Notification**: Sends Slack notification on completion
    
    ## Data Source
    NYC Taxi Trip Duration dataset from Kaggle
    
    ## Outputs
    - Cleaned dataset in PostgreSQL (for Power BI)
    - Trained ML model (pickle file)
    - PDF analytics report
    - Slack notification
    """
) as dag:
    
    # =====================
    # TASK 1: Check for Data Arrival
    # =====================
    check_data_arrival = FileSensor(
        task_id='check_data_arrival',
        filepath='/opt/airflow/data/raw/train.csv',
        poke_interval=30,  # Check every 30 seconds
        timeout=60 * 5,    # Timeout after 5 minutes
        mode='poke',
        soft_fail=False,
        doc_md="Monitors the raw data directory for the taxi dataset file."
    )
    
    # =====================
    # TASK 2: Process Data
    # =====================
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        doc_md="""
        Cleans and processes raw taxi data:
        - Removes trips < 60 seconds
        - Removes trips > 24 hours
        - Filters invalid coordinates
        - Extracts time features (hour, day, weekend)
        - Calculates trip distance using Haversine formula
        """
    )
    
    # =====================
    # TASK 3: Enrich with Weather Data
    # =====================
    enrich_weather_task = PythonOperator(
        task_id='enrich_weather',
        python_callable=enrich_with_weather,
        doc_md="""
        Enriches taxi data with historical weather:
        - Fetches weather from Open-Meteo API
        - Adds temperature, precipitation, wind speed
        - Creates weather condition flags (is_raining, is_bad_weather)
        """
    )
    
    # =====================
    # TASK 4: Train ML Model
    # =====================
    train_model_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
        doc_md="""
        Trains a Random Forest model for trip duration prediction:
        - Uses geographic, temporal, and weather features
        - Saves model to /models/trip_duration_model.pkl
        - Logs feature importance and metrics
        """
    )
    
    # =====================
    # TASK 5: Load to PostgreSQL
    # =====================
    load_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        doc_md="""
        Loads enriched data to PostgreSQL:
        - Creates taxi_trips table
        - Bulk inserts data with proper types
        - Creates indexes for query performance
        - Ready for Power BI connection
        """
    )
    
    # =====================
    # TASK 6: Generate Report
    # =====================
    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        doc_md="""
        Generates PDF analytics report:
        - Daily trip volume chart
        - Hourly distribution analysis
        - Weather impact visualization
        - Key metrics summary
        """
    )
    
    # =====================
    # TASK 7: Send Slack Notification
    # =====================
    def notify_success(**context):
        """Send success notification via Slack."""
        send_slack_notification(success=True)
    
    def notify_failure(context):
        """Send failure notification via Slack."""
        error_msg = str(context.get('exception', 'Unknown error'))
        send_slack_notification(success=False, error_message=error_msg)
    
    notify_slack_task = PythonOperator(
        task_id='notify_slack',
        python_callable=notify_success,
        trigger_rule='all_success',
        doc_md="Sends Slack notification on pipeline completion."
    )
    
    # =====================
    # TASK DEPENDENCIES
    # =====================
    # Define the pipeline flow
    (
        check_data_arrival 
        >> process_data_task 
        >> enrich_weather_task 
        >> train_model_task 
        >> load_postgres_task 
        >> generate_report_task 
        >> notify_slack_task
    )
