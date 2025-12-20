"""
PDF Report Generator - NYC Taxi Trip Duration
Generates a weekly analytics report with visualizations.
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os
from sqlalchemy import create_engine, text
from fpdf import FPDF
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
REPORT_DIR = '/opt/airflow/data/reports'
CHART_DIR = '/opt/airflow/data/reports/charts'

# Database connection
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'airflow')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'airflow')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'airflow')


def get_db_engine():
    """Create SQLAlchemy database engine."""
    connection_string = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(connection_string)


def create_charts(df):
    """Generate matplotlib charts for the report."""
    os.makedirs(CHART_DIR, exist_ok=True)
    chart_paths = {}
    
    # Set style
    plt.style.use('seaborn-v0_8-whitegrid')
    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#3B1F2B']
    
    # =====================
    # CHART 1: Daily Trip Volume
    # =====================
    fig, ax = plt.subplots(figsize=(10, 5))
    daily_trips = df.groupby('pickup_date').size()
    ax.fill_between(daily_trips.index, daily_trips.values, alpha=0.3, color=colors[0])
    ax.plot(daily_trips.index, daily_trips.values, color=colors[0], linewidth=2)
    ax.set_title('Daily Trip Volume', fontsize=14, fontweight='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('Number of Trips')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    plt.xticks(rotation=45)
    plt.tight_layout()
    chart_paths['daily_volume'] = os.path.join(CHART_DIR, 'daily_volume.png')
    plt.savefig(chart_paths['daily_volume'], dpi=150, bbox_inches='tight')
    plt.close()
    
    # =====================
    # CHART 2: Hourly Distribution
    # =====================
    fig, ax = plt.subplots(figsize=(10, 5))
    hourly_trips = df.groupby('pickup_hour').size()
    bars = ax.bar(hourly_trips.index, hourly_trips.values, color=colors[1], alpha=0.8)
    # Highlight rush hours
    for i, bar in enumerate(bars):
        if i in [7, 8, 9, 17, 18, 19]:
            bar.set_color(colors[2])
    ax.set_title('Trips by Hour of Day', fontsize=14, fontweight='bold')
    ax.set_xlabel('Hour')
    ax.set_ylabel('Number of Trips')
    ax.set_xticks(range(24))
    plt.tight_layout()
    chart_paths['hourly'] = os.path.join(CHART_DIR, 'hourly_distribution.png')
    plt.savefig(chart_paths['hourly'], dpi=150, bbox_inches='tight')
    plt.close()
    
    # =====================
    # CHART 3: Weather Impact on Duration
    # =====================
    fig, ax = plt.subplots(figsize=(8, 5))
    weather_duration = df.groupby('weather_condition')['trip_duration_min'].mean().sort_values(ascending=True)
    weather_duration = weather_duration.tail(10)  # Top 10
    bars = ax.barh(weather_duration.index, weather_duration.values, color=colors[0], alpha=0.8)
    ax.set_title('Average Trip Duration by Weather', fontsize=14, fontweight='bold')
    ax.set_xlabel('Average Duration (minutes)')
    plt.tight_layout()
    chart_paths['weather'] = os.path.join(CHART_DIR, 'weather_impact.png')
    plt.savefig(chart_paths['weather'], dpi=150, bbox_inches='tight')
    plt.close()
    
    # =====================
    # CHART 4: Day of Week Pattern
    # =====================
    fig, ax = plt.subplots(figsize=(8, 5))
    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    dow_trips = df.groupby('pickup_dayofweek').size()
    ax.bar(days, dow_trips.values, color=colors[3], alpha=0.8)
    ax.set_title('Trips by Day of Week', fontsize=14, fontweight='bold')
    ax.set_xlabel('Day')
    ax.set_ylabel('Number of Trips')
    plt.tight_layout()
    chart_paths['dow'] = os.path.join(CHART_DIR, 'day_of_week.png')
    plt.savefig(chart_paths['dow'], dpi=150, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Generated {len(chart_paths)} charts")
    return chart_paths


def generate_pdf_report(df, chart_paths):
    """Generate PDF report with charts and metrics."""
    report_date = datetime.now().strftime('%Y-%m-%d')
    report_path = os.path.join(REPORT_DIR, f'taxi_report_{report_date}.pdf')
    
    # Calculate key metrics
    total_trips = len(df)
    avg_duration = df['trip_duration_min'].mean()
    avg_distance = df['trip_distance_km'].mean()
    rain_trips_pct = (df['is_raining'].sum() / len(df)) * 100
    
    # Create PDF
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    # Title Page
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 24)
    pdf.cell(0, 20, 'NYC Taxi Analytics Report', ln=True, align='C')
    pdf.set_font('Helvetica', '', 14)
    pdf.cell(0, 10, f'Generated: {report_date}', ln=True, align='C')
    pdf.ln(20)
    
    # Key Metrics
    pdf.set_font('Helvetica', 'B', 16)
    pdf.cell(0, 10, 'Key Metrics', ln=True)
    pdf.set_font('Helvetica', '', 12)
    pdf.ln(5)
    
    metrics = [
        f"Total Trips Analyzed: {total_trips:,}",
        f"Average Trip Duration: {avg_duration:.1f} minutes",
        f"Average Trip Distance: {avg_distance:.2f} km",
        f"Trips During Rain: {rain_trips_pct:.1f}%",
        f"Date Range: {df['pickup_date'].min()} to {df['pickup_date'].max()}"
    ]
    
    for metric in metrics:
        pdf.cell(0, 8, f"  • {metric}", ln=True)
    
    pdf.ln(10)
    
    # Charts
    pdf.set_font('Helvetica', 'B', 16)
    pdf.cell(0, 10, 'Trip Volume Analysis', ln=True)
    if os.path.exists(chart_paths['daily_volume']):
        pdf.image(chart_paths['daily_volume'], x=10, w=190)
    
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 16)
    pdf.cell(0, 10, 'Hourly Distribution', ln=True)
    if os.path.exists(chart_paths['hourly']):
        pdf.image(chart_paths['hourly'], x=10, w=190)
    
    pdf.ln(10)
    pdf.set_font('Helvetica', 'B', 16)
    pdf.cell(0, 10, 'Day of Week Pattern', ln=True)
    if os.path.exists(chart_paths['dow']):
        pdf.image(chart_paths['dow'], x=10, w=160)
    
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 16)
    pdf.cell(0, 10, 'Weather Impact Analysis', ln=True)
    if os.path.exists(chart_paths['weather']):
        pdf.image(chart_paths['weather'], x=10, w=160)
    
    # Save PDF
    pdf.output(report_path)
    logger.info(f"PDF report saved to {report_path}")
    
    return report_path


def generate_report():
    """Main report generation function using SQL aggregations to save memory."""
    logger.info("Connecting to PostgreSQL...")
    
    engine = get_db_engine()
    os.makedirs(CHART_DIR, exist_ok=True)
    os.makedirs(REPORT_DIR, exist_ok=True)
    
    # Query AGGREGATED data only - much more memory efficient
    logger.info("Querying aggregated metrics from PostgreSQL...")
    
    # Get basic stats
    stats_query = """
    SELECT 
        COUNT(*) as total_trips,
        AVG(trip_duration_min) as avg_duration,
        AVG(trip_distance_km) as avg_distance,
        SUM(CASE WHEN is_raining = 1 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as rain_pct,
        MIN(pickup_date) as min_date,
        MAX(pickup_date) as max_date
    FROM taxi_trips
    """
    stats = pd.read_sql(stats_query, engine).iloc[0]
    
    # Daily trips
    daily_query = """
    SELECT pickup_date, COUNT(*) as trips
    FROM taxi_trips
    GROUP BY pickup_date
    ORDER BY pickup_date
    """
    daily_df = pd.read_sql(daily_query, engine)
    daily_df['pickup_date'] = pd.to_datetime(daily_df['pickup_date'])
    
    # Hourly trips
    hourly_query = """
    SELECT pickup_hour, COUNT(*) as trips
    FROM taxi_trips
    GROUP BY pickup_hour
    ORDER BY pickup_hour
    """
    hourly_df = pd.read_sql(hourly_query, engine)
    
    # Weather impact
    weather_query = """
    SELECT weather_condition, AVG(trip_duration_min) as avg_duration
    FROM taxi_trips
    GROUP BY weather_condition
    ORDER BY avg_duration
    """
    weather_df = pd.read_sql(weather_query, engine)
    
    # Day of week
    dow_query = """
    SELECT pickup_dayofweek, COUNT(*) as trips
    FROM taxi_trips
    GROUP BY pickup_dayofweek
    ORDER BY pickup_dayofweek
    """
    dow_df = pd.read_sql(dow_query, engine)
    
    logger.info(f"Loaded aggregated data. Total trips: {int(stats['total_trips']):,}")
    
    # Create charts with aggregated data
    chart_paths = {}
    plt.style.use('seaborn-v0_8-whitegrid')
    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#3B1F2B']
    
    # Chart 1: Daily Volume
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.fill_between(daily_df['pickup_date'], daily_df['trips'], alpha=0.3, color=colors[0])
    ax.plot(daily_df['pickup_date'], daily_df['trips'], color=colors[0], linewidth=2)
    ax.set_title('Daily Trip Volume', fontsize=14, fontweight='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('Number of Trips')
    plt.xticks(rotation=45)
    plt.tight_layout()
    chart_paths['daily_volume'] = os.path.join(CHART_DIR, 'daily_volume.png')
    plt.savefig(chart_paths['daily_volume'], dpi=150, bbox_inches='tight')
    plt.close()
    
    # Chart 2: Hourly Distribution
    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(hourly_df['pickup_hour'], hourly_df['trips'], color=colors[1], alpha=0.8)
    for i, bar in enumerate(bars):
        if i in [7, 8, 9, 17, 18, 19]:
            bar.set_color(colors[2])
    ax.set_title('Trips by Hour of Day', fontsize=14, fontweight='bold')
    ax.set_xlabel('Hour')
    ax.set_ylabel('Number of Trips')
    ax.set_xticks(range(24))
    plt.tight_layout()
    chart_paths['hourly'] = os.path.join(CHART_DIR, 'hourly_distribution.png')
    plt.savefig(chart_paths['hourly'], dpi=150, bbox_inches='tight')
    plt.close()
    
    # Chart 3: Weather Impact
    fig, ax = plt.subplots(figsize=(8, 5))
    weather_top = weather_df.tail(10)
    ax.barh(weather_top['weather_condition'], weather_top['avg_duration'], color=colors[0], alpha=0.8)
    ax.set_title('Average Trip Duration by Weather', fontsize=14, fontweight='bold')
    ax.set_xlabel('Average Duration (minutes)')
    plt.tight_layout()
    chart_paths['weather'] = os.path.join(CHART_DIR, 'weather_impact.png')
    plt.savefig(chart_paths['weather'], dpi=150, bbox_inches='tight')
    plt.close()
    
    # Chart 4: Day of Week
    fig, ax = plt.subplots(figsize=(8, 5))
    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    ax.bar(days, dow_df['trips'], color=colors[3], alpha=0.8)
    ax.set_title('Trips by Day of Week', fontsize=14, fontweight='bold')
    ax.set_xlabel('Day')
    ax.set_ylabel('Number of Trips')
    plt.tight_layout()
    chart_paths['dow'] = os.path.join(CHART_DIR, 'day_of_week.png')
    plt.savefig(chart_paths['dow'], dpi=150, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Generated {len(chart_paths)} charts")
    
    # Generate PDF
    report_date = datetime.now().strftime('%Y-%m-%d')
    report_path = os.path.join(REPORT_DIR, f'taxi_report_{report_date}.pdf')
    
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    # Title Page
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 24)
    pdf.cell(0, 20, 'NYC Taxi Analytics Report', ln=True, align='C')
    pdf.set_font('Helvetica', '', 14)
    pdf.cell(0, 10, f'Generated: {report_date}', ln=True, align='C')
    pdf.ln(20)
    
    # Key Metrics
    pdf.set_font('Helvetica', 'B', 16)
    pdf.cell(0, 10, 'Key Metrics', ln=True)
    pdf.set_font('Helvetica', '', 12)
    pdf.ln(5)
    
    metrics = [
        f"Total Trips Analyzed: {int(stats['total_trips']):,}",
        f"Average Trip Duration: {stats['avg_duration']:.1f} minutes",
        f"Average Trip Distance: {stats['avg_distance']:.2f} km",
        f"Trips During Rain: {stats['rain_pct']:.1f}%",
        f"Date Range: {stats['min_date']} to {stats['max_date']}"
    ]
    
    for metric in metrics:
        pdf.cell(0, 8, f"  * {metric}", ln=True)
    
    pdf.ln(10)
    
    # Add charts
    pdf.set_font('Helvetica', 'B', 16)
    pdf.cell(0, 10, 'Trip Volume Analysis', ln=True)
    if os.path.exists(chart_paths['daily_volume']):
        pdf.image(chart_paths['daily_volume'], x=10, w=190)
    
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 16)
    pdf.cell(0, 10, 'Hourly Distribution', ln=True)
    if os.path.exists(chart_paths['hourly']):
        pdf.image(chart_paths['hourly'], x=10, w=190)
    
    pdf.ln(10)
    pdf.set_font('Helvetica', 'B', 16)
    pdf.cell(0, 10, 'Day of Week Pattern', ln=True)
    if os.path.exists(chart_paths['dow']):
        pdf.image(chart_paths['dow'], x=10, w=160)
    
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 16)
    pdf.cell(0, 10, 'Weather Impact Analysis', ln=True)
    if os.path.exists(chart_paths['weather']):
        pdf.image(chart_paths['weather'], x=10, w=160)
    
    pdf.output(report_path)
    logger.info(f"PDF report saved to {report_path}")
    
    return report_path


if __name__ == '__main__':
    generate_report()

