# NYC Taxi ETL Pipeline with Apache Airflow

A production-ready ETL pipeline that processes 1.4M+ NYC taxi trip records, enriches them with weather data, trains an ML model, and visualizes insights in Power BI.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Pipeline Tasks](#pipeline-tasks)
- [Power BI Dashboard](#power-bi-dashboard)
- [Data Schema](#data-schema)
- [Outputs](#outputs)

---

## Overview

This project demonstrates a complete data engineering workflow including:

- **Orchestration**: Apache Airflow DAG with 7 automated tasks
- **Data Processing**: Cleaning, validation, and feature engineering
- **Weather Enrichment**: Integration with Open-Meteo API for historical weather data
- **Machine Learning**: Random Forest model for trip duration prediction
- **Data Warehousing**: PostgreSQL database optimized for analytics
- **Reporting**: Automated PDF report generation with matplotlib charts
- **Visualization**: 3-page Power BI dashboard with interactive analytics

---

## Architecture

```
                        Docker Compose Environment
+---------------------------------------------------------------------+
|                                                                     |
|  +-------------+    +-------------+    +-------------+              |
|  |   Airflow   |    |   Airflow   |    |   Airflow   |              |
|  |  Webserver  |    |  Scheduler  |    |  Triggerer  |              |
|  |    :8080    |    |             |    |             |              |
|  +-------------+    +-------------+    +-------------+              |
|         |                  |                  |                     |
|         +------------------+------------------+                     |
|                            |                                        |
|                   +--------v--------+                               |
|                   |   PostgreSQL    |<---- Power BI Desktop         |
|                   |      :5432      |      (External)               |
|                   |   taxi_trips    |                               |
|                   +-----------------+                               |
|                                                                     |
+---------------------------------------------------------------------+

                          Pipeline Flow
                          
+--------+   +--------+   +--------+   +--------+   +--------+
| Check  |-->| Process|-->| Enrich |-->| Train  |-->|  Load  |
| Data   |   | Data   |   | Weather|   | Model  |   |Postgres|
+--------+   +--------+   +--------+   +--------+   +--------+
                                                         |
                          +------------------------------+
                          |                              |
                   +------v------+                +------v------+
                   |  Generate   |                |   Notify    |
                   |   Report    |                |   Slack     |
                   +-------------+                +-------------+
```

---

## Features

### Data Processing
- Remove invalid trips (less than 60 seconds or over 24 hours)
- Filter out-of-bounds coordinates (non-NYC locations)
- Handle zero-passenger trips
- Feature engineering: time-based features, Haversine distance, speed calculations

### Weather Enrichment
- Historical weather from Open-Meteo API (free, no API key required)
- Temperature, humidity, precipitation, wind speed data
- Weather condition decoding (Clear, Rain, Snow, etc.)
- Derived features: is_raining, is_bad_weather, temp_category

### Machine Learning
- Random Forest Regressor for trip duration prediction
- Features: geographic, temporal, passenger, weather
- Model evaluation: MAE, RMSE, R-squared metrics
- Feature importance analysis

### Analytics and Reporting
- Automated PDF report with matplotlib visualizations
- 3-page Power BI dashboard
- Slack notifications for pipeline status

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Apache Airflow 2.7.3 |
| Containerization | Docker, Docker Compose |
| Database | PostgreSQL 15 |
| Language | Python 3.10 |
| Data Processing | Pandas, NumPy |
| Machine Learning | Scikit-learn |
| Visualization | Matplotlib, Power BI Desktop |
| PDF Generation | FPDF2 |
| Weather API | Open-Meteo (free) |
| Notifications | Slack Webhooks |

---

## Project Structure

```
Orchestrated_ETLPipeline/
├── dags/
│   └── taxi_etl_pipeline.py    # Main Airflow DAG (7 tasks)
├── scripts/
│   ├── process_data.py         # Data cleaning and feature engineering
│   ├── enrich_weather.py       # Weather API integration
│   ├── train_model.py          # ML model training
│   ├── load_to_postgres.py     # Database loader (chunked)
│   ├── generate_report.py      # PDF report generator
│   └── notify_slack.py         # Slack notifications
├── data/
│   ├── raw/                    # Input: train.csv from Kaggle
│   ├── processed/              # Intermediate CSV files
│   └── reports/                # Output: PDF reports
├── models/                     # Saved ML models (.pkl)
├── logs/                       # Airflow logs
├── docker-compose.yaml         # Docker services configuration
├── Dockerfile                  # Custom Airflow image
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables
└── README.md                   # This file
```

---

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Power BI Desktop (Windows)
- 8GB+ RAM recommended

### Step 1: Clone and Setup

```bash
cd Orchestrated_ETLPipeline
```

### Step 2: Download Dataset

Download `train.csv` from the Kaggle NYC Taxi Trip Duration competition:
https://www.kaggle.com/c/nyc-taxi-trip-duration/data

Place the file in:

```
data/raw/train.csv
```

### Step 3: Start Docker

```bash
docker-compose up -d
```

### Step 4: Access Airflow

Open http://localhost:8080

- Username: `admin`
- Password: `admin`

### Step 5: Add Filesystem Connection

Run this command to add the required filesystem connection:

```bash
docker exec airflow-scheduler airflow connections add 'fs_default' \
  --conn-type 'fs' --conn-extra '{"path": "/"}'
```

### Step 6: Trigger Pipeline

1. Toggle ON the `nyc_taxi_etl_pipeline` DAG
2. Click the play button to trigger

### Step 7: Connect Power BI

| Setting | Value |
|---------|-------|
| Server | localhost |
| Port | 5432 |
| Database | airflow |
| Username | airflow |
| Password | airflow |
| Table | taxi_trips |

---

## Pipeline Tasks

| Task | Description | Approx Duration |
|------|-------------|-----------------|
| check_data_arrival | FileSensor waits for train.csv | 1 second |
| process_data | Clean data and feature engineering | 2-3 minutes |
| enrich_weather | Fetch weather from Open-Meteo API | 1-2 minutes |
| train_model | Train Random Forest model | 3-5 minutes |
| load_to_postgres | Load 1.4M records to database | 2-3 minutes |
| generate_report | Create PDF with charts | 30 seconds |
| notify_slack | Send completion notification | 1 second |

Total pipeline duration is approximately 10-15 minutes.

---

## Power BI Dashboard

### Page 1: Executive Overview
- Total Trips: 1,440,469
- Average Duration: 14.15 minutes
- Average Distance: 3.46 km
- Daily trip volume trend
- Hourly distribution with rush hour highlighted
- Weekend vs Weekday split (71.5% / 28.5%)

### Page 2: Weather Impact Analysis
- Trip duration by weather condition
- Rain vs No-Rain comparison
- Speed vs Temperature correlation
- Rainy day trips: 149,453 (10.4%)
- Bad weather percentage: 12.46%

### Page 3: Trip Patterns
- Day x Hour heatmap matrix
- Day of week distribution
- Trip duration histogram
- Distance vs Duration scatter plot

---

## Data Schema

### taxi_trips Table (PostgreSQL)

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR(50) | Unique trip identifier |
| vendor_id | INTEGER | Taxi vendor |
| pickup_datetime | TIMESTAMP | Trip start time |
| dropoff_datetime | TIMESTAMP | Trip end time |
| passenger_count | INTEGER | Number of passengers |
| pickup_longitude | FLOAT | Pickup location |
| pickup_latitude | FLOAT | Pickup location |
| dropoff_longitude | FLOAT | Dropoff location |
| dropoff_latitude | FLOAT | Dropoff location |
| trip_duration | INTEGER | Duration in seconds |
| trip_duration_min | FLOAT | Duration in minutes |
| trip_distance_km | FLOAT | Haversine distance |
| avg_speed_kmh | FLOAT | Average speed |
| pickup_hour | INTEGER | Hour (0-23) |
| pickup_day | INTEGER | Day of month |
| pickup_month | INTEGER | Month (1-12) |
| pickup_dayofweek | INTEGER | Day of week (0=Monday) |
| is_weekend | INTEGER | Weekend flag (0/1) |
| is_rush_hour | INTEGER | Rush hour flag (0/1) |
| temperature_c | FLOAT | Temperature in Celsius |
| humidity_pct | FLOAT | Humidity percentage |
| precipitation_mm | FLOAT | Precipitation in mm |
| wind_speed_kmh | FLOAT | Wind speed in km/h |
| weather_condition | VARCHAR(50) | Clear, Rain, Snow, etc. |
| is_raining | INTEGER | Rain flag (0/1) |
| is_bad_weather | INTEGER | Bad weather flag (0/1) |
| temp_category | VARCHAR(20) | Cold/Cool/Warm/Hot |

---

## Outputs

After successful pipeline execution:

| Output | Location |
|--------|----------|
| Processed Data | data/processed/enriched_taxi_data.csv |
| ML Model | models/trip_duration_model.pkl |
| Model Metrics | models/model_metrics.json |
| PDF Report | data/reports/taxi_report_YYYY-MM-DD.pdf |
| Database Table | PostgreSQL taxi_trips (1.44M rows) |

---

## Common Commands

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# View logs
docker-compose logs -f airflow-scheduler

# Restart services
docker-compose restart

# Rebuild after code changes
docker-compose build --no-cache

# Access PostgreSQL
docker exec -it postgres psql -U airflow -d airflow

# Check table row count
docker exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM taxi_trips;"
```

---

## License

This project is for educational and portfolio purposes.

---

## Acknowledgments

- NYC Taxi Trip Duration Dataset
- Open-Meteo - Free Weather API
- Apache Airflow - Workflow Orchestration
