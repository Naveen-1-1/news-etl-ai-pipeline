"""
Configuration settings for News ETL Pipeline
"""
import os
from datetime import datetime

# API Configuration
NEWS_API_QUERY = "AI"  # Search query for news articles
NEWS_API_LANGUAGE = "en"  # Language filter

# Database Configuration
DB_NAME = "etl_news_data.sqlite"
DB_TABLE_NAME = "news_table"

# Airflow DAG Configuration
DAG_ID = "news_etl_pipeline"
DAG_SCHEDULE = "@daily"  # Run daily at midnight
DAG_START_DATE = datetime(2024, 1, 1)
DAG_RETRIES = 2

# Logging Configuration
LOG_LEVEL = "INFO"
