"""
Configuration settings for News ETL Pipeline
"""
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API Configuration
NEWS_API_QUERY = "AI"  # Search query for news articles
NEWS_API_LANGUAGE = "en"  # Language filter

# Database Configuration - PostgreSQL
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "news_etl_db")
DB_USER = os.getenv("DB_USER", "admin")  # Changed default to admin
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")  # Changed default to admin
DB_TABLE_NAME = "news_table"

# Construct PostgreSQL connection string (using psycopg3 driver)
DATABASE_URI = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Airflow DAG Configuration
DAG_ID = "news_etl_pipeline"
DAG_SCHEDULE = "@daily"  # Run daily at midnight
DAG_START_DATE = datetime(2024, 1, 1)
DAG_RETRIES = 2

# Logging Configuration
LOG_LEVEL = "INFO"
