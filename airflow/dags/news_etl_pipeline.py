"""
Airflow DAG for News ETL Pipeline

This DAG orchestrates the Extract-Transform-Load process for news articles:
1. Extract: Fetch news articles from NewsAPI
2. Transform: Clean and structure the data
3. Load: Store in SQLite database
"""
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from src.extract import extract_news_data
from src.transform import transform_news_data
from src.load import load_news_data
from config.config import DAG_ID, DAG_SCHEDULE, DAG_START_DATE, DAG_RETRIES


# Default arguments for the DAG
default_args = {
    'owner': 'naveen',
    'depends_on_past': False,
    'start_date': DAG_START_DATE,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': DAG_RETRIES,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='ETL pipeline to extract AI news from NewsAPI and store in SQLite',
    schedule=DAG_SCHEDULE,
    catchup=False,
    tags=['news', 'etl', 'newsapi'],
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_news',
    python_callable=extract_news_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_news',
    python_callable=transform_news_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_news',
    python_callable=load_news_data,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
