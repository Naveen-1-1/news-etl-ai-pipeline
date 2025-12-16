"""
Airflow DAG for News ETL Pipeline

This DAG orchestrates the Extract-Transform-Load process for news articles:
1. Extract: Fetch news articles from NewsAPI
2. Transform: Clean and structure the data
3. Load: Store in PostgreSQL database
"""
import sys
import os
from datetime import datetime, timedelta

# Set up Python path for imports
# Docker: dags at /opt/airflow/dags, src at /opt/airflow/src
# Local: dags at /project/airflow/dags, src at /project/src
dag_file_dir = os.path.dirname(os.path.abspath(__file__))

# Go up from /opt/airflow/dags or /project/airflow/dags to get to base directory
if '/opt/airflow/' in dag_file_dir:
    # Running in Docker - add /opt/airflow to path
    sys.path.insert(0, '/opt/airflow')
else:
    # Running locally - go up two levels
    project_root = os.path.dirname(os.path.dirname(dag_file_dir))
    sys.path.insert(0, project_root)

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
    description='ETL pipeline to extract AI news from NewsAPI and store in PostgreSQL',
    schedule=DAG_SCHEDULE,
    catchup=False,
    tags=['news', 'etl', 'newsapi', 'docker'],
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
