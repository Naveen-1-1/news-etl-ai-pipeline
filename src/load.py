"""
Load module - Store transformed data in SQLite database
"""
import pandas as pd
import sqlite3
import logging
import os


def load_news_data(**kwargs):
    """
    Load transformed news data into SQLite database.
    
    Creates table if not exists and appends new records.
    
    Args:
        **kwargs: Airflow context including task_instance for XCom
    """
    # Define database path
    db_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data",
        "etl_news_data.sqlite"
    )
    
    # Ensure data directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()
        
        # Create table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS news_table (
                "Source" VARCHAR(100),
                "Author Name" TEXT,
                "News Title" TEXT,
                "URL" TEXT PRIMARY KEY,
                "Date Published" TEXT,
                "Content" TEXT
            )
        ''')
        
        # Pull transformed data from XCom
        data = kwargs['task_instance'].xcom_pull(
            task_ids='transform_news', 
            key='transform_df'
        )
        
        # Convert JSON back to DataFrame
        df = pd.read_json(data)
        
        logging.info(f"Loading {len(df)} articles into database at {db_path}")
        
        # Load data into database (append mode)
        df.to_sql(
            name="news_table", 
            con=connection, 
            index=False, 
            if_exists="append"
        )
        
        # Get total record count
        cursor.execute("SELECT COUNT(*) FROM news_table")
        total_records = cursor.fetchone()[0]
        
        logging.info(f"Data successfully loaded. Total records in database: {total_records}")
