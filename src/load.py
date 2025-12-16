"""
Load module - Store transformed data in PostgreSQL database
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.config import DATABASE_URI, DB_TABLE_NAME


def load_news_data(**kwargs):
    """
    Load transformed news data into PostgreSQL database.
    
    Skips duplicate URLs gracefully using INSERT ... ON CONFLICT DO NOTHING.
    
    Args:
        **kwargs: Airflow context including task_instance for XCom
    """
    try:
        # Create SQLAlchemy engine
        engine = create_engine(DATABASE_URI)
        
        logging.info(f"Connected to PostgreSQL database successfully")
        
        # Pull transformed data from XCom
        data = kwargs['task_instance'].xcom_pull(
            task_ids='transform_news', 
            key='transform_df'
        )
        
        # Convert JSON back to DataFrame (parse dates as datetime)
        df = pd.read_json(data, convert_dates=['Date Published'])
        
        logging.info(f"Loading {len(df)} articles into PostgreSQL database")
        
        # Load data with ON CONFLICT DO NOTHING to skip duplicates
        with engine.connect() as connection:
            # Get initial count
            result = connection.execute(text(f"SELECT COUNT(*) FROM {DB_TABLE_NAME}"))
            initial_count = result.scalar()
            
            # Insert data row by row with conflict handling
            inserted_count = 0
            skipped_count = 0
            
            for _, row in df.iterrows():
                try:
                    insert_query = text(f"""
                        INSERT INTO {DB_TABLE_NAME} 
                        ("Source", "Author Name", "News Title", "URL", "Date Published", "Content")
                        VALUES (:source, :author, :title, :url, :date, :content)
                        ON CONFLICT ("URL") DO NOTHING
                    """)
                    
                    connection.execute(insert_query, {
                        'source': row['Source'],
                        'author': row['Author Name'],
                        'title': row['News Title'],
                        'url': row['URL'],
                        'date': row['Date Published'],
                        'content': row['Content']
                    })
                    inserted_count += 1
                except Exception as e:
                    skipped_count += 1
                    continue
            
            connection.commit()
            
            # Get final count
            result = connection.execute(text(f"SELECT COUNT(*) FROM {DB_TABLE_NAME}"))
            final_count = result.scalar()
            
            actual_inserted = final_count - initial_count
        
        logging.info(f"Successfully processed {len(df)} articles:")
        logging.info(f"  - Inserted: {actual_inserted} new articles")
        logging.info(f"  - Skipped: {len(df) - actual_inserted} duplicates")
        logging.info(f"  - Total records in database: {final_count}")
        
        # Close engine
        engine.dispose()
        
    except SQLAlchemyError as e:
        logging.error(f"Database error occurred: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to load data: {e}")
        raise
