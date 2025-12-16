"""
Database initialization script for PostgreSQL
Creates the news_table with proper schema and constraints
"""
import sys
import os

# Add parent directory to path for imports (works in both Docker and local)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from sqlalchemy import create_engine, text, inspect
from config.config import DATABASE_URI, DB_TABLE_NAME
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_database():
    """
    Initialize PostgreSQL database with required schema.
    Creates table if it doesn't exist.
    """
    try:
        # Create engine
        engine = create_engine(DATABASE_URI)
        
        # Check if table exists
        inspector = inspect(engine)
        if DB_TABLE_NAME in inspector.get_table_names():
            logger.info(f"Table '{DB_TABLE_NAME}' already exists")
            return
        
        # Create table with proper schema
        with engine.connect() as connection:
            create_table_query = text(f"""
                CREATE TABLE IF NOT EXISTS {DB_TABLE_NAME} (
                    id SERIAL PRIMARY KEY,
                    "Source" VARCHAR(100),
                    "Author Name" TEXT,
                    "News Title" TEXT,
                    "URL" TEXT UNIQUE NOT NULL,
                    "Date Published" TIMESTAMP,
                    "Content" TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            connection.execute(create_table_query)
            connection.commit()
            
            logger.info(f"✅ Table '{DB_TABLE_NAME}' created successfully")
        
        # Create index on URL for faster lookups
        with engine.connect() as connection:
            create_index_query = text(f"""
                CREATE INDEX IF NOT EXISTS idx_url 
                ON {DB_TABLE_NAME}("URL")
            """)
            
            connection.execute(create_index_query)
            connection.commit()
            
            logger.info(f"✅ Index on URL created successfully")
        
        # Create index on date for time-based queries
        with engine.connect() as connection:
            create_date_index_query = text(f"""
                CREATE INDEX IF NOT EXISTS idx_date_published 
                ON {DB_TABLE_NAME}("Date Published")
            """)
            
            connection.execute(create_date_index_query)
            connection.commit()
            
            logger.info(f"✅ Index on Date Published created successfully")
        
        engine.dispose()
        logger.info("Database initialization complete!")
        
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        raise


if __name__ == "__main__":
    init_database()
