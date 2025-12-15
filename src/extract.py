"""
Extract module - Fetch news articles from NewsAPI
"""
import logging
from datetime import datetime, timedelta
from newsapi import NewsApiClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize NewsAPI client
news_api_key = os.getenv("NEWS_API_KEY")
news_api = NewsApiClient(api_key=news_api_key)


def extract_news_data(**kwargs):
    """
    Extract news articles from NewsAPI for the previous day.
    
    Args:
        **kwargs: Airflow context including task_instance for XCom
        
    Returns:
        Pushes extracted articles to XCom
    """
    # Calculate date range (previous day)
    to_date = datetime.utcnow().date()
    from_date = to_date - timedelta(days=1)
    
    try: 
        # Fetch news articles about AI
        result = news_api.get_everything(
            q="AI", 
            language="en", 
            from_param=from_date, 
            to=to_date
        )
        
        logging.info(f"Successfully extracted {len(result['articles'])} articles from NewsAPI")
        
        # Push result to XCom for downstream tasks
        kwargs['task_instance'].xcom_push(
            key='extract_result', 
            value=result["articles"]
        )
        
    except Exception as e:
        logging.error(f"Failed to extract news data: {e}")
        raise
