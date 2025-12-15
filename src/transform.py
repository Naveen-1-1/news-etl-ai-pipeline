"""
Transform module - Clean and transform news data
"""
import pandas as pd
import logging


def clean_author_column(text):
    """
    Clean author field by extracting first author name.
    
    Args:
        text: Raw author string from API
        
    Returns:
        Cleaned author name in title case
    """
    try:
        return text.split(",")[0].title()
    except AttributeError:
        return "No Author"


def transform_news_data(**kwargs):
    """
    Transform raw news articles into structured DataFrame.
    
    Transformations:
    - Extract relevant fields (source, author, title, url, date, content)
    - Clean author names
    - Format dates
    
    Args:
        **kwargs: Airflow context including task_instance for XCom
        
    Returns:
        Pushes transformed DataFrame (as JSON) to XCom
    """
    # Pull raw data from XCom
    data = kwargs['task_instance'].xcom_pull(
        task_ids='extract_news', 
        key='extract_result'
    )
    
    logging.info(f"Starting transformation on {len(data)} articles")
    
    # Extract relevant fields from each article
    article_list = []
    for article in data:
        article_list.append([
            article.get("source", {}).get("name", "Unknown Source"),
            article.get("author"),
            article.get("title"),
            article.get("url"),
            article.get("publishedAt"),
            article.get("content")
        ])
    
    # Create DataFrame
    df = pd.DataFrame(
        article_list, 
        columns=["Source", "Author Name", "News Title", "URL", "Date Published", "Content"]
    )
    
    # Transform date format
    df["Date Published"] = pd.to_datetime(df["Date Published"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Clean author names
    df["Author Name"] = df["Author Name"].apply(clean_author_column)
    
    logging.info(f"Transformation complete. Processed {len(df)} articles")
    
    # Push transformed data to XCom
    kwargs['task_instance'].xcom_push(
        key='transform_df', 
        value=df.to_json()
    )
