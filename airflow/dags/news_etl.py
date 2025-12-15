import pandas as pd
import logging
import sqlite3
from newsapi import NewsApiClient
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import additional libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, time

# Initialize NewsApiClient
news_api_key = os.getenv("NEWS_API_KEY")
news_api = NewsApiClient(api_key=news_api_key)

# Remove top-level dynamic date calculations to avoid scheduler issues
# to_date = datetime.utcnow().date()
# from_date  = to_date - timedelta(days=1)

# Initialize DAG object
dag = DAG(
    dag_id="news_etl",
    default_args={'start_date': datetime(2023, 12, 1), 'retries': 1},
    schedule='@daily',
)

# Replace your function with the following extract_news_data function:
def extract_news_data(**kwargs):
    # Calculate dates inside the task used execution_date or current time
    to_date = datetime.utcnow().date()
    from_date = to_date - timedelta(days=1)
    
    try: 
        result = news_api.get_everything(q="AI", language="en", from_param=from_date, to=to_date)
        logging.info("Connection is successful.")
        # Push the result to the XCom
        kwargs['task_instance'].xcom_push(key='extract_result', value=result["articles"])
    except Exception as e:
        logging.error(f"Connection is unsuccessful: {e}")
    
def clean_author_column(text):    
    try:
        return text.split(",")[0].title()
    except AttributeError:
        return "No Author"

# Replace your function with the following transform_news_data function:
def transform_news_data(**kwargs):
    article_list = []
    # Add the XCom pull logic to pull data from the XCom
    data = kwargs['task_instance'].xcom_pull(task_ids='extract_news', key='extract_result')

    # Logging message after the XCom pull
    logging.info("Data pulled successfully.")

    for i in data:
        article_list.append([value.get("name", 0) if key == "source" else value for key, value in i.items() if key in ["author", "title", "publishedAt", "content", "url", "source"]])

    df = pd.DataFrame(article_list, columns=["Source", "Author Name", "News Title", "URL", "Date Published", "Content"])

    df["Date Published"] = pd.to_datetime(df["Date Published"]).dt.strftime('%Y-%m-%d %H:%M:%S')

    df["Author Name"] = df["Author Name"].apply(clean_author_column)
 
    #Add the XCom push logic to push data to the XCom
    kwargs['task_instance'].xcom_push(key='transform_df', value=df.to_json())

    # Logging message after the XCom push
    logging.info("Transformed data pushed to XCom successfully.")

# Replace your function with the following load_news_data function:
def load_news_data(**kwargs):
    # Line 83 in load_news_data function, change:
    with sqlite3.connect("/Users/naveen/Naveen/Projects/news-etl/etl_news_data.sqlite") as connection:
        # Create a cursor within the context manager
        cursor = connection.cursor()

        # Create a table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS news_table (
                "Source" VARCHAR(30),
                "Author Name" TEXT,
                "News Title" TEXT,
                "URL" TEXT,
                "Date Published" TEXT,
                "Content" TEXT
            )
        ''')

        # Pull data from XCom
        data = kwargs['task_instance'].xcom_pull(task_ids='transform_news', key='transform_df')
     
        # Convert data into a DataFrame
        df = pd.read_json(data)

        # Logging message before loading data
        logging.info("Ready to load data into the database.")

        df.to_sql(name="news_table", con=connection, index=False, if_exists="append")

        # Logging message after loading data
        logging.info("Data successfully loaded into the database.")

# Create Python operators
_extract_news_data = PythonOperator(
    task_id = "extract_news",
    python_callable = extract_news_data,
    dag = dag
)

_transform_news_data = PythonOperator(
    task_id = "transform_news",
    python_callable = transform_news_data,
    dag = dag
)

_load_news_data = PythonOperator(
    task_id = "load_news",
    python_callable = load_news_data,
    dag = dag
)

# Create dependencies
_extract_news_data >> _transform_news_data >> _load_news_data