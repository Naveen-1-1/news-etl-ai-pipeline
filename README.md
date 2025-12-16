# 📰 News ETL Pipeline with Apache Airflow & PostgreSQL

An automated ETL (Extract, Transform, Load) pipeline built with Apache Airflow that fetches AI-related news articles from NewsAPI, processes them, and stores them in a PostgreSQL database. The pipeline runs daily to keep the database updated with the latest news.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-3.1+-green.svg)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-blue.svg)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 🎯 Project Overview

This project demonstrates a production-ready data engineering workflow that:
- **Extracts** news articles from NewsAPI's AI-powered service
- **Transforms** raw data into structured, clean format
- **Loads** processed data into a PostgreSQL database with duplicate handling
- **Orchestrates** the entire workflow using Apache Airflow with proper error handling and logging

### Key Features

✅ Production-grade PostgreSQL database with indexing and constraints  
✅ Automatic duplicate detection and handling (ON CONFLICT)  
✅ Modular architecture with separation of concerns (Extract, Transform, Load)  
✅ Automated daily scheduling via Airflow DAG  
✅ Robust error handling and comprehensive logging  
✅ XCom integration for inter-task communication  
✅ Data quality transformations (date formatting, author cleaning)  
✅ SQLAlchemy ORM for database operations  
✅ Environment variable management for secure credentials  

---

## 🏗️ Architecture

```
Extract (NewsAPI)  →  Transform (Pandas)  →  Load (PostgreSQL)
      ↓                      ↓                     ↓
  Raw Articles        Cleaned Data       Database (w/ deduplication)
```

### Project Structure

```
news-etl-ai-pipeline/
├── README.md                          # Project documentation
├── requirements.txt                   # Python dependencies
├── .env.example                       # Environment variable template
├── .gitignore                         # Git ignore rules
│
├── config/
│   └── config.py                      # Centralized configuration
│
├── src/
│   ├── __init__.py
│   ├── extract.py                     # NewsAPI data extraction
│   ├── transform.py                   # Data cleaning & transformation
│   ├── load.py                        # PostgreSQL loading with dedup
│   └── init_db.py                     # Database initialization
│
└── airflow/
    └── dags/
        └── news_etl_pipeline.py       # Airflow DAG definition
```

---

## 🛠️ Technologies Used

| Technology | Purpose |
|------------|---------|
| **Apache Airflow 3.1+** | Workflow orchestration and scheduling |
| **Python 3.13** | Core programming language |
| **PostgreSQL 18** | Production-grade relational database |
| **SQLAlchemy** | Database ORM and connection management |
| **Pandas** | Data transformation and manipulation |
| **NewsAPI** | News article data source |
| **python-dotenv** | Environment variable management |

---

## 🚀 Getting Started

### Prerequisites

- Python 3.11 or higher (tested with Python 3.13)
- PostgreSQL 18
- pip (Python package manager)
- NewsAPI key (get free at [newsapi.org](https://newsapi.org))

### Step 1: Install PostgreSQL

**macOS (using Homebrew):**
```bash
brew install postgresql@18
brew services start postgresql@18

# Add PostgreSQL to PATH
echo 'export PATH="/opt/homebrew/opt/postgresql@18/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
```

### Step 2: Create Database

```bash
# Create database (using your system username as superuser)
createdb news_etl_db

# Verify it exists
psql -l | grep news_etl_db
```

### Step 3: Set Up Project

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/news-etl-ai-pipeline.git
cd news-etl-ai-pipeline

# 2. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 4. Set up environment variables
cp .env.example .env
nano .env
```

Edit `.env` with your credentials:
```env
# NewsAPI
NEWS_API_KEY=your_newsapi_key_here

# PostgreSQL (use your system username for local development)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=news_etl_db
DB_USER=your_username
DB_PASSWORD=
```

```bash
# 5. Initialize database schema
python src/init_db.py

# 6. Set AIRFLOW_HOME
export AIRFLOW_HOME=$(pwd)/airflow

# 7. Initialize Airflow
airflow db migrate

# 8. Start Airflow
airflow standalone
```

**Save the admin password** printed in the console!

### Step 4: Access Airflow UI

1. Open browser to `http://localhost:8080`
2. Login with username: `admin` and password from console
3. Find `news_etl_pipeline` DAG
4. Toggle it **ON** (switch on left)
5. Click **▶️ play button** to trigger manually

---

## 📊 Usage

### Running the Pipeline

**Via Airflow UI:**
- Navigate to `http://localhost:8080`
- Find `news_etl_pipeline` in the DAG list
- Click play button to trigger manually
- Monitor execution in Graph or Tree view

**Via Command Line:**
```bash
airflow dags trigger news_etl_pipeline
```

### Monitoring

**Check pipeline status:**
```bash
# View all DAGs
airflow dags list

# Check specific DAG runs
airflow dags list-runs -d news_etl_pipeline
```

**Query the database:**
```bash
psql -d news_etl_db

# In PostgreSQL prompt:
SELECT COUNT(*) FROM news_table;

SELECT "News Title", "Source", "Date Published" 
FROM news_table 
ORDER BY "Date Published" DESC 
LIMIT 5;

\q
```

**Useful SQL Queries:**
```sql
-- Count articles by source
SELECT "Source", COUNT(*) as count 
FROM news_table 
GROUP BY "Source" 
ORDER BY count DESC;

-- Recent articles (last 7 days)
SELECT "News Title", "Date Published" 
FROM news_table 
WHERE "Date Published" >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY "Date Published" DESC;

-- Search for specific topics
SELECT "News Title", "Source" 
FROM news_table 
WHERE "Content" ILIKE '%machine learning%';

-- Check for duplicates (should return 0)
SELECT "URL", COUNT(*) 
FROM news_table 
GROUP BY "URL" 
HAVING COUNT(*) > 1;
```

---

## 🗄️ Database Schema

```sql
CREATE TABLE news_table (
    id SERIAL PRIMARY KEY,
    "Source" VARCHAR(100),
    "Author Name" TEXT,
    "News Title" TEXT,
    "URL" TEXT UNIQUE NOT NULL,           -- Prevents duplicates
    "Date Published" TIMESTAMP,
    "Content" TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Performance indexes
CREATE INDEX idx_url ON news_table("URL");
CREATE INDEX idx_date_published ON news_table("Date Published");
```

### Duplicate Handling

The pipeline uses PostgreSQL's `ON CONFLICT` to handle duplicates:
```sql
INSERT INTO news_table (...) VALUES (...)
ON CONFLICT ("URL") DO NOTHING;
```

This ensures:
- No duplicate articles (same URL)
- Pipeline doesn't fail on re-runs
- Idempotent behavior (safe to run multiple times)

---

## 🔧 Configuration

### Application Config (`config/config.py`)
- Search query (default: "AI")
- Language filter (default: "en")
- Schedule interval (default: daily)
- Database connection settings
- Retry logic

### Environment Variables (`.env`)
- PostgreSQL credentials
- NewsAPI key
- Database host/port

---

## 🐛 Troubleshooting

### PostgreSQL Connection Error
```bash
# Check PostgreSQL is running
brew services list | grep postgresql  # macOS
sudo systemctl status postgresql      # Linux

# Test connection
psql -d news_etl_db

# Check credentials in .env file
cat .env
```

### Module Import Errors
```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### DAG Not Showing in UI
```bash
# Verify AIRFLOW_HOME
echo $AIRFLOW_HOME

# Should output: /path/to/news-etl-ai-pipeline/airflow
# If not set: export AIRFLOW_HOME=$(pwd)/airflow

# Restart Airflow (Ctrl+C then restart)
airflow standalone
```

### Database Table Not Found
```bash
# Reinitialize database
python src/init_db.py
```

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---