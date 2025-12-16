# 📰 News ETL Pipeline with Apache Airflow & PostgreSQL

An automated ETL (Extract, Transform, Load) pipeline built with Apache Airflow that fetches AI-related news articles from NewsAPI, processes them, and stores them in a PostgreSQL database. The pipeline runs daily to keep the database updated with the latest news.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-3.1+-green.svg)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-blue.svg)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 🎯 Project Overview

This project demonstrates a production-ready data engineering workflow that:
- **Extracts** news articles from NewsAPI's AI-powered service
- **Transforms** raw data into structured, clean format
- **Loads** processed data into a PostgreSQL database with duplicate handling
- **Orchestrates** the entire workflow using Apache Airflow with proper error handling and logging
- **Containerized** with Docker for easy deployment and portability

### Key Features

✅ **Docker-ready** - One-command deployment with docker-compose  
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

### Docker Architecture

```
┌─────────────────────────────────────┐
│     Docker Compose Network          │
│                                     │
│  ┌─────────────┐   ┌──────────────┐│
│  │  Airflow    │───│  PostgreSQL  ││
│  │  Container  │   │   Container  ││
│  │  (Port 8080)│   │  (Port 5432) ││
│  └─────────────┘   └──────────────┘│
└─────────────────────────────────────┘
```

### Project Structure

```
news-etl-ai-pipeline/
├── README.md                          # Project documentation
├── requirements.txt                   # Python dependencies
├── Dockerfile                         # Docker image definition
├── docker-compose.yml                 # Multi-container orchestration
├── .env.example                       # Environment variable template
├── .env.docker                        # Docker-specific env template
├── .dockerignore                      # Docker build exclusions
├── .gitignore                         # Git ignore rules
├── create_admin.py                    # Admin user creation script
│
├── config/
│   ├── __init__.py
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
| **Docker & Docker Compose** | Containerization and deployment |
| **SQLAlchemy** | Database ORM and connection management |
| **Pandas** | Data transformation and manipulation |
| **NewsAPI** | News article data source |
| **python-dotenv** | Environment variable management |

---

## 🚀 Getting Started

### Option 1: Docker Setup (Recommended - 2 minutes) 🐳

**Prerequisites:**
- Docker Desktop installed
- NewsAPI key

**Steps:**
```bash
# 1. Clone repository
git clone https://github.com/yourusername/news-etl-ai-pipeline.git
cd news-etl-ai-pipeline

# 2. Set up environment variables
cp .env.docker .env
nano .env  # Add your NewsAPI key

# 3. Start everything with Docker Compose
docker-compose up -d

# 4. Check logs
docker-compose logs -f airflow

# 5. Access Airflow UI
# Open http://localhost:8080
# Login: username=admin, password=admin
```

**That's it!** PostgreSQL and Airflow are now running in containers.

**To stop:**
```bash
docker-compose down
```

**To restart:**
```bash
docker-compose up -d
```

---

### Option 2: Local Setup (Manual)

**Prerequisites:**
- Python 3.11 or higher (tested with Python 3.13)
- PostgreSQL 18
- pip (Python package manager)
- NewsAPI key (get free at [newsapi.org](https://newsapi.org))

**Step 1: Install PostgreSQL**

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

**Step 2: Create Database**

```bash
# Create database (using your system username as superuser)
createdb news_etl_db

# Verify it exists
psql -l | grep news_etl_db
```

**Step 3: Set Up Project**

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

**Step 4: Access Airflow UI**

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

**Via Command Line (Docker):**
```bash
docker-compose exec airflow airflow dags trigger news_etl_pipeline
docker compose exec airflow airflow dags unpause news_etl_pipeline
```

**Via Command Line (Local):**
```bash
airflow dags trigger news_etl_pipeline
```

### Monitoring

**Docker:**
```bash
# View Airflow logs
docker-compose logs -f airflow

# View PostgreSQL logs
docker-compose logs -f postgres

# Access PostgreSQL shell
docker-compose exec postgres psql -U airflow -d news_etl_db
```

**Local:**
```bash
# View all DAGs
airflow dags list

# Check specific DAG runs
airflow dags list-runs -d news_etl_pipeline
```

### Querying the Database

**Docker:**
```bash
docker-compose exec postgres psql -U airflow -d news_etl_db

# In PostgreSQL prompt:
SELECT COUNT(*) FROM news_table;
SELECT "News Title", "Source" FROM news_table LIMIT 5;
\q
```

**Local:**
```bash
psql -d news_etl_db

# Run your queries
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
```

---

## 🐳 Docker Commands

### Essential Commands

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# View logs
docker-compose logs -f

# Restart services
docker-compose restart

# Rebuild after code changes
docker-compose up -d --build

# Remove everything (including volumes)
docker-compose down -v
```

### Accessing Containers

```bash
# Airflow shell
docker-compose exec airflow bash

# PostgreSQL shell
docker-compose exec postgres psql -U airflow -d news_etl_db

# View Airflow logs
docker-compose exec airflow tail -f /opt/airflow/logs/scheduler/latest/*.log
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

### Environment Variables

**For Docker (`.env`):**
```env
NEWS_API_KEY=your_key_here
```

**For Local Development (`.env`):**
```env
NEWS_API_KEY=your_key_here
DB_HOST=localhost
DB_PORT=5432
DB_NAME=news_etl_db
DB_USER=your_username
DB_PASSWORD=
```

---

## 🐛 Troubleshooting

### Docker Issues

**Container won't start:**
```bash
# Check logs
docker-compose logs airflow
docker-compose logs postgres

# Rebuild containers
docker-compose down
docker-compose up -d --build
```

**Port already in use:**
```bash
# Check what's using port 8080
lsof -i :8080

# Kill the process or change port in docker-compose.yml
```

**Database connection issues:**
```bash
# Verify PostgreSQL is healthy
docker-compose ps

# Should show postgres as "healthy"
```

### Local Setup Issues

**PostgreSQL Connection Error:**
```bash
# Check PostgreSQL is running
brew services list | grep postgresql  # macOS
sudo systemctl status postgresql      # Linux

# Test connection
psql -d news_etl_db
```

**Module Import Errors:**
```bash
source .venv/bin/activate
pip install -r requirements.txt
```

**DAG Not Showing:**
```bash
# Verify AIRFLOW_HOME
echo $AIRFLOW_HOME
export AIRFLOW_HOME=$(pwd)/airflow

# Restart Airflow
```

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---