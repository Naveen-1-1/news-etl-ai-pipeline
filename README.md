# 📰 News ETL Pipeline with Apache Airflow

An automated ETL (Extract, Transform, Load) pipeline built with Apache Airflow that fetches AI-related news articles from NewsAPI, processes them, and stores them in a SQLite database. The pipeline runs daily to keep the database updated with the latest news.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-3.1+-green.svg)](https://airflow.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 🎯 Project Overview

This project demonstrates a production-ready data engineering workflow that:
- **Extracts** news articles from NewsAPI's AI-powered service
- **Transforms** raw data into structured, clean format
- **Loads** processed data into a SQLite database
- **Orchestrates** the entire workflow using Apache Airflow with proper error handling and logging

### Key Features

✅ Modular architecture with separation of concerns (Extract, Transform, Load)  
✅ Automated daily scheduling via Airflow DAG  
✅ Robust error handling and comprehensive logging  
✅ XCom integration for inter-task communication  
✅ Data quality transformations (date formatting, author cleaning)  
✅ Configurable parameters through centralized config  
✅ Environment variable management for secure API key storage  

---

## 🏗️ Architecture

```
Extract (NewsAPI)  →  Transform (Pandas)  →  Load (SQLite)
      ↓                      ↓                     ↓
  Raw Articles        Cleaned Data            Database
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
│   └── load.py                        # Database loading
│
├── airflow/
│   └── dags/
│       └── news_etl_pipeline.py       # Airflow DAG definition
│
└── data/
    └── etl_news_data.sqlite           # SQLite database (generated)
```

---

## 🛠️ Technologies Used

| Technology | Purpose |
|------------|---------|
| **Apache Airflow 3.1+** | Workflow orchestration and scheduling |
| **Python 3.11+** | Core programming language |
| **Pandas** | Data transformation and manipulation |
| **NewsAPI** | News article data source |
| **SQLite** | Data storage |
| **python-dotenv** | Environment variable management |

---

## 🚀 Getting Started

### Prerequisites

- Python 3.11 or higher (tested with Python 3.13)
- pip (Python package manager)
- NewsAPI key (get free at [newsapi.org](https://newsapi.org))

### Quick Setup (5 minutes)

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/news-etl-ai-pipeline.git
cd news-etl-ai-pipeline

# 2. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# On Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 4. Set up environment variables
cp .env.example .env
nano .env  # Add your NewsAPI key

# 5. Set AIRFLOW_HOME
export AIRFLOW_HOME=$(pwd)/airflow

# 6. Initialize Airflow
airflow db migrate

# 7. Start Airflow
airflow standalone
```

**Save the admin password** printed in the console!

### Access Airflow UI

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
sqlite3 data/etl_news_data.sqlite

# In SQLite prompt:
SELECT COUNT(*) FROM news_table;
SELECT * FROM news_table LIMIT 5;
.exit
```

### Daily Operation

After initial setup, the pipeline runs automatically at midnight. To manually operate:

```bash
# Navigate to project
cd /path/to/news-etl-ai-pipeline

# Activate virtual environment
source .venv/bin/activate

# Set AIRFLOW_HOME (if not permanent)
export AIRFLOW_HOME=$(pwd)/airflow

# Start Airflow
airflow standalone
```

**Stop Airflow:** Press `Ctrl+C`

---

## 🔧 Configuration

Modify `config/config.py` to customize:
- Search query (default: "AI")
- Language filter (default: "en")
- Schedule interval (default: daily)
- Database settings
- Retry logic

---

## 🐛 Troubleshooting

### ModuleNotFoundError
```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### DAG not showing in UI
```bash
# Verify AIRFLOW_HOME
echo $AIRFLOW_HOME

# Should output: /path/to/news-etl-ai-pipeline/airflow
# If not set, run: export AIRFLOW_HOME=$(pwd)/airflow

# Restart Airflow
```

### API Key Error
```bash
# Verify .env file exists and has correct format
cat .env
# Should show: NEWS_API_KEY=your_key_here (no spaces around =)
```

### Port 8080 Already in Use
```bash
airflow standalone --port 8081
```

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---