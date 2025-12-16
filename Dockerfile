FROM apache/airflow:3.1.5-python3.13

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /opt/airflow/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy project files
COPY --chown=airflow:root ./src /opt/airflow/src
COPY --chown=airflow:root ./config /opt/airflow/config
COPY --chown=airflow:root ./airflow/dags /opt/airflow/dags

# Set working directory
WORKDIR /opt/airflow

# Expose Airflow webserver port
EXPOSE 8080

# Default command (can be overridden in docker-compose)
CMD ["airflow", "standalone"]
