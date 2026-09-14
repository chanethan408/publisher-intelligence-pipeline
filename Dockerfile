FROM apache/airflow:2.9.3-python3.11

USER root

# Install system dependencies if required
RUN apt-get update && \
    apt-get install -y --no-install-recommends git gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Copy and install python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt