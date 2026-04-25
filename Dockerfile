FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY data_ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline scripts
COPY data_ingestion/ ./data_ingestion/
