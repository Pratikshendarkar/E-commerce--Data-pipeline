"""
Airflow DAG for orchestrating the Olist E-Commerce Data Pipeline.

Medallion architecture: Bronze -> Silver -> Gold
Schedule: Daily at 2 AM UTC (configurable)
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from dotenv import load_dotenv

# Add project root to path for imports
PROJECT_ROOT = str(Path(__file__).parent.parent)
sys.path.insert(0, PROJECT_ROOT)

# Load environment variables
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# Default DAG arguments
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# DAG definition
dag = DAG(
    dag_id="olist_data_pipeline",
    default_args=default_args,
    description="End-to-end Olist data pipeline: S3 -> Bronze -> Silver -> Gold -> S3",
    schedule_interval="0 2 * * *",  # 2 AM UTC daily
    catchup=False,
    tags=["data-engineering", "snowflake", "medallion"],
)


def run_s3_to_bronze(**context):
    """Load raw CSV files from S3 into Snowflake BRONZE schema."""
    import subprocess
    result = subprocess.run(
        ["python", f"{PROJECT_ROOT}/data_ingestion/s3_to_bronze.py"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"s3_to_bronze failed: {result.stderr}")
    print(result.stdout)


def run_quality_checks(**context):
    """Run quality checks on BRONZE tables."""
    import subprocess
    result = subprocess.run(
        ["python", f"{PROJECT_ROOT}/data_ingestion/quality_checks.py"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"quality_checks failed: {result.stderr}")
    print(result.stdout)


def run_data_cleaning(**context):
    """Flag and cap outliers in BRONZE data."""
    import subprocess
    result = subprocess.run(
        ["python", f"{PROJECT_ROOT}/data_ingestion/data_cleaning.py"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"data_cleaning failed: {result.stderr}")
    print(result.stdout)


def run_bronze_to_silver(**context):
    """Promote clean data from BRONZE to SILVER schema."""
    import subprocess
    result = subprocess.run(
        ["python", f"{PROJECT_ROOT}/data_ingestion/bronze_to_silver.py"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"bronze_to_silver failed: {result.stderr}")
    print(result.stdout)


def run_silver_to_gold(**context):
    """Transform SILVER to GOLD: star schema + aggregations + ML features."""
    import subprocess
    result = subprocess.run(
        ["python", f"{PROJECT_ROOT}/data_ingestion/silver_to_gold.py"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"silver_to_gold failed: {result.stderr}")
    print(result.stdout)


def run_gold_to_s3(**context):
    """Export GOLD tables back to S3."""
    import subprocess
    result = subprocess.run(
        ["python", f"{PROJECT_ROOT}/data_ingestion/gold_to_s3.py"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"gold_to_s3 failed: {result.stderr}")
    print(result.stdout)


# ───────────────────────────────────────────────────────────────────────────────
# Task Definitions
# ───────────────────────────────────────────────────────────────────────────────

with dag:
    # Stage 1: Ingest raw data
    task_s3_to_bronze = PythonOperator(
        task_id="s3_to_bronze",
        python_callable=run_s3_to_bronze,
        doc_md="Load raw CSVs from S3 into Snowflake BRONZE schema.",
    )

    # Stage 2: Validation & Cleaning (parallel)
    with TaskGroup("stage_2_validation_cleaning") as stage_2:
        task_quality_checks = PythonOperator(
            task_id="quality_checks",
            python_callable=run_quality_checks,
            doc_md="Run 25 data quality checks on BRONZE tables.",
        )

        task_data_cleaning = PythonOperator(
            task_id="data_cleaning",
            python_callable=run_data_cleaning,
            doc_md="Flag and cap outliers using IQR method.",
        )

    # Stage 3: Bronze to Silver
    task_bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_bronze_to_silver,
        doc_md="Remove invalid records, write _stage tables to SILVER schema.",
    )

    # Stage 4: Silver to Gold
    task_silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_silver_to_gold,
        doc_md="Transform to star schema: 5 dims + 2 facts + 4 aggs + 4 ML features + 1 master.",
    )

    # Stage 5: Export to S3
    task_gold_to_s3 = PythonOperator(
        task_id="gold_to_s3",
        python_callable=run_gold_to_s3,
        doc_md="COPY INTO unload all GOLD tables to S3.",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Define task dependencies
    # ─────────────────────────────────────────────────────────────────────────

    task_s3_to_bronze >> stage_2
    stage_2 >> task_bronze_to_silver
    task_bronze_to_silver >> task_silver_to_gold
    task_silver_to_gold >> task_gold_to_s3
