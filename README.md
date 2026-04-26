# Olist E-Commerce Data Pipeline

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-FF9900?style=for-the-badge&logo=amazons3&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

> End-to-end data engineering pipeline for the Brazilian Olist E-Commerce dataset.
> Ingests raw CSVs from S3, applies a **Bronze → Silver → Gold** medallion architecture in Snowflake,
> and exports the final Gold layer back to S3 — ready for Power BI dashboards and ML model training.

---
<img width="1875" height="920" alt="image" src="https://github.com/user-attachments/assets/7df89aca-fdf2-4331-bec8-f348fa2b8541" />


## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Pipeline Layers](#data-pipeline-layers)
- [Gold Layer — Data Model](#gold-layer--data-model)
- [Setup](#setup)
- [Running the Pipeline](#running-the-pipeline)
- [Airflow Orchestration](#airflow-orchestration)
- [Docker](#docker)
- [Data Quality](#data-quality)

---

## Architecture

```mermaid
flowchart LR
    subgraph S3_IN["☁️ AWS S3 (Input)"]
        RAW["s3://awsdatapratik/baselayer/\n*.csv"]
    end

    subgraph SNOWFLAKE["❄️ Snowflake — DATAENGINEER"]
        direction TB
        BRONZE["🥉 BRONZE Schema\nRaw tables loaded as-is\n8 tables"]
        SILVER["🥈 SILVER Schema\nCleaned & validated\n8 _stage tables"]
        GOLD["🥇 GOLD Schema\nTransformed & modelled\n16 tables"]
        BRONZE --> SILVER --> GOLD
    end

    subgraph S3_OUT["☁️ AWS S3 (Output)"]
        FINAL["s3://awsdatapratik/finaloutput/\n<table_name>/*.csv"]
    end

    S3_IN -->|s3_to_bronze.py| BRONZE
    GOLD -->|gold_to_s3.py| S3_OUT
    GOLD -->|Power BI| BI["📊 Dashboards"]
    GOLD -->|ML Models| ML["🤖 Predictive Models"]
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Storage (Raw & Output) | AWS S3 |
| Data Warehouse | Snowflake |
| Orchestration | Apache Airflow 2.8.1 |
| Workflow Management | Docker Compose |
| Containerisation | Docker |
| BI / Reporting | Power BI |
| Language | Python 3.11 |

---

## Project Structure

```
Dataengineer/
├── Dockerfile                            # Container image definition
├── docker-compose.yml                    # Docker orchestration
├── .env                                  # Credentials (not committed)
├── .gitignore
│
├── dags/
│   └── olist_pipeline_dag.py             # Airflow DAG: all 6 pipeline stages
│
├── data_ingestion/
│   ├── s3_to_bronze.py                   # S3 → Snowflake BRONZE
│   ├── bronze_to_silver.py               # BRONZE → SILVER (remove bad records)
│   ├── silver_to_gold.py                 # SILVER → GOLD (transform & model)
│   ├── gold_to_s3.py                     # GOLD → S3 export
│   ├── data_cleaning.py                  # Flag & cap outliers (IQR method)
│   ├── quality_checks.py                 # 25 QC checks on BRONZE tables
│   ├── send_report.py                    # Email: data cleaning summary
│   ├── send_silver_report.py             # Email: Bronze→Silver summary
│   └── requirements.txt                  # Python dependencies
│
├── AIRFLOW_SETUP.md                      # Local Airflow setup guide
├── DOCKER_AIRFLOW.md                     # Docker Airflow guide
├── README.md                             # This file
│
└── Inputdata/                            # Source CSVs (local reference)
    ├── olist_customers_dataset.csv
    ├── olist_orders_dataset.csv
    ├── olist_order_items_dataset.csv
    ├── olist_order_payments_dataset.csv
    ├── olist_order_reviews_dataset.csv
    ├── olist_products_dataset.csv
    ├── olist_geolocation_dataset.csv
    └── product_category_name_translation.csv
```

---

## Data Pipeline Layers

### 🥉 Bronze — Raw Ingestion

Loads all 8 source CSVs from S3 into Snowflake **as-is** with no transformations.
Handles encoding (`ISO-8859-1`), timestamp formats, and file tracking for incremental loads.

| Table | Rows |
|---|---|
| olist_customers | 99,441 |
| olist_geolocation | 1,000,163 |
| olist_order_items | 112,650 |
| olist_order_payments | 103,886 |
| olist_order_reviews | 99,224 |
| olist_orders | 99,441 |
| olist_products | 32,951 |
| product_category_name_translation | 71 |

**Incremental load:** run `s3_to_bronze.py` normally to skip already-loaded files.
Use `--full-refresh` to drop and reload from scratch.

---

### 🥈 Silver — Cleaned & Validated

Removes invalid records from Bronze and writes clean `_stage` tables.
Referential integrity is cascaded — removing a parent row removes orphaned children.

| Issue | Action | Rows Removed |
|---|---|---|
| Duplicate `review_id` | Keep most complete row | 814 |
| `payment_value <= 0` | Remove (invalid payments) | 9 |
| `payment_installments < 1` | Remove | 2 |
| Date sequence violations in orders | Remove entire order row | 1,382 |
| Price / freight outliers (IQR) | Remove from order_items | ~17,558 |
| Payment value outliers (IQR) | Remove | ~7,981 |
| Product dimension outliers (IQR) | Remove | ~5,968 |

**25 / 25 quality checks pass** on all Silver tables after cleaning.

---

### 🥇 Gold — Transformed & Modelled

Star schema + aggregations + ML feature stores. All dim tables carry surrogate integer PKs
and declared FKs so Power BI auto-detects relationships.

| Category | Tables |
|---|---|
| Dimensions | `dim_date`, `dim_geolocation`, `dim_customers`, `dim_sellers`, `dim_products` |
| Facts | `fact_orders`, `fact_order_items` |
| Aggregates | `agg_revenue_by_category`, `agg_revenue_by_state`, `agg_seller_performance`, `agg_customer_cohorts` |
| ML Features | `ml_customer_features`, `ml_seller_features`, `ml_delivery_features`, `ml_review_features` |
| Master | `master_table` (108,841 rows — fully denormalized, one row per order-item) |

---

## Gold Layer — Data Model

```mermaid
erDiagram
    dim_geolocation {
        int geo_key PK
        string zip_code_prefix
        string city
        string state
        float avg_lat
        float avg_lng
    }
    dim_customers {
        int customer_key PK
        string customer_id
        string customer_unique_id
        string state
        int geo_key FK
    }
    dim_sellers {
        int seller_key PK
        string seller_id
    }
    dim_products {
        int product_key PK
        string product_id
        string product_category_name_english
        float product_weight_g
        float product_volume_cm3
    }
    dim_date {
        int date_key PK
        date full_date
        int year
        int month
        string day_name
        boolean is_weekend
    }
    fact_orders {
        string order_id PK
        int customer_key FK
        int order_date_key FK
        float total_amount
        float total_payment
        int review_score
        int delivery_days
        boolean is_late
    }
    fact_order_items {
        string order_id FK
        int order_item_id
        int product_key FK
        int seller_key FK
        int order_date_key FK
        float price
        float freight_value
    }

    dim_geolocation ||--o{ dim_customers : "geo_key"
    dim_customers ||--o{ fact_orders : "customer_key"
    dim_date ||--o{ fact_orders : "order_date_key"
    fact_orders ||--o{ fact_order_items : "order_id"
    dim_products ||--o{ fact_order_items : "product_key"
    dim_sellers ||--o{ fact_order_items : "seller_key"
    dim_date ||--o{ fact_order_items : "order_date_key"
```

---

## Setup

### Prerequisites

- Python 3.11+
- Docker Desktop
- Snowflake account
- AWS S3 bucket with source CSVs uploaded

### Environment Variables

Create a `.env` file in the project root:

```env
# Snowflake
SNOWFLAKE_USER=<your_user>
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_ACCOUNT=<your_account>
SNOWFLAKE_WAREHOUSE=<your_warehouse>
SNOWFLAKE_DATABASE=<your_database>
SNOWFLAKE_SCHEMA=BRONZE

# AWS S3
AWS_KEY_ID=<your_access_key>
AWS_SECRET_KEY=<your_secret_key>
S3_BUCKET=<your_bucket>
S3_PATH=baselayer/

# Email (Gmail App Password)
EMAIL_SENDER=<your_gmail>@gmail.com
EMAIL_PASSWORD=<your_app_password>
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
```

### Install dependencies locally

```bash
pip install -r data_ingestion/requirements.txt
```

---

## Running the Pipeline

### Option 1: Airflow Orchestration (Recommended) 🚀

```bash
# Start Airflow scheduler & webserver in Docker
docker-compose up -d airflow-scheduler airflow-webserver

# Open Airflow UI
# http://localhost:8080

# Login with: admin / admin

# Find "olist_data_pipeline" DAG and click play button ▶
```

**Features:**
- Automatic scheduling (daily at 2 AM UTC)
- Real-time monitoring & logging
- Automatic retries on failure
- Task dependencies enforced
- All 6 stages run in sequence

For detailed setup, see: [DOCKER_AIRFLOW.md](DOCKER_AIRFLOW.md)

---

### Option 2: Docker (Manual Stages)

```bash
# Build image
docker build -t olist-pipeline .

# Run individual stages
docker-compose run s3_to_bronze
docker-compose run quality_checks
docker-compose run data_cleaning
docker-compose run bronze_to_silver
docker-compose run silver_to_gold
docker-compose run gold_to_s3
```

---

### Option 3: Local Python (Direct Execution)

```bash
pip install -r data_ingestion/requirements.txt

# 1. Ingest raw CSVs from S3 into Snowflake BRONZE
python data_ingestion/s3_to_bronze.py

# 2. Run quality checks on BRONZE
python data_ingestion/quality_checks.py

# 3. Clean BRONZE data (flag & cap outliers)
python data_ingestion/data_cleaning.py

# 4. Promote clean data to SILVER (remove bad records)
python data_ingestion/bronze_to_silver.py

# 5. Build GOLD layer (star schema + ML features)
python data_ingestion/silver_to_gold.py

# 6. Export GOLD tables back to S3
python data_ingestion/gold_to_s3.py
```

**Full refresh (drop and reload all tables):**

```bash
python data_ingestion/s3_to_bronze.py --full-refresh
```

---

## Airflow Orchestration

**DAG:** `olist_data_pipeline`

**Schedule:** Daily at 2 AM UTC (configurable)

**Tasks:**
1. `s3_to_bronze` — Load raw CSVs from S3
2. `quality_checks` — Run 25 validation checks (parallel)
3. `data_cleaning` — Flag & cap outliers (parallel)
4. `bronze_to_silver` — Remove invalid records
5. `silver_to_gold` — Build star schema + ML features
6. `gold_to_s3` — Export GOLD tables back to S3

**Task Dependencies:**
```
s3_to_bronze
  └─> [quality_checks, data_cleaning] (parallel)
        └─> bronze_to_silver
              └─> silver_to_gold
                    └─> gold_to_s3
```

**Web UI:** http://localhost:8080

**Features:**
- ✅ Automatic daily scheduling
- ✅ Real-time task monitoring
- ✅ Detailed logs for each task
- ✅ Automatic retry on failure
- ✅ One-click manual triggers
- ✅ DAG dependency visualization

See: [AIRFLOW_SETUP.md](AIRFLOW_SETUP.md) | [DOCKER_AIRFLOW.md](DOCKER_AIRFLOW.md)
<img width="1478" height="529" alt="image" src="https://github.com/user-attachments/assets/bce5a346-1950-45aa-8fad-dfa3e7f9f501" />

---

## Docker

### Build the image

```bash
docker build -t olist-pipeline .
```

### Start all services (Airflow + Pipeline)

```bash
docker-compose up -d
```

### Stop all services

```bash
docker-compose down
```

### View logs

```bash
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
```

> **Note:** Credentials are injected at runtime via `.env` — they are never baked into the image.

---

## Data Quality

Quality checks are run automatically as part of `bronze_to_silver.py` and reported to the console.
A full HTML report can also be emailed after each run.

| Check Type | Count | Result |
|---|---|---|
| Null checks on key columns | 8 | ✅ All pass |
| Duplicate PK checks | 5 | ✅ All pass (after Silver cleaning) |
| Referential integrity | 5 | ✅ All pass (after Silver cleaning) |
| Value range checks | 5 | ✅ All pass |
| Date sequence checks | 2 | ✅ All pass |
| **Total** | **25** | **25 / 25 PASS** |

---

## Quick Reference

### Commands

```bash
# Airflow (Recommended)
docker-compose up -d airflow-scheduler airflow-webserver
# Open http://localhost:8080 → Trigger "olist_data_pipeline" DAG

# Manual Docker
docker-compose run s3_to_bronze
docker-compose run bronze_to_silver
docker-compose run silver_to_gold
docker-compose run gold_to_s3

# Local Python
python data_ingestion/s3_to_bronze.py
python data_ingestion/bronze_to_silver.py
python data_ingestion/silver_to_gold.py
python data_ingestion/gold_to_s3.py
```

### Key Files

| File | Purpose |
|------|---------|
| `dags/olist_pipeline_dag.py` | Airflow DAG definition |
| `data_ingestion/s3_to_bronze.py` | Load S3 → BRONZE |
| `data_ingestion/bronze_to_silver.py` | Clean & validate |
| `data_ingestion/silver_to_gold.py` | Transform & model |
| `data_ingestion/gold_to_s3.py` | Export to S3 |
| `docker-compose.yml` | Container orchestration |
| `.env` | Configuration (not committed) |

### Documentation

- **[AIRFLOW_SETUP.md](AIRFLOW_SETUP.md)** — Local Airflow setup
- **[DOCKER_AIRFLOW.md](DOCKER_AIRFLOW.md)** — Docker + Airflow guide
- **[README.md](README.md)** — This file

---

## Troubleshooting

**Airflow DAG not showing in UI:**
1. Check `dags/olist_pipeline_dag.py` exists
2. Validate syntax: `docker-compose exec airflow-webserver airflow dags validate`
3. Restart scheduler: `docker-compose restart airflow-scheduler`

**Snowflake connection failed:**
1. Verify `.env` credentials: `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`
2. Test connection: `python data_ingestion/s3_to_bronze.py` (local)

**S3 access denied:**
1. Check `AWS_KEY_ID` and `AWS_SECRET_KEY` in `.env`
2. Verify bucket name: `S3_BUCKET=awsdatapratik`

**Docker out of disk space:**
```bash
docker system prune -a --volumes  # Clean up unused images
docker-compose down -v             # Remove all volumes
```

**Need help?**
- Airflow logs: http://localhost:8080 → Task → Logs
- Docker logs: `docker-compose logs -f <service-name>`
- Full transcript: `.claude/projects/...`

---

## Author

**Pratik Shendarkar**  
Data Engineer | Rutgers University  
[ps1424@scarletmail.rutgers.edu](mailto:ps1424@scarletmail.rutgers.edu)
