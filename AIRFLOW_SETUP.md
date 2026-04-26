# Airflow Orchestration Setup

> Orchestrate the Olist data pipeline using Apache Airflow for scheduled, monitored, and fault-tolerant execution.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r data_ingestion/requirements.txt
```

This installs:
- `apache-airflow==2.8.1` — Orchestration engine
- `apache-airflow-providers-snowflake==5.3.0` — Snowflake integration

### 2. Initialize Airflow

Set the Airflow home directory (optional, default: `~/airflow`):

```bash
export AIRFLOW_HOME=/path/to/Project/Dataengineer/.airflow
```

Or add to `.env`:

```env
AIRFLOW_HOME=./.airflow
```

Initialize the Airflow database:

```bash
airflow db init
```

This creates SQLite metadata database at `$AIRFLOW_HOME/airflow.db` (fine for local dev).

### 3. Create Airflow User

```bash
airflow users create \
  --username admin \
  --firstname Pratik \
  --lastname Shendarkar \
  --role Admin \
  --email ps1424@scarletmail.rutgers.edu \
  --password <secure_password>
```

### 4. Start Airflow Services

In **two separate terminals**:

**Terminal 1: Web UI**
```bash
airflow webserver --port 8080
```

Visit: http://localhost:8080 (log in with credentials above)

**Terminal 2: Scheduler**
```bash
airflow scheduler
```

---

## DAG Overview

### `olist_data_pipeline` DAG

**Schedule:** Daily at 2 AM UTC (configurable)

**Task Dependency Graph:**

```
s3_to_bronze
    ↓
├─ quality_checks ─┐
└─ data_cleaning ──┤
                   ↓
           bronze_to_silver
                   ↓
            silver_to_gold
                   ↓
    ┌──────────────┼──────────────┐
    ↓              ↓              ↓
gold_to_s3    send_report   send_silver_report
```

### Task Details

| Task | Description | Duration | Depends On |
|------|-------------|----------|-----------|
| **s3_to_bronze** | Load raw CSVs from S3 (incremental) | 5-10 min | Start |
| **quality_checks** | Run 25 validation checks | 2-3 min | s3_to_bronze |
| **data_cleaning** | Flag & cap outliers (IQR) | 3-5 min | s3_to_bronze |
| **bronze_to_silver** | Remove invalid records | 5-10 min | quality_checks, data_cleaning |
| **silver_to_gold** | Build star schema + ML features | 10-15 min | bronze_to_silver |
| **gold_to_s3** | Export all GOLD tables | 5-10 min | silver_to_gold |
| **send_report** | Email data cleaning summary | 1 min | gold_to_s3 |
| **send_silver_report** | Email Bronze→Silver summary | 1 min | gold_to_s3 |

**Total Pipeline Duration:** ~30-50 minutes

---

## Configuration

### Scheduling

Edit `dags/olist_pipeline_dag.py`:

```python
schedule_interval="0 2 * * *",  # Current: 2 AM UTC daily
```

**Cron Examples:**
- `"0 2 * * *"` — 2 AM UTC every day
- `"0 2 * * 1-5"` — 2 AM UTC weekdays only
- `"0 0 * * 0"` — Sunday midnight UTC
- `"*/30 * * * *"` — Every 30 minutes

[Cron Syntax Reference](https://crontab.guru/)

### Timezone

Default is UTC. To use your local timezone, set in DAG:

```python
dag = DAG(
    ...
    timezone="America/New_York",  # Or your timezone
)
```

### Retries

Modify `default_args` in DAG:

```python
default_args = {
    ...
    "retries": 1,                              # Retry once on failure
    "retry_delay": timedelta(minutes=5),       # Wait 5 min before retry
}
```

### Alerting

Enable email alerts on failure:

```python
default_args = {
    ...
    "email_on_failure": True,
    "email_on_retry": True,
    "email": ["ps1424@scarletmail.rutgers.edu"],
}
```

Configure SMTP in `airflow.cfg`:

```ini
[email]
email_backend = airflow.providers.google.gmail.utils.gmail.SendgridEmailBackend
# OR
email_backend = airflow.providers.smtp.utils.send_email_smtp

[smtp]
smtp_host = smtp.gmail.com
smtp_port = 587
smtp_user = ps1424@scarletmail.rutgers.edu
smtp_password = <app_password>
smtp_mail_from = ps1424@scarletmail.rutgers.edu
```

---

## Local Development & Testing

### Run a Single DAG

```bash
# Trigger DAG manually
airflow dags trigger olist_data_pipeline

# Trigger with a specific date
airflow dags trigger olist_data_pipeline --exec-date 2026-01-15
```

### Test a Single Task

```bash
# Test task without running full DAG
airflow tasks test olist_data_pipeline s3_to_bronze 2026-01-01

# Test with TaskGroup
airflow tasks test olist_data_pipeline stage_2_validation_cleaning 2026-01-01
```

### View Logs

```bash
# Stream logs in real-time
airflow tasks logs olist_data_pipeline s3_to_bronze 2026-01-01 -f
```

### Reset DAG State

```bash
# Delete all task instances (if you need a clean start)
airflow dags delete olist_data_pipeline
```

---

## Production Deployment

### Use PostgreSQL instead of SQLite

SQLite doesn't support concurrent access. For production:

```bash
# Install PostgreSQL adapter
pip install psycopg2-binary

# Edit airflow.cfg
[core]
sql_alchemy_conn = postgresql://user:password@localhost/airflow_db
```

### Use CeleryExecutor for Parallel Execution

LocalExecutor runs tasks sequentially. For parallel runs:

```bash
pip install celery redis

# Edit airflow.cfg
[core]
executor = CeleryExecutor

[celery]
broker_url = redis://localhost:6379/0
result_backend = postgresql://user:password@localhost/airflow_db
```

Then run:

```bash
# Terminal 1: Scheduler
airflow scheduler

# Terminal 2: Webserver
airflow webserver

# Terminal 3+: Celery workers
airflow celery worker
```

### Monitor with Cloud Services

- **AWS MWAA** (Managed Workflows for Apache Airflow)
- **GCP Cloud Composer** (Fully managed Airflow)
- **Astronomer** (Airflow as a service)

---

## Troubleshooting

### DAG not appearing in UI

1. Check `dags/` folder location
2. Verify DAG syntax:
   ```bash
   airflow dags validate
   ```
3. Restart scheduler:
   ```bash
   pkill -f "airflow scheduler"
   airflow scheduler
   ```

### Task fails with "module not found"

Ensure all imports work from project root:

```bash
python -c "import sys; sys.path.insert(0, '.'); from data_ingestion import s3_to_bronze"
```

### Connection/credential errors

Airflow reads from `.env` via `load_dotenv()`. Verify:

```bash
cat .env | grep SNOWFLAKE_USER
```

If still failing, set as Airflow Variables (web UI → Admin → Variables):

```
Key: SNOWFLAKE_USER
Value: PRATIK99
```

Then use in DAG:

```python
from airflow.models import Variable
user = Variable.get("SNOWFLAKE_USER")
```

---

## File Structure

```
Dataengineer/
├── dags/
│   └── olist_pipeline_dag.py          # Main DAG orchestration
├── data_ingestion/
│   ├── s3_to_bronze.py
│   ├── quality_checks.py
│   ├── data_cleaning.py
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   ├── gold_to_s3.py
│   ├── send_report.py
│   ├── send_silver_report.py
│   └── requirements.txt
├── .airflow/                           # Airflow metadata (created by init)
│   ├── airflow.db
│   ├── airflow.cfg
│   └── logs/
├── .env                                # Credentials (NOT committed)
├── AIRFLOW_SETUP.md                    # This file
└── README.md
```

---

## Next Steps

1. ✅ Install dependencies: `pip install -r data_ingestion/requirements.txt`
2. ✅ Initialize Airflow: `airflow db init`
3. ✅ Create admin user: `airflow users create ...`
4. ✅ Start scheduler & webserver
5. ✅ Trigger `olist_data_pipeline` DAG manually
6. ✅ Monitor in Airflow UI at http://localhost:8080
7. ✅ View logs for each task
8. ✅ Set up email alerts (optional)
9. ✅ Deploy to production (MWAA / Composer / Astronomer)

---

## Reference

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow CLI Commands](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html)
- [Snowflake Airflow Provider](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/)
- [Cron Expression Generator](https://crontab.guru/)
