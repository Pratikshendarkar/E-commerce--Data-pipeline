# Running Airflow in Docker

Quick start guide for orchestrating the Olist pipeline using Docker containers.

## Prerequisites

- Docker Desktop installed and running
- `.env` file configured with Snowflake & AWS credentials
- `dags/olist_pipeline_dag.py` in place (created by Airflow setup)

## Quick Start

### 1. Start Airflow Services

```bash
# Initialize Airflow DB and create admin user, then start all services
docker-compose up airflow-init airflow-scheduler airflow-webserver
```

This starts:
- **airflow-init** — One-time DB initialization + admin user creation
- **airflow-scheduler** — Orchestrates DAG execution
- **airflow-webserver** — Web UI at http://localhost:8080

### 2. Access Airflow Web UI

Open browser: **http://localhost:8080**

**Login Credentials:**
- Username: `admin`
- Password: `admin`

### 3. Trigger the Pipeline DAG

1. Click **DAGs** in sidebar
2. Find `olist_data_pipeline`
3. Click the play button (▶) → **Trigger DAG**
4. Watch tasks execute in real-time

### 4. View Logs

Click on any task → **Logs** tab to see full output

### 5. Stop Services

```bash
docker-compose down
```

---

## Services

| Service | Role | Port | Notes |
|---------|------|------|-------|
| `airflow-init` | Initialize DB | — | Runs once, exits |
| `airflow-scheduler` | Orchestration engine | 8793 | Runs continuously |
| `airflow-webserver` | Web UI + REST API | 8080 | Access dashboard |
| `s3_to_bronze` | Pipeline stage | — | Manual run only |
| `bronze_to_silver` | Pipeline stage | — | Manual run only |
| `data_cleaning` | Pipeline stage | — | Manual run only |
| `quality_checks` | Pipeline stage | — | Manual run only |
| `silver_to_gold` | Pipeline stage | — | Manual run only |
| `gold_to_s3` | Pipeline stage | — | Manual run only |
| `send_report` | Pipeline stage | — | Manual run only |
| `send_silver_report` | Pipeline stage | — | Manual run only |

---

## Running Pipeline Stages Manually

If you want to bypass Airflow and run individual stages:

```bash
# Run s3_to_bronze standalone
docker-compose run s3_to_bronze

# Run bronze_to_silver standalone
docker-compose run bronze_to_silver

# Run full pipeline end-to-end
docker-compose run s3_to_bronze && \
docker-compose run quality_checks && \
docker-compose run data_cleaning && \
docker-compose run bronze_to_silver && \
docker-compose run silver_to_gold && \
docker-compose run gold_to_s3 && \
docker-compose run send_report && \
docker-compose run send_silver_report
```

---

## Configuration

### Change Admin Password

```bash
docker-compose exec airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password <new_password>
```

### Change Scheduling

Edit `dags/olist_pipeline_dag.py`:

```python
schedule_interval="0 2 * * *",  # Change this line
```

Then restart scheduler:

```bash
docker-compose restart airflow-scheduler
```

### Change DAG Folder

Default: `./dags/`

To mount a different folder:

```yaml
volumes:
  - /path/to/other/dags:/app/dags  # Change this
```

### Environment Variables

All services read from `.env`:

```env
SNOWFLAKE_USER=PRATIK99
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ACCOUNT=...
AWS_KEY_ID=...
AWS_SECRET_KEY=...
S3_BUCKET=...
EMAIL_SENDER=...
# etc.
```

If `.env` is missing, services will fail with credential errors.

---

## Troubleshooting

### DAG not showing in Airflow UI

1. Check DAG file is in `./dags/` folder
2. Verify syntax:
   ```bash
   docker-compose exec airflow-webserver airflow dags validate
   ```
3. Restart scheduler:
   ```bash
   docker-compose restart airflow-scheduler
   ```

### "Permission denied" errors

Ensure `.env` file has correct permissions:

```bash
chmod 600 .env
```

And contains all required variables:

```bash
cat .env | grep SNOWFLAKE_USER
cat .env | grep AWS_KEY_ID
```

### Docker logs

View detailed logs:

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler

# Last 50 lines
docker-compose logs --tail 50 airflow-scheduler
```

### Out of disk space

SQLite database grows over time. Clean up:

```bash
# Remove old log files
rm -rf logs/

# Reinitialize database (WARNING: loses all history)
rm -rf .airflow/
docker-compose up airflow-init
```

### Can't connect to Snowflake

Check credentials in `.env`:

```bash
docker-compose exec airflow-webserver python -c "
import os
from dotenv import load_dotenv
load_dotenv()
print(f'User: {os.getenv(\"SNOWFLAKE_USER\")}')
print(f'Account: {os.getenv(\"SNOWFLAKE_ACCOUNT\")}')
print(f'Database: {os.getenv(\"SNOWFLAKE_DATABASE\")}')
"
```

---

## Production Considerations

### Use PostgreSQL instead of SQLite

```yaml
airflow-scheduler:
  environment:
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql://user:pass@postgres:5432/airflow

services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-data:/var/lib/postgresql/data

volumes:
  postgres-data:
```

### Persist Logs

Current setup stores logs in `./logs/` (volume mount).

For cloud storage (S3, GCS):

```python
# Edit dags/olist_pipeline_dag.py
AIRFLOW__LOGGING__REMOTE_LOGGING = True
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID = s3_default
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER = s3://your-bucket/airflow-logs
```

### Health Checks

Both services have health checks. View status:

```bash
docker-compose ps

# Example output:
# NAME                    STATUS
# airflow-webserver       Up 2 minutes (healthy)
# airflow-scheduler       Up 2 minutes (healthy)
```

---

## File Structure

```
Dataengineer/
├── dags/
│   └── olist_pipeline_dag.py        # DAG definition
├── data_ingestion/
│   ├── s3_to_bronze.py
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   ├── gold_to_s3.py
│   ├── quality_checks.py
│   ├── data_cleaning.py
│   ├── send_report.py
│   ├── send_silver_report.py
│   └── requirements.txt
├── .airflow/                        # Airflow metadata (auto-created)
│   ├── airflow.db                   # SQLite database
│   ├── airflow.cfg                  # Config file
│   └── logs/
├── logs/                            # Pipeline logs (mounted volume)
├── .env                             # Credentials (NOT in git)
├── Dockerfile                       # Container image definition
├── docker-compose.yml               # Docker orchestration
├── DOCKER_AIRFLOW.md                # This file
└── README.md
```

---

## Common Commands

```bash
# Start all services
docker-compose up

# Start specific services (interactive)
docker-compose up -d airflow-scheduler airflow-webserver

# Stop all services
docker-compose down

# Remove volumes (WARNING: deletes DB & history)
docker-compose down -v

# View service logs
docker-compose logs -f <service-name>

# Run command in container
docker-compose exec airflow-webserver airflow <command>

# Rebuild image (after Dockerfile changes)
docker-compose build

# Test DAG validity
docker-compose exec airflow-webserver airflow dags validate

# List all DAGs
docker-compose exec airflow-webserver airflow dags list

# Trigger DAG from CLI
docker-compose exec airflow-webserver airflow dags trigger olist_data_pipeline
```

---

## Next Steps

1. ✅ Run `docker-compose up airflow-init airflow-scheduler airflow-webserver`
2. ✅ Open http://localhost:8080
3. ✅ Log in with `admin` / `admin`
4. ✅ Find `olist_data_pipeline` DAG
5. ✅ Click play button to trigger
6. ✅ Watch tasks execute
7. ✅ View logs for debugging
8. ✅ Configure scheduling in `dags/olist_pipeline_dag.py`

---

## Reference

- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Reference](https://docs.docker.com/compose/compose-file/)
- [Airflow Docker Documentation](https://airflow.apache.org/docs/docker-compose/)
