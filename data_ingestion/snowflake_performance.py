"""
snowflake_performance.py

One-time setup for Snowflake performance optimisations:

  1. Warehouse auto-suspend (60s idle) + auto-resume
     -> Saves cost when pipeline is not running

  2. Clustering keys on Gold fact & aggregate tables
     -> Speeds up Power BI queries that filter on date, state, category

  3. Snowpipe per Bronze table (AUTO_INGEST = TRUE)
     -> New files dropped in S3 are loaded automatically without
        running s3_to_bronze.py manually

After running, the script prints the SQS ARN for each pipe.
You must add that ARN as an S3 Event Notification in the AWS console
(one-time step) to complete the Snowpipe wiring.

Usage:
    python data_ingestion/snowflake_performance.py
"""

import json
import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

DB        = os.getenv("SNOWFLAKE_DATABASE")
BRONZE    = "BRONZE"
GOLD      = "GOLD"
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PATH   = os.getenv("S3_PATH")
AWS_KEY   = os.getenv("AWS_KEY_ID")
AWS_SEC   = os.getenv("AWS_SECRET_KEY")

SNOWFLAKE_CONFIG = {
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": WAREHOUSE,
    "database":  DB,
    "schema":    BRONZE,
}

# Bronze tables and their S3 file pattern
BRONZE_TABLES = {
    "olist_customers":                  ".*olist_customers_dataset.*\\.csv",
    "olist_geolocation":                ".*olist_geolocation_dataset.*\\.csv",
    "olist_order_items":                ".*olist_order_items_dataset.*\\.csv",
    "olist_order_payments":             ".*olist_order_payments_dataset.*\\.csv",
    "olist_order_reviews":              ".*olist_order_reviews_dataset.*\\.csv",
    "olist_orders":                     ".*olist_orders_dataset.*\\.csv",
    "olist_products":                   ".*olist_products_dataset.*\\.csv",
    "product_category_name_translation": ".*product_category_name_translation.*\\.csv",
}

# Gold tables and their clustering columns
GOLD_CLUSTERING = {
    "fact_orders":              "(order_date_key)",
    "fact_order_items":         "(order_date_key)",
    "master_table":             "(order_date_key, customer_state)",
    "agg_revenue_by_category":  "(year, month)",
    "agg_revenue_by_state":     "(year, month, customer_state)",
    "agg_seller_performance":   "(seller_key)",
    "ml_delivery_features":     "(order_date_key, customer_state)",
    "ml_review_features":       "(order_date_key)",
}


def get_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


# ── 1. Warehouse auto-suspend ─────────────────────────────────────────────────

def setup_warehouse(cursor):
    print("[1] Configuring warehouse auto-suspend & auto-resume ...")
    cursor.execute(f"""
        ALTER WAREHOUSE {WAREHOUSE}
        SET AUTO_SUSPEND = 60
            AUTO_RESUME  = TRUE
    """)
    print(f"    {WAREHOUSE}: AUTO_SUSPEND = 60s, AUTO_RESUME = TRUE\n")


# ── 2. Clustering keys on Gold tables ─────────────────────────────────────────

def setup_clustering(cursor):
    print("[2] Adding clustering keys to GOLD tables ...")
    for table, keys in GOLD_CLUSTERING.items():
        full = f"{DB}.{GOLD}.{table}"
        cursor.execute(f"ALTER TABLE {full} CLUSTER BY {keys}")
        print(f"    {table:<45}  CLUSTER BY {keys}")
    print()


# ── 3. Snowpipe per Bronze table ──────────────────────────────────────────────

def setup_snowpipe(cursor):
    print("[3] Creating Snowpipes for Bronze tables (AUTO_INGEST = TRUE) ...")
    print("    Stage  : olist_s3_stage")
    print(f"    Bucket : s3://{S3_BUCKET}/{S3_PATH}\n")

    sqs_arns = {}

    for table, pattern in BRONZE_TABLES.items():
        pipe_name = f"{DB}.{BRONZE}.{table}_pipe"

        cursor.execute(f"""
            CREATE OR REPLACE PIPE {pipe_name}
                AUTO_INGEST = TRUE
            AS
            COPY INTO {DB}.{BRONZE}.{table}
            FROM @{DB}.{BRONZE}.olist_s3_stage
            FILE_FORMAT = (FORMAT_NAME = '{DB}.{BRONZE}.csv_format')
            PATTERN     = '{pattern}'
            ON_ERROR    = 'CONTINUE'
        """)

        # Fetch the SQS notification channel ARN via SHOW PIPES
        cursor.execute(f"SHOW PIPES LIKE '{table}_pipe' IN SCHEMA {DB}.{BRONZE}")
        row = cursor.fetchone()
        # notification_channel is column index 10 in SHOW PIPES output
        sqs_arn = row[10] if row and row[10] else "N/A"
        sqs_arns[table] = sqs_arn

        print(f"    {table}_pipe")
        print(f"      SQS ARN : {sqs_arn}\n")

    return sqs_arns


# ── AWS S3 config instructions ────────────────────────────────────────────────

def print_aws_instructions(sqs_arns):
    print("=" * 70)
    print("  ACTION REQUIRED — Configure S3 Event Notifications in AWS")
    print("=" * 70)
    print("""
  For each pipe below, add an S3 Event Notification in the AWS Console:

  Steps:
    1. Go to AWS Console -> S3 -> awsdatapratik -> Properties
    2. Scroll to "Event notifications" -> Create event notification
    3. Set:
         Event types : s3:ObjectCreated:*
         Prefix      : baselayer/
         Destination : SQS Queue -> paste the ARN below
    4. Save

  Since all pipes share the same S3 bucket prefix you can use a SINGLE
  S3 notification pointing to any one of the SQS ARNs — Snowflake will
  route the file to the correct pipe based on the PATTERN filter.

  Recommended: use the olist_orders_pipe SQS ARN as the single endpoint.

  Pipe SQS ARNs:
""")
    for table, arn in sqs_arns.items():
        print(f"    {table+'_pipe':<52}  {arn}")
    print()


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(cursor):
    print("\n-- Performance Setup Summary -----------------------------------")

    print("\n  Warehouse")
    cursor.execute(f"SHOW WAREHOUSES LIKE '{WAREHOUSE}'")
    row = cursor.fetchone()
    if row:
        print(f"    {WAREHOUSE}: auto_suspend={row[10]}s  auto_resume={row[11]}")

    print("\n  Clustering Keys (GOLD)")
    for table in GOLD_CLUSTERING:
        cursor.execute(
            f"SELECT CLUSTERING_KEY FROM information_schema.tables "
            f"WHERE table_schema='{GOLD}' AND table_name=UPPER('{table}')"
        )
        row = cursor.fetchone()
        key = row[0] if row and row[0] else "N/A"
        print(f"    {table:<45}  {key}")

    print("\n  Snowpipes (BRONZE)")
    cursor.execute(f"SHOW PIPES IN SCHEMA {DB}.{BRONZE}")
    pipes = cursor.fetchall()
    for p in pipes:
        print(f"    {p[1]}")


def main():
    print(f"Snowflake Performance Setup — {DB}\n")
    conn   = get_connection()
    cursor = conn.cursor()
    try:
        setup_warehouse(cursor)
        setup_clustering(cursor)
        sqs_arns = setup_snowpipe(cursor)
        print_aws_instructions(sqs_arns)
        print_summary(cursor)
        print("\nSetup complete.")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
