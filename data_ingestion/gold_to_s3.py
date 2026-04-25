"""
gold_to_s3.py

Exports all GOLD layer tables from Snowflake to S3.

Output path: s3://awsdatapratik/finaloutput/<table_name>/
Format      : CSV with header, no compression

Usage:
    python data_ingestion/gold_to_s3.py
"""

import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

DB   = os.getenv("SNOWFLAKE_DATABASE")
GOLD = "GOLD"

SNOWFLAKE_CONFIG = {
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database":  DB,
    "schema":    GOLD,
}

S3_BUCKET    = os.getenv("S3_BUCKET")
AWS_KEY_ID   = os.getenv("AWS_KEY_ID")
AWS_SECRET   = os.getenv("AWS_SECRET_KEY")
S3_BASE_PATH = "finaloutput"

GOLD_TABLES = [
    # Dimensions
    "dim_date",
    "dim_geolocation",
    "dim_customers",
    "dim_sellers",
    "dim_products",
    # Facts
    "fact_orders",
    "fact_order_items",
    # Aggregates
    "agg_revenue_by_category",
    "agg_revenue_by_state",
    "agg_seller_performance",
    "agg_customer_cohorts",
    # ML Feature Tables
    "ml_customer_features",
    "ml_seller_features",
    "ml_delivery_features",
    "ml_review_features",
    # Master
    "master_table",
]


def get_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def export_table(cursor, table):
    s3_path = f"s3://{S3_BUCKET}/{S3_BASE_PATH}/{table}/"

    cursor.execute(f"""
        COPY INTO '{s3_path}'
        FROM {DB}.{GOLD}.{table}
        CREDENTIALS = (
            AWS_KEY_ID      = '{AWS_KEY_ID}'
            AWS_SECRET_KEY  = '{AWS_SECRET}'
        )
        FILE_FORMAT = (
            TYPE                        = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            COMPRESSION                 = NONE
            NULL_IF                     = ('NULL', 'null', '')
        )
        HEADER        = TRUE
        OVERWRITE     = TRUE
        SINGLE        = FALSE
        MAX_FILE_SIZE = 104857600
    """)

    rows = cursor.fetchall()
    # COPY INTO unload returns: (rows_unloaded, input_bytes, output_bytes)
    total_rows = sum(r[0] for r in rows) if rows else 0
    files      = len(rows)
    print(f"  {table:<45} {total_rows:>10,} rows  ->  {files} file(s)  ->  {s3_path}")


def main():
    print(f"Exporting {len(GOLD_TABLES)} GOLD tables to s3://{S3_BUCKET}/{S3_BASE_PATH}/\n")

    conn   = get_connection()
    cursor = conn.cursor()
    try:
        for table in GOLD_TABLES:
            export_table(cursor, table)
        print(f"\nAll tables exported successfully to s3://{S3_BUCKET}/{S3_BASE_PATH}/")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
