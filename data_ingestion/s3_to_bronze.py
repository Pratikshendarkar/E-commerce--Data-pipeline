"""
s3_to_bronze.py

Loads CSV files from S3 into Snowflake BRONZE schema.

Load strategy:
  - Incremental (default):
      * New file  -> append rows
      * Same file already loaded -> delete existing rows for that file
                                    then reload (delete + insert)
  - Full refresh (--full-refresh):
      * Drop and recreate every table, reload all files from scratch

Each table gets two metadata columns automatically:
    _source_file  VARCHAR   -- S3 path of the source file (METADATA$FILENAME)
    _loaded_at    TIMESTAMP -- timestamp of the load run

Usage:
    python data_ingestion/s3_to_bronze.py                 # incremental
    python data_ingestion/s3_to_bronze.py --full-refresh  # full reload
"""

import os
import argparse
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONFIG = {
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database":  os.getenv("SNOWFLAKE_DATABASE"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA"),
}

S3_BUCKET     = os.getenv("S3_BUCKET")
S3_PATH       = os.getenv("S3_PATH")
AWS_KEY_ID    = os.getenv("AWS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

# col_count = number of data columns (excludes _source_file, _loaded_at)
TABLES = {
    "olist_customers_dataset.csv": {
        "table": "olist_customers",
        "col_count": 5,
        "columns": """
            customer_id              STRING,
            customer_unique_id       STRING,
            customer_zip_code_prefix STRING,
            customer_city            STRING,
            customer_state           STRING,
            _source_file             VARCHAR,
            _loaded_at               TIMESTAMP
        """,
        "watermark_col": None,
    },
    "olist_geolocation_dataset.csv": {
        "table": "olist_geolocation",
        "col_count": 5,
        "columns": """
            geolocation_zip_code_prefix STRING,
            geolocation_lat             FLOAT,
            geolocation_lng             FLOAT,
            geolocation_city            STRING,
            geolocation_state           STRING,
            _source_file                VARCHAR,
            _loaded_at                  TIMESTAMP
        """,
        "watermark_col": None,
    },
    "olist_order_items_dataset.csv": {
        "table": "olist_order_items",
        "col_count": 7,
        "columns": """
            order_id            STRING,
            order_item_id       INT,
            product_id          STRING,
            seller_id           STRING,
            shipping_limit_date TIMESTAMP,
            price               FLOAT,
            freight_value       FLOAT,
            _source_file        VARCHAR,
            _loaded_at          TIMESTAMP
        """,
        "watermark_col": "shipping_limit_date",
    },
    "olist_order_payments_dataset.csv": {
        "table": "olist_order_payments",
        "col_count": 5,
        "columns": """
            order_id             STRING,
            payment_sequential   INT,
            payment_type         STRING,
            payment_installments INT,
            payment_value        FLOAT,
            _source_file         VARCHAR,
            _loaded_at           TIMESTAMP
        """,
        "watermark_col": None,
    },
    "olist_order_reviews_dataset.csv": {
        "table": "olist_order_reviews",
        "col_count": 7,
        "columns": """
            review_id               STRING,
            order_id                STRING,
            review_score            INT,
            review_comment_title    STRING,
            review_comment_message  STRING,
            review_creation_date    TIMESTAMP,
            review_answer_timestamp TIMESTAMP,
            _source_file            VARCHAR,
            _loaded_at              TIMESTAMP
        """,
        "watermark_col": "review_creation_date",
    },
    "olist_orders_dataset.csv": {
        "table": "olist_orders",
        "col_count": 8,
        "columns": """
            order_id                      STRING,
            customer_id                   STRING,
            order_status                  STRING,
            order_purchase_timestamp      TIMESTAMP,
            order_approved_at             TIMESTAMP,
            order_delivered_carrier_date  TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP,
            _source_file                  VARCHAR,
            _loaded_at                    TIMESTAMP
        """,
        "watermark_col": "order_purchase_timestamp",
    },
    "olist_products_dataset.csv": {
        "table": "olist_products",
        "col_count": 9,
        "columns": """
            product_id                 STRING,
            product_category_name      STRING,
            product_name_lenght        INT,
            product_description_lenght INT,
            product_photos_qty         INT,
            product_weight_g           INT,
            product_length_cm          INT,
            product_height_cm          INT,
            product_width_cm           INT,
            _source_file               VARCHAR,
            _loaded_at                 TIMESTAMP
        """,
        "watermark_col": None,
    },
    "product_category_name_translation.csv": {
        "table": "product_category_name_translation",
        "col_count": 2,
        "columns": """
            product_category_name         STRING,
            product_category_name_english STRING,
            _source_file                  VARCHAR,
            _loaded_at                    TIMESTAMP
        """,
        "watermark_col": None,
    },
}


def get_connection():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    warehouse = SNOWFLAKE_CONFIG["warehouse"]
    conn.cursor().execute(f"ALTER WAREHOUSE {warehouse} SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE")
    return conn


def setup_stage(cursor):
    cursor.execute("""
        CREATE OR REPLACE FILE FORMAT csv_format
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
        NULL_IF = ('NULL', 'null', '')
        TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
        ENCODING = 'ISO-8859-1'
    """)
    cursor.execute(f"""
        CREATE OR REPLACE STAGE olist_s3_stage
        URL = 's3://{S3_BUCKET}/{S3_PATH}'
        CREDENTIALS = (
            AWS_KEY_ID     = '{AWS_KEY_ID}'
            AWS_SECRET_KEY = '{AWS_SECRET_KEY}'
        )
        FILE_FORMAT = csv_format
    """)


def ensure_metadata_columns(cursor, table):
    """Add _source_file / _loaded_at if the table exists but predates this feature."""
    cursor.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = UPPER('{SNOWFLAKE_CONFIG["schema"]}')
          AND table_name   = UPPER('{table}')
          AND column_name  = '_SOURCE_FILE'
    """)
    if cursor.fetchone()[0] == 0:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN _source_file VARCHAR")
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN _loaded_at  TIMESTAMP")
        print(f"  [{table}] metadata columns added (_source_file, _loaded_at)")


def file_already_loaded(cursor, table, filename):
    """Returns True if any row in the table came from this filename."""
    cursor.execute(f"""
        SELECT COUNT(*) FROM {table}
        WHERE _source_file LIKE '%{filename}%'
    """)
    return cursor.fetchone()[0] > 0


def get_watermark(cursor, table, watermark_col):
    cursor.execute(f"SELECT MAX({watermark_col}) FROM {table}")
    row = cursor.fetchone()
    return row[0] if row else None


def copy_into(cursor, table, filename, col_count, force=False):
    """Run COPY INTO using a SELECT so METADATA$FILENAME can be captured."""
    col_refs   = ", ".join(f"${i}" for i in range(1, col_count + 1))
    force_flag = "FORCE = TRUE" if force else ""

    cursor.execute(f"""
        COPY INTO {table}
        FROM (
            SELECT {col_refs},
                   METADATA$FILENAME,
                   CURRENT_TIMESTAMP()
            FROM @olist_s3_stage/{filename}
        )
        FILE_FORMAT = (FORMAT_NAME = 'csv_format')
        ON_ERROR    = 'CONTINUE'
        {force_flag}
    """)
    results = cursor.fetchall()
    return results[0] if results else (None, 'LOADED', 0, 0, 0, 0)


def load_table(cursor, filename, table, columns, col_count, watermark_col, full_refresh):
    if full_refresh:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cursor.execute(f"CREATE TABLE {table} ({columns})")
        result = copy_into(cursor, table, filename, col_count, force=True)
        status      = result[1] if len(result) > 1 else "LOADED"
        rows_loaded = result[3] if len(result) > 3 else 0
        errors      = result[5] if len(result) > 5 else 0
        print(f"  {table}: {status} — {rows_loaded} rows loaded, {errors} errors  [FULL REFRESH]")
        return

    # ── Incremental logic ─────────────────────────────────────────────────────
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns})")
    ensure_metadata_columns(cursor, table)

    if watermark_col:
        watermark = get_watermark(cursor, table, watermark_col)
        if watermark:
            print(f"  [{table}] last watermark: {watermark}")

    already_loaded = file_already_loaded(cursor, table, filename)

    if already_loaded:
        # Delete existing records for this file then reload
        cursor.execute(f"DELETE FROM {table} WHERE _source_file LIKE '%{filename}%'")
        result = copy_into(cursor, table, filename, col_count, force=True)
        action = "DELETE + RELOAD"
    else:
        # New file — append
        result = copy_into(cursor, table, filename, col_count, force=False)
        action = "APPEND"

    status      = result[1] if len(result) > 1 else "LOADED"
    rows_loaded = result[3] if len(result) > 3 else 0
    errors      = result[5] if len(result) > 5 else 0
    print(f"  {table}: {status} — {rows_loaded} rows loaded, {errors} errors  [{action}]")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Drop and reload all tables from scratch.",
    )
    args = parser.parse_args()

    mode = "FULL REFRESH" if args.full_refresh else "INCREMENTAL"
    print(f"Mode: {mode}")
    print("Setting up stage...")

    conn   = get_connection()
    cursor = conn.cursor()
    try:
        setup_stage(cursor)
        print("Loading tables...\n")
        for filename, config in TABLES.items():
            load_table(
                cursor,
                filename,
                config["table"],
                config["columns"],
                config["col_count"],
                config["watermark_col"],
                args.full_refresh,
            )
        print("\nAll tables loaded successfully!")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
