import os
import argparse
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PATH = os.getenv("S3_PATH")
AWS_KEY_ID = os.getenv("AWS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

TABLES = {
    "olist_customers_dataset.csv": {
        "table": "olist_customers",
        "columns": """
            customer_id              STRING,
            customer_unique_id       STRING,
            customer_zip_code_prefix STRING,
            customer_city            STRING,
            customer_state           STRING
        """,
        "watermark_col": None,
    },
    "olist_geolocation_dataset.csv": {
        "table": "olist_geolocation",
        "columns": """
            geolocation_zip_code_prefix STRING,
            geolocation_lat             FLOAT,
            geolocation_lng             FLOAT,
            geolocation_city            STRING,
            geolocation_state           STRING
        """,
        "watermark_col": None,
    },
    "olist_order_items_dataset.csv": {
        "table": "olist_order_items",
        "columns": """
            order_id            STRING,
            order_item_id       INT,
            product_id          STRING,
            seller_id           STRING,
            shipping_limit_date TIMESTAMP,
            price               FLOAT,
            freight_value       FLOAT
        """,
        "watermark_col": "shipping_limit_date",
    },
    "olist_order_payments_dataset.csv": {
        "table": "olist_order_payments",
        "columns": """
            order_id             STRING,
            payment_sequential   INT,
            payment_type         STRING,
            payment_installments INT,
            payment_value        FLOAT
        """,
        "watermark_col": None,
    },
    "olist_order_reviews_dataset.csv": {
        "table": "olist_order_reviews",
        "columns": """
            review_id               STRING,
            order_id                STRING,
            review_score            INT,
            review_comment_title    STRING,
            review_comment_message  STRING,
            review_creation_date    TIMESTAMP,
            review_answer_timestamp TIMESTAMP
        """,
        "watermark_col": "review_creation_date",
    },
    "olist_orders_dataset.csv": {
        "table": "olist_orders",
        "columns": """
            order_id                      STRING,
            customer_id                   STRING,
            order_status                  STRING,
            order_purchase_timestamp      TIMESTAMP,
            order_approved_at             TIMESTAMP,
            order_delivered_carrier_date  TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP
        """,
        "watermark_col": "order_purchase_timestamp",
    },
    "olist_products_dataset.csv": {
        "table": "olist_products",
        "columns": """
            product_id                 STRING,
            product_category_name      STRING,
            product_name_lenght        INT,
            product_description_lenght INT,
            product_photos_qty         INT,
            product_weight_g           INT,
            product_length_cm          INT,
            product_height_cm          INT,
            product_width_cm           INT
        """,
        "watermark_col": None,
    },
    "product_category_name_translation.csv": {
        "table": "product_category_name_translation",
        "columns": """
            product_category_name         STRING,
            product_category_name_english STRING
        """,
        "watermark_col": None,
    },
}


def get_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


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
            AWS_KEY_ID = '{AWS_KEY_ID}'
            AWS_SECRET_KEY = '{AWS_SECRET_KEY}'
        )
        FILE_FORMAT = csv_format
    """)


def get_watermark(cursor, table, watermark_col):
    cursor.execute(f"SELECT MAX({watermark_col}) FROM {table}")
    row = cursor.fetchone()
    return row[0] if row else None


def load_table(cursor, filename, table, columns, watermark_col, full_refresh):
    if full_refresh:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cursor.execute(f"CREATE TABLE {table} ({columns})")
        force_clause = "FORCE = TRUE"
    else:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns})")
        force_clause = ""

    # Show watermark so you can verify what range is already loaded
    if watermark_col and not full_refresh:
        watermark = get_watermark(cursor, table, watermark_col)
        if watermark:
            print(f"  [{table}] last loaded watermark: {watermark}")

    cursor.execute(f"""
        COPY INTO {table}
        FROM @olist_s3_stage/{filename}
        FILE_FORMAT = (FORMAT_NAME = 'csv_format')
        ON_ERROR = 'CONTINUE'
        {force_clause}
    """)

    result = cursor.fetchone()
    status = result[1]
    rows_loaded = result[4]
    errors = result[5]
    print(f"  {table}: {status} — {rows_loaded} rows loaded, {errors} errors")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Drop and reload all tables from scratch. Default is incremental.",
    )
    args = parser.parse_args()

    mode = "FULL REFRESH" if args.full_refresh else "INCREMENTAL"
    print(f"Mode: {mode}")
    print("Setting up stage...")

    conn = get_connection()
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
                config["watermark_col"],
                args.full_refresh,
            )
        print("\nAll tables loaded successfully!")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
