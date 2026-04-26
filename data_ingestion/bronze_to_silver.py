"""
bronze_to_silver.py

Reads from BRONZE schema, removes bad/invalid records, runs quality checks,
and writes clean tables into SILVER schema with a _stage suffix.

Removal strategy (not capping/flagging):
  - olist_order_reviews   : remove duplicate review_id rows (keep most complete)
  - olist_order_payments  : remove payment_value <= 0 and payment_installments < 1
  - olist_orders          : remove rows with date sequence violations
  - olist_order_items     : remove IQR outliers on price and freight_value
  - olist_order_payments  : remove IQR outliers on payment_value
  - olist_products        : remove IQR outliers on dimensions / weight
  - olist_customers       : copy as-is (no issues found)
  - olist_geolocation     : copy as-is (no issues found)
  - product_category_name_translation : copy as-is

Usage:
    python data_ingestion/bronze_to_silver.py
"""

import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

DB       = os.getenv("SNOWFLAKE_DATABASE")
BRONZE   = "BRONZE"
SILVER   = "SILVER"

SNOWFLAKE_CONFIG = {
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database":  DB,
    "schema":    BRONZE,
}

PASS = "PASS"
FAIL = "FAIL"
qc_results = []


def get_connection():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    warehouse = SNOWFLAKE_CONFIG["warehouse"]
    conn.cursor().execute(f"ALTER WAREHOUSE {warehouse} SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE")
    return conn


def b(table):
    return f"{DB}.{BRONZE}.{table}"


def s(table):
    return f"{DB}.{SILVER}.{table}_stage"


# ── Schema setup ──────────────────────────────────────────────────────────────

def create_silver_schema(cursor):
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DB}.{SILVER}")
    print(f"Schema {DB}.{SILVER} ready.\n")


# ── Table transfers ───────────────────────────────────────────────────────────

def transfer_customers(cursor):
    print("[1] olist_customers  ->  olist_customers_stage")
    cursor.execute(f"""
        CREATE OR REPLACE TABLE {s('olist_customers')} AS
        SELECT * FROM {b('olist_customers')}
    """)
    _print_count(cursor, s('olist_customers'))


def transfer_geolocation(cursor):
    print("[2] olist_geolocation  ->  olist_geolocation_stage")
    cursor.execute(f"""
        CREATE OR REPLACE TABLE {s('olist_geolocation')} AS
        SELECT * FROM {b('olist_geolocation')}
    """)
    _print_count(cursor, s('olist_geolocation'))


def transfer_category_translation(cursor):
    print("[3] product_category_name_translation  ->  product_category_name_translation_stage")
    cursor.execute(f"""
        CREATE OR REPLACE TABLE {s('product_category_name_translation')} AS
        SELECT * FROM {b('product_category_name_translation')}
    """)
    _print_count(cursor, s('product_category_name_translation'))


def transfer_products(cursor):
    print("[4] olist_products  ->  olist_products_stage  (remove dimension/weight outliers)")

    dim_cols = ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    filters = []
    for col in dim_cols:
        cursor.execute(f"""
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col}),
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col})
            FROM {b('olist_products')}
            WHERE {col} IS NOT NULL
        """)
        q1, q3 = (float(v) for v in cursor.fetchone())
        iqr = q3 - q1
        lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        filters.append(f"({col} IS NULL OR ({col} >= {lo} AND {col} <= {hi}))")
        print(f"    {col}: bounds [{lo:.1f}, {hi:.1f}]")

    where = " AND ".join(filters)
    cursor.execute(f"""
        CREATE OR REPLACE TABLE {s('olist_products')} AS
        SELECT
            product_id,
            product_category_name,
            product_name_lenght,
            product_description_lenght,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        FROM {b('olist_products')}
        WHERE {where}
    """)
    _print_count(cursor, s('olist_products'))


def transfer_order_reviews(cursor):
    print("[5] olist_order_reviews  ->  olist_order_reviews_stage  (dedup + cascade from orders)")
    cursor.execute(f"""
        CREATE OR REPLACE TABLE {s('olist_order_reviews')} AS
        SELECT
            review_id,
            order_id,
            review_score,
            review_comment_title,
            review_comment_message,
            review_creation_date,
            review_answer_timestamp
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY review_id
                    ORDER BY
                        (CASE WHEN review_comment_message IS NOT NULL THEN 0 ELSE 1 END),
                        review_answer_timestamp DESC
                ) AS rn
            FROM {b('olist_order_reviews')}
            WHERE order_id IN (SELECT order_id FROM {s('olist_orders')})
        )
        WHERE rn = 1
    """)
    _print_count(cursor, s('olist_order_reviews'))


def transfer_orders(cursor):
    print("[6] olist_orders  ->  olist_orders_stage  (remove date-sequence violations)")
    cursor.execute(f"""
        CREATE OR REPLACE TABLE {s('olist_orders')} AS
        SELECT
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        FROM {b('olist_orders')}
        WHERE
            NOT (order_delivered_carrier_date IS NOT NULL
                 AND order_approved_at IS NOT NULL
                 AND order_delivered_carrier_date < order_approved_at)
          AND
            NOT (order_delivered_customer_date IS NOT NULL
                 AND order_delivered_carrier_date IS NOT NULL
                 AND order_delivered_customer_date < order_delivered_carrier_date)
    """)
    _print_count(cursor, s('olist_orders'))


def transfer_order_items(cursor):
    print("[7] olist_order_items  ->  olist_order_items_stage  (remove price/freight outliers)")
    for col in ("price", "freight_value"):
        cursor.execute(f"""
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col}),
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col})
            FROM {b('olist_order_items')}
        """)
        q1, q3 = cursor.fetchone()
        iqr = q3 - q1
        lo, hi = float(q1 - 1.5 * iqr), float(q3 + 1.5 * iqr)
        print(f"    {col}: bounds [{lo:.2f}, {hi:.2f}]")

    cursor.execute(f"""
        CREATE OR REPLACE TABLE {s('olist_order_items')} AS
        WITH bounds AS (
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price)         AS price_q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price)         AS price_q3,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY freight_value) AS freight_q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY freight_value) AS freight_q3
            FROM {b('olist_order_items')}
        )
        SELECT
            oi.order_id,
            oi.order_item_id,
            oi.product_id,
            oi.seller_id,
            oi.shipping_limit_date,
            oi.price,
            oi.freight_value
        FROM {b('olist_order_items')} oi
        CROSS JOIN bounds b
        WHERE
            oi.order_id   IN (SELECT order_id   FROM {s('olist_orders')})
          AND
            oi.product_id IN (SELECT product_id FROM {s('olist_products')})
          AND
            oi.price         BETWEEN b.price_q1   - 1.5*(b.price_q3   - b.price_q1)
                                 AND b.price_q3   + 1.5*(b.price_q3   - b.price_q1)
          AND
            oi.freight_value BETWEEN b.freight_q1 - 1.5*(b.freight_q3 - b.freight_q1)
                                 AND b.freight_q3 + 1.5*(b.freight_q3 - b.freight_q1)
    """)
    _print_count(cursor, s('olist_order_items'))


def transfer_order_payments(cursor):
    print("[8] olist_order_payments  ->  olist_order_payments_stage  (remove invalid + outlier payments)")
    cursor.execute(f"""
        SELECT
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY payment_value),
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY payment_value)
        FROM {b('olist_order_payments')}
        WHERE payment_value > 0
    """)
    q1, q3 = cursor.fetchone()
    iqr = q3 - q1
    upper = float(q3 + 1.5 * iqr)
    print(f"    payment_value: upper bound {upper:.2f}")

    cursor.execute(f"""
        CREATE OR REPLACE TABLE {s('olist_order_payments')} AS
        SELECT
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value
        FROM {b('olist_order_payments')}
        WHERE order_id IN (SELECT order_id FROM {s('olist_orders')})
          AND payment_value > 0
          AND payment_installments >= 1
          AND payment_value <= {upper}
    """)
    _print_count(cursor, s('olist_order_payments'))


# ── Quality checks on SILVER tables ──────────────────────────────────────────

def qc(cursor, name, query):
    cursor.execute(query)
    val = cursor.fetchone()[0]
    status = PASS if val == 0 else FAIL
    qc_results.append((status, name, val))


def run_quality_checks(cursor):
    print("\n-- Running quality checks on SILVER tables ---------------------")

    sv = lambda t: s(t)

    # Null checks
    qc(cursor, "customers: no null customer_id",
       f"SELECT COUNT(*) FROM {sv('olist_customers')} WHERE customer_id IS NULL")
    qc(cursor, "orders: no null order_id",
       f"SELECT COUNT(*) FROM {sv('olist_orders')} WHERE order_id IS NULL")
    qc(cursor, "orders: no null customer_id",
       f"SELECT COUNT(*) FROM {sv('olist_orders')} WHERE customer_id IS NULL")
    qc(cursor, "order_items: no null order_id",
       f"SELECT COUNT(*) FROM {sv('olist_order_items')} WHERE order_id IS NULL")
    qc(cursor, "order_items: no null product_id",
       f"SELECT COUNT(*) FROM {sv('olist_order_items')} WHERE product_id IS NULL")
    qc(cursor, "order_payments: no null order_id",
       f"SELECT COUNT(*) FROM {sv('olist_order_payments')} WHERE order_id IS NULL")
    qc(cursor, "order_reviews: no null review_id",
       f"SELECT COUNT(*) FROM {sv('olist_order_reviews')} WHERE review_id IS NULL")
    qc(cursor, "products: no null product_id",
       f"SELECT COUNT(*) FROM {sv('olist_products')} WHERE product_id IS NULL")

    # Duplicate checks
    qc(cursor, "customers: no duplicate customer_id",
       f"SELECT COUNT(*) FROM (SELECT customer_id FROM {sv('olist_customers')} GROUP BY 1 HAVING COUNT(*)>1)")
    qc(cursor, "orders: no duplicate order_id",
       f"SELECT COUNT(*) FROM (SELECT order_id FROM {sv('olist_orders')} GROUP BY 1 HAVING COUNT(*)>1)")
    qc(cursor, "order_reviews: no duplicate review_id",
       f"SELECT COUNT(*) FROM (SELECT review_id FROM {sv('olist_order_reviews')} GROUP BY 1 HAVING COUNT(*)>1)")
    qc(cursor, "products: no duplicate product_id",
       f"SELECT COUNT(*) FROM (SELECT product_id FROM {sv('olist_products')} GROUP BY 1 HAVING COUNT(*)>1)")
    qc(cursor, "order_items: no duplicate (order_id, order_item_id)",
       f"SELECT COUNT(*) FROM (SELECT order_id, order_item_id FROM {sv('olist_order_items')} GROUP BY 1,2 HAVING COUNT(*)>1)")

    # Referential integrity
    qc(cursor, "RI: orders.customer_id -> customers",
       f"""SELECT COUNT(*) FROM {sv('olist_orders')} o
           LEFT JOIN {sv('olist_customers')} c ON o.customer_id = c.customer_id
           WHERE c.customer_id IS NULL""")
    qc(cursor, "RI: order_items.order_id -> orders",
       f"""SELECT COUNT(*) FROM {sv('olist_order_items')} oi
           LEFT JOIN {sv('olist_orders')} o ON oi.order_id = o.order_id
           WHERE o.order_id IS NULL""")
    qc(cursor, "RI: order_payments.order_id -> orders",
       f"""SELECT COUNT(*) FROM {sv('olist_order_payments')} op
           LEFT JOIN {sv('olist_orders')} o ON op.order_id = o.order_id
           WHERE o.order_id IS NULL""")
    qc(cursor, "RI: order_reviews.order_id -> orders",
       f"""SELECT COUNT(*) FROM {sv('olist_order_reviews')} r
           LEFT JOIN {sv('olist_orders')} o ON r.order_id = o.order_id
           WHERE o.order_id IS NULL""")
    qc(cursor, "RI: order_items.product_id -> products",
       f"""SELECT COUNT(*) FROM {sv('olist_order_items')} oi
           LEFT JOIN {sv('olist_products')} p ON oi.product_id = p.product_id
           WHERE p.product_id IS NULL""")

    # Value checks
    qc(cursor, "order_reviews: review_score between 1 and 5",
       f"SELECT COUNT(*) FROM {sv('olist_order_reviews')} WHERE review_score NOT BETWEEN 1 AND 5")
    qc(cursor, "order_items: price > 0",
       f"SELECT COUNT(*) FROM {sv('olist_order_items')} WHERE price <= 0")
    qc(cursor, "order_items: freight_value >= 0",
       f"SELECT COUNT(*) FROM {sv('olist_order_items')} WHERE freight_value < 0")
    qc(cursor, "order_payments: payment_value > 0",
       f"SELECT COUNT(*) FROM {sv('olist_order_payments')} WHERE payment_value <= 0")
    qc(cursor, "order_payments: payment_installments >= 1",
       f"SELECT COUNT(*) FROM {sv('olist_order_payments')} WHERE payment_installments < 1")

    # Date sanity
    qc(cursor, "orders: carrier_date >= approved_at",
       f"""SELECT COUNT(*) FROM {sv('olist_orders')}
           WHERE order_delivered_carrier_date IS NOT NULL
             AND order_approved_at IS NOT NULL
             AND order_delivered_carrier_date < order_approved_at""")
    qc(cursor, "orders: customer_date >= carrier_date",
       f"""SELECT COUNT(*) FROM {sv('olist_orders')}
           WHERE order_delivered_customer_date IS NOT NULL
             AND order_delivered_carrier_date IS NOT NULL
             AND order_delivered_customer_date < order_delivered_carrier_date""")


# ── Summary ───────────────────────────────────────────────────────────────────

def print_row_counts(cursor):
    silver_tables = [
        "olist_customers",
        "olist_geolocation",
        "olist_order_items",
        "olist_order_payments",
        "olist_order_reviews",
        "olist_orders",
        "olist_products",
        "product_category_name_translation",
    ]
    print("\n-- SILVER Row Counts -------------------------------------------")
    for t in silver_tables:
        cursor.execute(f"SELECT COUNT(*) FROM {s(t)}")
        silver_count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM {b(t)}")
        bronze_count = cursor.fetchone()[0]
        removed = bronze_count - silver_count
        tag = f"  (-{removed:,} removed)" if removed else "  (no rows removed)"
        print(f"  {t+'_stage':<50} {silver_count:>9,}{tag}")


def print_qc_report():
    passed = [r for r in qc_results if r[0] == PASS]
    failed = [r for r in qc_results if r[0] == FAIL]

    print("\n-- Quality Check Results (SILVER) ------------------------------")
    for status, name, val in qc_results:
        tag = f"  ({val} violations)" if status == FAIL else ""
        print(f"  [{status}] {name}{tag}")

    print(f"\n  {len(passed)} passed / {len(failed)} failed out of {len(qc_results)} checks")
    if failed:
        print("\n  FAILED:")
        for _, name, val in failed:
            print(f"    - {name}: {val} violations")


def _print_count(cursor, full_table):
    cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
    print(f"    -> {cursor.fetchone()[0]:,} rows\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    conn   = get_connection()
    cursor = conn.cursor()
    try:
        create_silver_schema(cursor)

        transfer_customers(cursor)
        transfer_geolocation(cursor)
        transfer_category_translation(cursor)
        transfer_products(cursor)
        transfer_order_reviews(cursor)
        transfer_orders(cursor)
        transfer_order_items(cursor)
        transfer_order_payments(cursor)

        run_quality_checks(cursor)
        print_row_counts(cursor)
        print_qc_report()

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
