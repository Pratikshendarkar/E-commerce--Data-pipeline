import os
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

PASS = "PASS"
FAIL = "FAIL"
results = []


def get_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def check(cursor, name, query, expect_zero=True):
    cursor.execute(query)
    value = cursor.fetchone()[0]
    status = PASS if (value == 0) == expect_zero else FAIL
    results.append((status, name, value))


# ── Row counts ────────────────────────────────────────────────────────────────

def row_counts(cursor):
    tables = [
        "olist_customers",
        "olist_geolocation",
        "olist_order_items",
        "olist_order_payments",
        "olist_order_reviews",
        "olist_orders",
        "olist_products",
        "product_category_name_translation",
    ]
    print("\n-- Row Counts --------------------------------------------------")
    for t in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {t}")
        count = cursor.fetchone()[0]
        print(f"  {t}: {count:,}")


# ── Null checks ───────────────────────────────────────────────────────────────

def null_checks(cursor):
    checks = [
        ("olist_customers.customer_id",          "SELECT COUNT(*) FROM olist_customers WHERE customer_id IS NULL"),
        ("olist_customers.customer_unique_id",    "SELECT COUNT(*) FROM olist_customers WHERE customer_unique_id IS NULL"),
        ("olist_orders.order_id",                 "SELECT COUNT(*) FROM olist_orders WHERE order_id IS NULL"),
        ("olist_orders.customer_id",              "SELECT COUNT(*) FROM olist_orders WHERE customer_id IS NULL"),
        ("olist_orders.order_purchase_timestamp", "SELECT COUNT(*) FROM olist_orders WHERE order_purchase_timestamp IS NULL"),
        ("olist_order_items.order_id",            "SELECT COUNT(*) FROM olist_order_items WHERE order_id IS NULL"),
        ("olist_order_items.product_id",          "SELECT COUNT(*) FROM olist_order_items WHERE product_id IS NULL"),
        ("olist_order_payments.order_id",         "SELECT COUNT(*) FROM olist_order_payments WHERE order_id IS NULL"),
        ("olist_order_reviews.review_id",         "SELECT COUNT(*) FROM olist_order_reviews WHERE review_id IS NULL"),
        ("olist_order_reviews.order_id",          "SELECT COUNT(*) FROM olist_order_reviews WHERE order_id IS NULL"),
        ("olist_products.product_id",             "SELECT COUNT(*) FROM olist_products WHERE product_id IS NULL"),
        ("olist_geolocation.geolocation_zip_code_prefix", "SELECT COUNT(*) FROM olist_geolocation WHERE geolocation_zip_code_prefix IS NULL"),
    ]
    for name, query in checks:
        check(cursor, f"No nulls in {name}", query)


# ── Duplicate checks ──────────────────────────────────────────────────────────

def duplicate_checks(cursor):
    checks = [
        ("olist_customers PK (customer_id)",
         "SELECT COUNT(*) FROM (SELECT customer_id FROM olist_customers GROUP BY customer_id HAVING COUNT(*) > 1)"),
        ("olist_orders PK (order_id)",
         "SELECT COUNT(*) FROM (SELECT order_id FROM olist_orders GROUP BY order_id HAVING COUNT(*) > 1)"),
        ("olist_order_items PK (order_id, order_item_id)",
         "SELECT COUNT(*) FROM (SELECT order_id, order_item_id FROM olist_order_items GROUP BY order_id, order_item_id HAVING COUNT(*) > 1)"),
        ("olist_order_reviews PK (review_id)",
         "SELECT COUNT(*) FROM (SELECT review_id FROM olist_order_reviews GROUP BY review_id HAVING COUNT(*) > 1)"),
        ("olist_products PK (product_id)",
         "SELECT COUNT(*) FROM (SELECT product_id FROM olist_products GROUP BY product_id HAVING COUNT(*) > 1)"),
        ("product_category_name_translation PK (product_category_name)",
         "SELECT COUNT(*) FROM (SELECT product_category_name FROM product_category_name_translation GROUP BY product_category_name HAVING COUNT(*) > 1)"),
    ]
    for name, query in checks:
        check(cursor, f"No duplicates in {name}", query)


# ── Referential integrity ─────────────────────────────────────────────────────

def referential_integrity(cursor):
    checks = [
        ("olist_orders.customer_id -> olist_customers",
         "SELECT COUNT(*) FROM olist_orders o LEFT JOIN olist_customers c ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL"),
        ("olist_order_items.order_id -> olist_orders",
         "SELECT COUNT(*) FROM olist_order_items oi LEFT JOIN olist_orders o ON oi.order_id = o.order_id WHERE o.order_id IS NULL"),
        ("olist_order_payments.order_id -> olist_orders",
         "SELECT COUNT(*) FROM olist_order_payments op LEFT JOIN olist_orders o ON op.order_id = o.order_id WHERE o.order_id IS NULL"),
        ("olist_order_reviews.order_id -> olist_orders",
         "SELECT COUNT(*) FROM olist_order_reviews r LEFT JOIN olist_orders o ON r.order_id = o.order_id WHERE o.order_id IS NULL"),
        ("olist_order_items.product_id -> olist_products",
         "SELECT COUNT(*) FROM olist_order_items oi LEFT JOIN olist_products p ON oi.product_id = p.product_id WHERE p.product_id IS NULL"),
    ]
    for name, query in checks:
        check(cursor, f"RI: {name}", query)


# ── Value range checks ────────────────────────────────────────────────────────

def value_checks(cursor):
    checks = [
        ("review_score between 1 and 5",
         "SELECT COUNT(*) FROM olist_order_reviews WHERE review_score NOT BETWEEN 1 AND 5"),
        ("order_items.price > 0",
         "SELECT COUNT(*) FROM olist_order_items WHERE price <= 0"),
        ("order_items.freight_value >= 0",
         "SELECT COUNT(*) FROM olist_order_items WHERE freight_value < 0"),
        ("order_payments.payment_value > 0",
         "SELECT COUNT(*) FROM olist_order_payments WHERE payment_value <= 0"),
        ("order_payments.payment_installments >= 1",
         "SELECT COUNT(*) FROM olist_order_payments WHERE payment_installments < 1"),
        ("geolocation.lat between -90 and 90",
         "SELECT COUNT(*) FROM olist_geolocation WHERE geolocation_lat NOT BETWEEN -90 AND 90"),
        ("geolocation.lng between -180 and 180",
         "SELECT COUNT(*) FROM olist_geolocation WHERE geolocation_lng NOT BETWEEN -180 AND 180"),
    ]
    for name, query in checks:
        check(cursor, f"Value check: {name}", query)


# ── Date sanity checks ────────────────────────────────────────────────────────

def date_checks(cursor):
    checks = [
        ("order_approved_at >= order_purchase_timestamp",
         "SELECT COUNT(*) FROM olist_orders WHERE order_approved_at < order_purchase_timestamp"),
        ("order_delivered_carrier_date >= order_approved_at",
         "SELECT COUNT(*) FROM olist_orders WHERE order_delivered_carrier_date IS NOT NULL AND order_approved_at IS NOT NULL AND order_delivered_carrier_date < order_approved_at"),
        ("order_delivered_customer_date >= order_delivered_carrier_date",
         "SELECT COUNT(*) FROM olist_orders WHERE order_delivered_customer_date IS NOT NULL AND order_delivered_carrier_date IS NOT NULL AND order_delivered_customer_date < order_delivered_carrier_date"),
        ("review_answer_timestamp >= review_creation_date",
         "SELECT COUNT(*) FROM olist_order_reviews WHERE review_answer_timestamp < review_creation_date"),
    ]
    for name, query in checks:
        check(cursor, f"Date sanity: {name}", query)


# ── Report ────────────────────────────────────────────────────────────────────

def print_report():
    passed = [r for r in results if r[0] == PASS]
    failed = [r for r in results if r[0] == FAIL]

    print("\n-- Quality Check Results ---------------------------------------")
    for status, name, value in results:
        indicator = "PASS" if status == PASS else "FAIL"
        suffix = f"  ({value} violations)" if status == FAIL else ""
        print(f"  [{indicator}] {name}{suffix if status == FAIL else ''}")

    print(f"\n  {len(passed)} passed / {len(failed)} failed out of {len(results)} checks")
    if failed:
        print("\n  FAILED CHECKS:")
        for _, name, value in failed:
            print(f"    - {name}: {value} violations")


def main():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        row_counts(cursor)
        null_checks(cursor)
        duplicate_checks(cursor)
        referential_integrity(cursor)
        value_checks(cursor)
        date_checks(cursor)
        print_report()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
