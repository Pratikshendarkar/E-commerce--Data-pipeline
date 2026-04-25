"""
data_cleaning.py

Handles all data inconsistencies and outliers found by quality_checks.py.
Creates a cleaned version of each affected table (suffix: _clean).
Sends an HTML email report via email_alert.py when done.

Issues addressed:
  1. Duplicate review_id in olist_order_reviews       (789 duplicate keys)
  2. payment_value <= 0 in olist_order_payments        (9 rows)
  3. payment_installments < 1 in olist_order_payments  (2 rows)
  4. carrier_date < approved_at in olist_orders         (1,359 rows)
  5. customer_date < carrier_date in olist_orders       (23 rows)
  6. Price / freight outliers in olist_order_items      (IQR-based)
  7. Payment value outliers in olist_order_payments     (IQR-based)
  8. Product dimension / weight outliers                (IQR-based)
"""

import os
from datetime import datetime
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

report = {
    "run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "issues": [],
    "outliers": [],
    "clean_tables": [],
}


def get_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def add_issue(table, description, count, action, severity="high"):
    report["issues"].append({
        "table": table,
        "description": description,
        "count": count,
        "action": action,
        "severity": severity,
    })


def add_outlier(table, column, count, bounds):
    report["outliers"].append({
        "table": table,
        "column": column,
        "count": count,
        "bounds": bounds,
    })


# ── 1. Deduplicate olist_order_reviews ────────────────────────────────────────

def fix_duplicate_reviews(cursor):
    print("\n[1] Deduplicating olist_order_reviews ...")

    cursor.execute("SELECT COUNT(*) FROM olist_order_reviews")
    before = cursor.fetchone()[0]

    cursor.execute("""
        CREATE OR REPLACE TABLE olist_order_reviews_clean AS
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
            FROM olist_order_reviews
        )
        WHERE rn = 1
    """)

    cursor.execute("SELECT COUNT(*) FROM olist_order_reviews_clean")
    after = cursor.fetchone()[0]
    removed = before - after
    print(f"  {before:,} -> {after:,} rows  (removed {removed:,} duplicates)")

    add_issue(
        table="olist_order_reviews",
        description=f"Duplicate review_id keys ({before - after} duplicate rows)",
        count=removed,
        action=f"Removed {removed:,} duplicates; kept row with most complete comment or latest timestamp",
        severity="high",
    )
    report["clean_tables"].append("olist_order_reviews_clean")


# ── 2 & 3. Fix payment anomalies ─────────────────────────────────────────────

def fix_payment_anomalies(cursor):
    print("\n[2] Fixing payment anomalies in olist_order_payments ...")

    cursor.execute("SELECT COUNT(*) FROM olist_order_payments WHERE payment_value <= 0")
    zero_payments = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM olist_order_payments WHERE payment_installments < 1")
    bad_installments = cursor.fetchone()[0]

    cursor.execute("""
        CREATE OR REPLACE TABLE olist_order_payments_clean AS
        SELECT
            order_id,
            payment_sequential,
            payment_type,
            GREATEST(payment_installments, 1) AS payment_installments,
            payment_value,
            (payment_value <= 0)              AS is_zero_payment
        FROM olist_order_payments
    """)

    print(f"  {zero_payments} zero-value payments flagged, {bad_installments} installment(s) corrected to 1")

    if zero_payments:
        add_issue(
            table="olist_order_payments",
            description="payment_value <= 0 (likely voucher/free orders)",
            count=zero_payments,
            action="Kept rows; flagged with is_zero_payment = TRUE",
            severity="medium",
        )
    if bad_installments:
        add_issue(
            table="olist_order_payments",
            description="payment_installments < 1 (invalid value)",
            count=bad_installments,
            action="Corrected to 1 using GREATEST(payment_installments, 1)",
            severity="medium",
        )


# ── 4 & 5. Fix date inconsistencies in olist_orders ──────────────────────────

def fix_date_inconsistencies(cursor):
    print("\n[3] Fixing date inconsistencies in olist_orders ...")

    cursor.execute("""
        SELECT COUNT(*) FROM olist_orders
        WHERE order_delivered_carrier_date IS NOT NULL
          AND order_approved_at IS NOT NULL
          AND order_delivered_carrier_date < order_approved_at
    """)
    bad_carrier = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*) FROM olist_orders
        WHERE order_delivered_customer_date IS NOT NULL
          AND order_delivered_carrier_date IS NOT NULL
          AND order_delivered_customer_date < order_delivered_carrier_date
    """)
    bad_customer = cursor.fetchone()[0]

    cursor.execute("""
        CREATE OR REPLACE TABLE olist_orders_clean AS
        SELECT
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            CASE
                WHEN order_delivered_carrier_date IS NOT NULL
                 AND order_approved_at IS NOT NULL
                 AND order_delivered_carrier_date < order_approved_at
                THEN NULL
                ELSE order_delivered_carrier_date
            END AS order_delivered_carrier_date,
            CASE
                WHEN order_delivered_customer_date IS NOT NULL
                 AND order_delivered_carrier_date IS NOT NULL
                 AND order_delivered_customer_date < order_delivered_carrier_date
                THEN NULL
                ELSE order_delivered_customer_date
            END AS order_delivered_customer_date,
            order_estimated_delivery_date,
            (
                (order_delivered_carrier_date IS NOT NULL AND order_approved_at IS NOT NULL
                 AND order_delivered_carrier_date < order_approved_at)
                OR
                (order_delivered_customer_date IS NOT NULL AND order_delivered_carrier_date IS NOT NULL
                 AND order_delivered_customer_date < order_delivered_carrier_date)
            ) AS has_date_inconsistency
        FROM olist_orders
    """)

    print(f"  carrier_date nullified on {bad_carrier:,} rows, customer_date on {bad_customer:,} rows")

    add_issue(
        table="olist_orders",
        description=f"carrier_date < approved_at ({bad_carrier:,} rows)",
        count=bad_carrier,
        action="Nullified invalid carrier_date; flagged with has_date_inconsistency",
        severity="high",
    )
    add_issue(
        table="olist_orders",
        description=f"customer_date < carrier_date ({bad_customer:,} rows)",
        count=bad_customer,
        action="Nullified invalid customer delivery date; flagged with has_date_inconsistency",
        severity="medium",
    )
    report["clean_tables"].append("olist_orders_clean")


# ── 6. Outlier handling for olist_order_items ─────────────────────────────────

def fix_outliers_order_items(cursor):
    print("\n[4] Detecting and capping outliers in olist_order_items ...")

    for col in ("price", "freight_value"):
        cursor.execute(f"""
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col}) AS q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col}) AS q3
            FROM olist_order_items
        """)
        q1, q3 = cursor.fetchone()
        iqr = q3 - q1
        lower = float(q1 - 1.5 * iqr)
        upper = float(q3 + 1.5 * iqr)

        cursor.execute(f"SELECT COUNT(*) FROM olist_order_items WHERE {col} < {lower} OR {col} > {upper}")
        count = cursor.fetchone()[0]
        print(f"  {col}: {count:,} outliers  (bounds [{lower:.2f}, {upper:.2f}])")
        add_outlier("olist_order_items", col, count, f"[{lower:.2f}, {upper:.2f}]")

    cursor.execute("""
        CREATE OR REPLACE TABLE olist_order_items_clean AS
        WITH bounds AS (
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price)         AS price_q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price)         AS price_q3,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY freight_value) AS freight_q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY freight_value) AS freight_q3
            FROM olist_order_items
        )
        SELECT
            oi.order_id,
            oi.order_item_id,
            oi.product_id,
            oi.seller_id,
            oi.shipping_limit_date,
            LEAST(GREATEST(oi.price,
                b.price_q1 - 1.5 * (b.price_q3 - b.price_q1)),
                b.price_q3 + 1.5 * (b.price_q3 - b.price_q1))          AS price,
            LEAST(GREATEST(oi.freight_value,
                b.freight_q1 - 1.5 * (b.freight_q3 - b.freight_q1)),
                b.freight_q3 + 1.5 * (b.freight_q3 - b.freight_q1))    AS freight_value,
            (oi.price < b.price_q1 - 1.5 * (b.price_q3 - b.price_q1)
             OR oi.price > b.price_q3 + 1.5 * (b.price_q3 - b.price_q1))   AS price_is_outlier,
            (oi.freight_value < b.freight_q1 - 1.5 * (b.freight_q3 - b.freight_q1)
             OR oi.freight_value > b.freight_q3 + 1.5 * (b.freight_q3 - b.freight_q1)) AS freight_is_outlier
        FROM olist_order_items oi
        CROSS JOIN bounds b
    """)
    report["clean_tables"].append("olist_order_items_clean")


# ── 7. Outlier handling for olist_order_payments ──────────────────────────────

def fix_outliers_order_payments(cursor):
    print("\n[5] Detecting and capping outliers in olist_order_payments ...")

    cursor.execute("""
        SELECT
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY payment_value) AS q1,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY payment_value) AS q3
        FROM olist_order_payments
        WHERE payment_value > 0
    """)
    q1, q3 = cursor.fetchone()
    iqr = q3 - q1
    upper = float(q3 + 1.5 * iqr)

    cursor.execute(f"SELECT COUNT(*) FROM olist_order_payments WHERE payment_value > {upper}")
    count = cursor.fetchone()[0]
    print(f"  payment_value: {count:,} outliers above upper bound {upper:.2f}")
    add_outlier("olist_order_payments", "payment_value", count, f"[0, {upper:.2f}]")

    cursor.execute(f"""
        CREATE OR REPLACE TABLE olist_order_payments_clean AS
        WITH bounds AS (
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY payment_value) AS q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY payment_value) AS q3
            FROM olist_order_payments
            WHERE payment_value > 0
        )
        SELECT
            op.order_id,
            op.payment_sequential,
            op.payment_type,
            GREATEST(op.payment_installments, 1)          AS payment_installments,
            LEAST(GREATEST(op.payment_value, 0),
                b.q3 + 1.5 * (b.q3 - b.q1))              AS payment_value,
            (op.payment_value <= 0)                       AS is_zero_payment,
            (op.payment_value > b.q3 + 1.5 * (b.q3 - b.q1)) AS payment_is_outlier
        FROM olist_order_payments op
        CROSS JOIN bounds b
    """)
    report["clean_tables"].append("olist_order_payments_clean")


# ── 8. Outlier handling for olist_products ────────────────────────────────────

def fix_outliers_products(cursor):
    print("\n[6] Detecting and capping outliers in olist_products ...")

    outlier_cols = [
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]

    bounds = {}
    for col in outlier_cols:
        cursor.execute(f"""
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col}) AS q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col}) AS q3
            FROM olist_products
            WHERE {col} IS NOT NULL
        """)
        q1, q3 = (float(v) for v in cursor.fetchone())
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        bounds[col] = (q1, q3, iqr)

        cursor.execute(f"""
            SELECT COUNT(*) FROM olist_products
            WHERE {col} IS NOT NULL
              AND ({col} < {lower} OR {col} > {upper})
        """)
        count = cursor.fetchone()[0]
        print(f"  {col}: {count:,} outliers  (bounds [{lower:.1f}, {upper:.1f}])")
        add_outlier("olist_products", col, count, f"[{lower:.1f}, {upper:.1f}]")

    def cap_expr(col):
        q1, q3, iqr = bounds[col]
        lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        return (
            f"LEAST(GREATEST({col}, {lo}), {hi}) AS {col}",
            f"({col} IS NOT NULL AND ({col} < {lo} OR {col} > {hi})) AS {col}_is_outlier",
        )

    col_exprs, flag_exprs = [], []
    for col in outlier_cols:
        c, f = cap_expr(col)
        col_exprs.append(c)
        flag_exprs.append(f)

    cursor.execute(f"""
        CREATE OR REPLACE TABLE olist_products_clean AS
        SELECT
            product_id,
            product_category_name,
            product_name_lenght,
            product_description_lenght,
            product_photos_qty,
            {', '.join(col_exprs)},
            {', '.join(flag_exprs)}
        FROM olist_products
    """)
    report["clean_tables"].append("olist_products_clean")


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary():
    print("\n-- Cleaning Summary --------------------------------------------")
    total_issues   = sum(i["count"] for i in report["issues"])
    total_outliers = sum(o["count"] for o in report["outliers"])
    print(f"  Inconsistencies fixed : {total_issues:,}")
    print(f"  Outliers capped       : {total_outliers:,}")
    print(f"  Clean tables created  : {len(report['clean_tables'])}")
    for t in report["clean_tables"]:
        print(f"    - {t}")


def main():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        fix_duplicate_reviews(cursor)
        fix_payment_anomalies(cursor)
        fix_date_inconsistencies(cursor)
        fix_outliers_order_items(cursor)
        fix_outliers_order_payments(cursor)
        fix_outliers_products(cursor)
        print_summary()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
