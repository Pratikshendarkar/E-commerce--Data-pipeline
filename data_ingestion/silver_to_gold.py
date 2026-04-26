"""
silver_to_gold.py

Reads from SILVER schema, builds the full GOLD layer:
  Dimensions  : dim_date, dim_geolocation, dim_customers, dim_sellers, dim_products
  Facts       : fact_orders, fact_order_items
  Aggregates  : agg_revenue_by_category, agg_revenue_by_state,
                agg_seller_performance, agg_customer_cohorts
  ML Features : ml_customer_features, ml_seller_features,
                ml_delivery_features, ml_review_features
  Master      : master_table (fully denormalized, one row per order-item)

All dim tables carry a surrogate integer PK + natural key.
PKs and FKs are declared in Snowflake (informational, not enforced —
Power BI auto-detects relationships from them).

Usage:
    python data_ingestion/silver_to_gold.py
"""

import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

DB     = os.getenv("SNOWFLAKE_DATABASE")
SILVER = "SILVER"
GOLD   = "GOLD"

SNOWFLAKE_CONFIG = {
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database":  DB,
    "schema":    SILVER,
}


def get_connection():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    warehouse = SNOWFLAKE_CONFIG["warehouse"]
    conn.cursor().execute(f"ALTER WAREHOUSE {warehouse} SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE")
    return conn

def si(t):  return f"{DB}.{SILVER}.{t}_stage"
def g(t):   return f"{DB}.{GOLD}.{t}"

def exe(cursor, sql):
    cursor.execute(sql)

def cnt(cursor, table):
    cursor.execute(f"SELECT COUNT(*) FROM {g(table)}")
    return cursor.fetchone()[0]

def add_pk(cursor, table, col):
    cursor.execute(f"ALTER TABLE {g(table)} ADD PRIMARY KEY ({col})")

def add_fk(cursor, table, col, ref_table, ref_col):
    cursor.execute(
        f"ALTER TABLE {g(table)} ADD FOREIGN KEY ({col}) "
        f"REFERENCES {g(ref_table)}({ref_col})"
    )


# ── Schema ────────────────────────────────────────────────────────────────────

def create_gold_schema(cursor):
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DB}.{GOLD}")
    print(f"Schema {DB}.{GOLD} ready.\n")


# ── Dimensions ────────────────────────────────────────────────────────────────

def build_dim_date(cursor):
    print("[1] dim_date ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('dim_date')} AS
        WITH dates AS (
            SELECT DATEADD('day', SEQ4(), '2016-01-01'::DATE) AS full_date
            FROM TABLE(GENERATOR(ROWCOUNT => 1461))
        )
        SELECT
            TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD'))                         AS date_key,
            full_date,
            YEAR(full_date)                                                    AS year,
            QUARTER(full_date)                                                 AS quarter,
            MONTH(full_date)                                                   AS month,
            MONTHNAME(full_date)                                               AS month_name,
            WEEKOFYEAR(full_date)                                              AS week_of_year,
            DAY(full_date)                                                     AS day_of_month,
            DAYOFWEEKISO(full_date)                                            AS day_of_week,
            DAYNAME(full_date)                                                 AS day_name,
            CASE WHEN DAYOFWEEKISO(full_date) IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend
        FROM dates
    """)
    add_pk(cursor, 'dim_date', 'date_key')
    print(f"    {cnt(cursor, 'dim_date'):,} rows\n")


def build_dim_geolocation(cursor):
    print("[2] dim_geolocation ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('dim_geolocation')} AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY geolocation_zip_code_prefix) AS geo_key,
            geolocation_zip_code_prefix                               AS zip_code_prefix,
            MAX(geolocation_city)                                     AS city,
            MAX(geolocation_state)                                    AS state,
            AVG(geolocation_lat)                                      AS avg_lat,
            AVG(geolocation_lng)                                      AS avg_lng
        FROM {si('olist_geolocation')}
        GROUP BY geolocation_zip_code_prefix
    """)
    add_pk(cursor, 'dim_geolocation', 'geo_key')
    print(f"    {cnt(cursor, 'dim_geolocation'):,} rows\n")


def build_dim_customers(cursor):
    print("[3] dim_customers ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('dim_customers')} AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY c.customer_id) AS customer_key,
            c.customer_id,
            c.customer_unique_id,
            c.customer_zip_code_prefix                 AS zip_code_prefix,
            c.customer_city                            AS city,
            c.customer_state                           AS state,
            g.geo_key
        FROM {si('olist_customers')} c
        LEFT JOIN {g('dim_geolocation')} g
               ON c.customer_zip_code_prefix = g.zip_code_prefix
    """)
    add_pk(cursor, 'dim_customers', 'customer_key')
    add_fk(cursor, 'dim_customers', 'geo_key', 'dim_geolocation', 'geo_key')
    print(f"    {cnt(cursor, 'dim_customers'):,} rows\n")


def build_dim_sellers(cursor):
    print("[4] dim_sellers ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('dim_sellers')} AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY seller_id) AS seller_key,
            seller_id
        FROM (SELECT DISTINCT seller_id FROM {si('olist_order_items')})
    """)
    add_pk(cursor, 'dim_sellers', 'seller_key')
    print(f"    {cnt(cursor, 'dim_sellers'):,} rows\n")


def build_dim_products(cursor):
    print("[5] dim_products ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('dim_products')} AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY p.product_id)                        AS product_key,
            p.product_id,
            p.product_category_name,
            COALESCE(t.product_category_name_english,
                     p.product_category_name)                                AS product_category_name_english,
            p.product_name_lenght                                            AS product_name_length,
            p.product_description_lenght                                     AS product_description_length,
            p.product_photos_qty,
            p.product_weight_g,
            p.product_length_cm,
            p.product_height_cm,
            p.product_width_cm,
            (p.product_length_cm * p.product_height_cm * p.product_width_cm) AS product_volume_cm3
        FROM {si('olist_products')} p
        LEFT JOIN {si('product_category_name_translation')} t
               ON p.product_category_name = t.product_category_name
    """)
    add_pk(cursor, 'dim_products', 'product_key')
    print(f"    {cnt(cursor, 'dim_products'):,} rows\n")


# ── Facts ─────────────────────────────────────────────────────────────────────

def build_fact_orders(cursor):
    print("[6] fact_orders ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('fact_orders')} AS
        WITH payment_agg AS (
            SELECT
                order_id,
                SUM(payment_value)        AS total_payment,
                MAX(payment_installments) AS payment_installments
            FROM {si('olist_order_payments')}
            GROUP BY order_id
        ),
        payment_type_agg AS (
            SELECT order_id, payment_type
            FROM (
                SELECT order_id, payment_type,
                    ROW_NUMBER() OVER (
                        PARTITION BY order_id
                        ORDER BY SUM(payment_value) DESC
                    ) AS rn
                FROM {si('olist_order_payments')}
                GROUP BY order_id, payment_type
            )
            WHERE rn = 1
        ),
        item_agg AS (
            SELECT
                order_id,
                COUNT(*)           AS total_items,
                SUM(price)         AS total_amount,
                SUM(freight_value) AS total_freight
            FROM {si('olist_order_items')}
            GROUP BY order_id
        )
        SELECT
            o.order_id,
            dc.customer_key,
            TO_NUMBER(TO_CHAR(o.order_purchase_timestamp::DATE,      'YYYYMMDD')) AS order_date_key,
            TO_NUMBER(TO_CHAR(o.order_approved_at::DATE,             'YYYYMMDD')) AS approved_date_key,
            TO_NUMBER(TO_CHAR(o.order_delivered_carrier_date::DATE,  'YYYYMMDD')) AS delivered_carrier_date_key,
            TO_NUMBER(TO_CHAR(o.order_delivered_customer_date::DATE, 'YYYYMMDD')) AS delivered_customer_date_key,
            TO_NUMBER(TO_CHAR(o.order_estimated_delivery_date::DATE, 'YYYYMMDD')) AS estimated_delivery_date_key,
            o.order_status,
            COALESCE(ia.total_items,   0)                                          AS total_items,
            COALESCE(ia.total_amount,  0)                                          AS total_amount,
            COALESCE(ia.total_freight, 0)                                          AS total_freight,
            COALESCE(pa.total_payment, 0)                                          AS total_payment,
            pt.payment_type,
            COALESCE(pa.payment_installments, 0)                                   AS payment_installments,
            r.review_score,
            DATEDIFF('day', o.order_purchase_timestamp, o.order_approved_at)       AS approval_days,
            DATEDIFF('day', o.order_purchase_timestamp,
                            o.order_delivered_customer_date)                        AS delivery_days,
            DATEDIFF('day', o.order_purchase_timestamp,
                            o.order_estimated_delivery_date)                        AS estimated_delivery_days,
            DATEDIFF('day', o.order_estimated_delivery_date,
                            o.order_delivered_customer_date)                        AS delivery_delay_days,
            CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
                 THEN TRUE ELSE FALSE END                                           AS is_late
        FROM {si('olist_orders')} o
        LEFT JOIN {g('dim_customers')}           dc ON o.order_id   = o.order_id
                                                   AND o.customer_id = dc.customer_id
        LEFT JOIN item_agg                       ia ON o.order_id   = ia.order_id
        LEFT JOIN payment_agg                    pa ON o.order_id   = pa.order_id
        LEFT JOIN payment_type_agg               pt ON o.order_id   = pt.order_id
        LEFT JOIN {si('olist_order_reviews')}     r ON o.order_id   = r.order_id
    """)
    add_pk(cursor, 'fact_orders', 'order_id')
    add_fk(cursor, 'fact_orders', 'customer_key',   'dim_customers', 'customer_key')
    add_fk(cursor, 'fact_orders', 'order_date_key', 'dim_date',      'date_key')
    print(f"    {cnt(cursor, 'fact_orders'):,} rows\n")


def build_fact_order_items(cursor):
    print("[7] fact_order_items ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('fact_order_items')} AS
        SELECT
            oi.order_id,
            oi.order_item_id,
            dp.product_key,
            ds.seller_key,
            TO_NUMBER(TO_CHAR(o.order_purchase_timestamp::DATE, 'YYYYMMDD')) AS order_date_key,
            TO_NUMBER(TO_CHAR(oi.shipping_limit_date::DATE,     'YYYYMMDD')) AS shipping_limit_date_key,
            oi.price,
            oi.freight_value,
            (oi.price + oi.freight_value)                                    AS total_item_value
        FROM {si('olist_order_items')} oi
        JOIN {si('olist_orders')}  o  ON oi.order_id    = o.order_id
        JOIN {g('dim_products')}  dp  ON oi.product_id  = dp.product_id
        JOIN {g('dim_sellers')}   ds  ON oi.seller_id   = ds.seller_id
    """)
    add_fk(cursor, 'fact_order_items', 'order_id',               'fact_orders',   'order_id')
    add_fk(cursor, 'fact_order_items', 'product_key',            'dim_products',  'product_key')
    add_fk(cursor, 'fact_order_items', 'seller_key',             'dim_sellers',   'seller_key')
    add_fk(cursor, 'fact_order_items', 'order_date_key',         'dim_date',      'date_key')
    print(f"    {cnt(cursor, 'fact_order_items'):,} rows\n")


# ── Aggregates ────────────────────────────────────────────────────────────────

def build_agg_revenue_by_category(cursor):
    print("[8] agg_revenue_by_category ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('agg_revenue_by_category')} AS
        SELECT
            d.year,
            d.month,
            dp.product_category_name_english,
            COUNT(DISTINCT fo.order_id)  AS total_orders,
            SUM(foi.price)               AS total_revenue,
            SUM(foi.freight_value)       AS total_freight,
            AVG(fo.review_score)         AS avg_review_score,
            AVG(fo.delivery_days)        AS avg_delivery_days
        FROM {g('fact_order_items')} foi
        JOIN {g('fact_orders')}      fo  ON foi.order_id    = fo.order_id
        JOIN {g('dim_products')}     dp  ON foi.product_key = dp.product_key
        JOIN {g('dim_date')}         d   ON foi.order_date_key = d.date_key
        GROUP BY d.year, d.month, dp.product_category_name_english
    """)
    print(f"    {cnt(cursor, 'agg_revenue_by_category'):,} rows\n")


def build_agg_revenue_by_state(cursor):
    print("[9] agg_revenue_by_state ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('agg_revenue_by_state')} AS
        SELECT
            d.year,
            d.month,
            dc.state                     AS customer_state,
            COUNT(DISTINCT fo.order_id)  AS total_orders,
            SUM(fo.total_amount)         AS total_revenue,
            AVG(fo.delivery_days)        AS avg_delivery_days,
            AVG(fo.review_score)         AS avg_review_score
        FROM {g('fact_orders')}      fo
        JOIN {g('dim_customers')}    dc  ON fo.customer_key   = dc.customer_key
        JOIN {g('dim_date')}         d   ON fo.order_date_key = d.date_key
        GROUP BY d.year, d.month, dc.state
    """)
    print(f"    {cnt(cursor, 'agg_revenue_by_state'):,} rows\n")


def build_agg_seller_performance(cursor):
    print("[10] agg_seller_performance ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('agg_seller_performance')} AS
        SELECT
            ds.seller_key,
            ds.seller_id,
            COUNT(DISTINCT fo.order_id)                               AS total_orders,
            COUNT(foi.order_item_id)                                  AS total_items,
            SUM(foi.price)                                            AS total_revenue,
            COUNT(DISTINCT foi.product_key)                           AS unique_products,
            COUNT(DISTINCT fo.customer_key)                           AS unique_customers,
            AVG(fo.review_score)                                      AS avg_review_score,
            AVG(CASE WHEN fo.review_score = 5 THEN 1.0 ELSE 0.0 END) AS pct_5_star,
            AVG(CASE WHEN fo.review_score = 1 THEN 1.0 ELSE 0.0 END) AS pct_1_star,
            AVG(fo.delivery_days)                                     AS avg_delivery_days,
            AVG(CASE WHEN fo.is_late = FALSE THEN 1.0 ELSE 0.0 END)  AS on_time_delivery_rate,
            MIN(d.full_date)                                          AS first_sale_date,
            MAX(d.full_date)                                          AS last_sale_date
        FROM {g('fact_order_items')} foi
        JOIN {g('dim_sellers')}      ds  ON foi.seller_key    = ds.seller_key
        JOIN {g('fact_orders')}      fo  ON foi.order_id      = fo.order_id
        JOIN {g('dim_date')}         d   ON foi.order_date_key = d.date_key
        GROUP BY ds.seller_key, ds.seller_id
    """)
    add_fk(cursor, 'agg_seller_performance', 'seller_key', 'dim_sellers', 'seller_key')
    print(f"    {cnt(cursor, 'agg_seller_performance'):,} rows\n")


def build_agg_customer_cohorts(cursor):
    print("[11] agg_customer_cohorts ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('agg_customer_cohorts')} AS
        WITH first_purchase AS (
            SELECT
                dc.customer_unique_id,
                DATE_TRUNC('month', MIN(d.full_date)) AS cohort_month
            FROM {g('fact_orders')}   fo
            JOIN {g('dim_customers')} dc ON fo.customer_key   = dc.customer_key
            JOIN {g('dim_date')}      d  ON fo.order_date_key = d.date_key
            GROUP BY dc.customer_unique_id
        ),
        cohort_sizes AS (
            SELECT cohort_month, COUNT(DISTINCT customer_unique_id) AS cohort_size
            FROM first_purchase
            GROUP BY cohort_month
        ),
        all_orders AS (
            SELECT
                dc.customer_unique_id,
                DATE_TRUNC('month', d.full_date) AS order_month,
                fo.order_id,
                fo.total_amount
            FROM {g('fact_orders')}   fo
            JOIN {g('dim_customers')} dc ON fo.customer_key   = dc.customer_key
            JOIN {g('dim_date')}      d  ON fo.order_date_key = d.date_key
        )
        SELECT
            fp.cohort_month,
            ao.order_month,
            DATEDIFF('month', fp.cohort_month, ao.order_month)  AS months_since_cohort,
            cs.cohort_size                                       AS total_customers,
            COUNT(DISTINCT ao.customer_unique_id)                AS active_customers,
            COUNT(DISTINCT ao.order_id)                          AS total_orders,
            SUM(ao.total_amount)                                 AS total_revenue,
            COUNT(DISTINCT ao.customer_unique_id)
                / NULLIF(cs.cohort_size, 0)                      AS retention_rate
        FROM first_purchase fp
        JOIN all_orders    ao ON fp.customer_unique_id = ao.customer_unique_id
        JOIN cohort_sizes  cs ON fp.cohort_month       = cs.cohort_month
        GROUP BY fp.cohort_month, ao.order_month, cs.cohort_size
        ORDER BY fp.cohort_month, ao.order_month
    """)
    print(f"    {cnt(cursor, 'agg_customer_cohorts'):,} rows\n")


# ── ML Feature Tables ─────────────────────────────────────────────────────────

def build_ml_customer_features(cursor):
    print("[12] ml_customer_features ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('ml_customer_features')} AS
        WITH orders_base AS (
            SELECT
                dc.customer_unique_id,
                dc.state          AS customer_state,
                fo.order_id,
                fo.total_amount,
                fo.total_items,
                fo.review_score,
                fo.delivery_days,
                fo.delivery_delay_days,
                fo.payment_type,
                d.full_date       AS order_date
            FROM {g('fact_orders')}   fo
            JOIN {g('dim_customers')} dc ON fo.customer_key   = dc.customer_key
            JOIN {g('dim_date')}       d ON fo.order_date_key = d.date_key
        ),
        top_category AS (
            SELECT customer_unique_id, product_category_name_english
            FROM (
                SELECT
                    dc.customer_unique_id,
                    dp.product_category_name_english,
                    ROW_NUMBER() OVER (
                        PARTITION BY dc.customer_unique_id
                        ORDER BY COUNT(*) DESC
                    ) AS rn
                FROM {g('fact_order_items')} foi
                JOIN {g('fact_orders')}      fo ON foi.order_id    = fo.order_id
                JOIN {g('dim_customers')}    dc ON fo.customer_key = dc.customer_key
                JOIN {g('dim_products')}     dp ON foi.product_key = dp.product_key
                GROUP BY dc.customer_unique_id, dp.product_category_name_english
            )
            WHERE rn = 1
        ),
        top_payment AS (
            SELECT customer_unique_id, payment_type
            FROM (
                SELECT customer_unique_id, payment_type,
                    ROW_NUMBER() OVER (
                        PARTITION BY customer_unique_id
                        ORDER BY COUNT(*) DESC
                    ) AS rn
                FROM orders_base
                GROUP BY customer_unique_id, payment_type
            )
            WHERE rn = 1
        )
        SELECT
            ob.customer_unique_id,
            COUNT(DISTINCT ob.order_id)                              AS total_orders,
            SUM(ob.total_amount)                                     AS total_spend,
            AVG(ob.total_amount)                                     AS avg_order_value,
            SUM(ob.total_items)                                      AS total_items_bought,
            AVG(ob.review_score)                                     AS avg_review_score,
            AVG(ob.delivery_days)                                    AS avg_delivery_days,
            AVG(ob.delivery_delay_days)                              AS avg_delay_days,
            DATEDIFF('day', MAX(ob.order_date), CURRENT_DATE())      AS recency_days,
            MIN(ob.order_date)                                       AS first_order_date,
            MAX(ob.order_date)                                       AS last_order_date,
            tp.payment_type                                          AS preferred_payment_type,
            tc.product_category_name_english                        AS preferred_category,
            ob.customer_state
        FROM orders_base ob
        LEFT JOIN top_payment tp ON ob.customer_unique_id = tp.customer_unique_id
        LEFT JOIN top_category tc ON ob.customer_unique_id = tc.customer_unique_id
        GROUP BY ob.customer_unique_id, tp.payment_type,
                 tc.product_category_name_english, ob.customer_state
    """)
    add_pk(cursor, 'ml_customer_features', 'customer_unique_id')
    print(f"    {cnt(cursor, 'ml_customer_features'):,} rows\n")


def build_ml_seller_features(cursor):
    print("[13] ml_seller_features ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('ml_seller_features')} AS
        SELECT
            ds.seller_id,
            ds.seller_key,
            COUNT(DISTINCT fo.order_id)                               AS total_orders,
            COUNT(foi.order_item_id)                                  AS total_items,
            SUM(foi.price)                                            AS total_revenue,
            COUNT(DISTINCT foi.product_key)                           AS unique_products,
            COUNT(DISTINCT fo.customer_key)                           AS unique_customers,
            AVG(fo.review_score)                                      AS avg_review_score,
            AVG(CASE WHEN fo.review_score = 5 THEN 1.0 ELSE 0.0 END) AS pct_5_star,
            AVG(CASE WHEN fo.review_score = 1 THEN 1.0 ELSE 0.0 END) AS pct_1_star,
            AVG(fo.delivery_days)                                     AS avg_delivery_days,
            AVG(CASE WHEN fo.is_late = FALSE THEN 1.0 ELSE 0.0 END)  AS on_time_delivery_rate,
            AVG(foi.price)                                            AS avg_price,
            AVG(foi.freight_value / NULLIF(foi.price, 0))            AS avg_freight_ratio
        FROM {g('fact_order_items')} foi
        JOIN {g('dim_sellers')}      ds  ON foi.seller_key    = ds.seller_key
        JOIN {g('fact_orders')}      fo  ON foi.order_id      = fo.order_id
        GROUP BY ds.seller_id, ds.seller_key
    """)
    add_pk(cursor, 'ml_seller_features', 'seller_id')
    add_fk(cursor, 'ml_seller_features', 'seller_key', 'dim_sellers', 'seller_key')
    print(f"    {cnt(cursor, 'ml_seller_features'):,} rows\n")


def build_ml_delivery_features(cursor):
    print("[14] ml_delivery_features ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('ml_delivery_features')} AS
        WITH primary_item AS (
            SELECT order_id, product_key, seller_key
            FROM (
                SELECT order_id, product_key, seller_key,
                    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY price DESC) AS rn
                FROM {g('fact_order_items')}
            )
            WHERE rn = 1
        )
        SELECT
            fo.order_id,
            fo.customer_key,
            pi.seller_key,
            pi.product_key,
            fo.order_date_key,
            dc.state                                                AS customer_state,
            d.month                                                 AS order_month,
            d.day_of_week                                           AS order_day_of_week,
            fo.total_items,
            fo.total_amount,
            COALESCE(fo.total_freight / NULLIF(fo.total_amount,0), 0) AS freight_ratio,
            dp.product_weight_g,
            dp.product_volume_cm3,
            fo.payment_type,
            fo.payment_installments,
            fo.delivery_days                                        AS actual_delivery_days,
            fo.is_late
        FROM {g('fact_orders')}   fo
        JOIN {g('dim_customers')} dc  ON fo.customer_key    = dc.customer_key
        JOIN {g('dim_date')}       d  ON fo.order_date_key  = d.date_key
        LEFT JOIN primary_item    pi  ON fo.order_id        = pi.order_id
        LEFT JOIN {g('dim_products')} dp ON pi.product_key  = dp.product_key
        WHERE fo.delivery_days IS NOT NULL
    """)
    add_pk(cursor, 'ml_delivery_features', 'order_id')
    add_fk(cursor, 'ml_delivery_features', 'customer_key',   'dim_customers', 'customer_key')
    add_fk(cursor, 'ml_delivery_features', 'seller_key',     'dim_sellers',   'seller_key')
    add_fk(cursor, 'ml_delivery_features', 'product_key',    'dim_products',  'product_key')
    add_fk(cursor, 'ml_delivery_features', 'order_date_key', 'dim_date',      'date_key')
    print(f"    {cnt(cursor, 'ml_delivery_features'):,} rows\n")


def build_ml_review_features(cursor):
    print("[15] ml_review_features ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('ml_review_features')} AS
        WITH primary_item AS (
            SELECT order_id, product_key, seller_key
            FROM (
                SELECT order_id, product_key, seller_key,
                    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY price DESC) AS rn
                FROM {g('fact_order_items')}
            )
            WHERE rn = 1
        )
        SELECT
            fo.order_id,
            fo.customer_key,
            pi.product_key,
            pi.seller_key,
            fo.order_date_key,
            fo.delivery_delay_days,
            fo.total_amount,
            COALESCE(fo.total_freight / NULLIF(fo.total_amount,0), 0) AS freight_ratio,
            fo.total_items,
            fo.payment_installments,
            dp.product_category_name_english,
            dc.state                                                   AS customer_state,
            fo.review_score
        FROM {g('fact_orders')}   fo
        JOIN {g('dim_customers')} dc  ON fo.customer_key   = dc.customer_key
        LEFT JOIN primary_item    pi  ON fo.order_id       = pi.order_id
        LEFT JOIN {g('dim_products')} dp ON pi.product_key = dp.product_key
        WHERE fo.review_score IS NOT NULL
    """)
    add_pk(cursor, 'ml_review_features', 'order_id')
    add_fk(cursor, 'ml_review_features', 'customer_key',   'dim_customers', 'customer_key')
    add_fk(cursor, 'ml_review_features', 'product_key',    'dim_products',  'product_key')
    add_fk(cursor, 'ml_review_features', 'seller_key',     'dim_sellers',   'seller_key')
    add_fk(cursor, 'ml_review_features', 'order_date_key', 'dim_date',      'date_key')
    print(f"    {cnt(cursor, 'ml_review_features'):,} rows\n")


# ── Master Table ──────────────────────────────────────────────────────────────

def build_master_table(cursor):
    print("[16] master_table (fully denormalized, one row per order-item) ...")
    exe(cursor, f"""
        CREATE OR REPLACE TABLE {g('master_table')} AS
        WITH payment_agg AS (
            SELECT
                order_id,
                SUM(payment_value)        AS total_payment,
                MAX(payment_installments) AS payment_installments
            FROM {si('olist_order_payments')}
            GROUP BY order_id
        ),
        payment_type_agg AS (
            SELECT order_id, payment_type
            FROM (
                SELECT order_id, payment_type,
                    ROW_NUMBER() OVER (
                        PARTITION BY order_id
                        ORDER BY SUM(payment_value) DESC
                    ) AS rn
                FROM {si('olist_order_payments')}
                GROUP BY order_id, payment_type
            )
            WHERE rn = 1
        )
        SELECT
            -- Keys (surrogate + natural)
            dc.customer_key,
            ds.seller_key,
            dp.product_key,
            TO_NUMBER(TO_CHAR(o.order_purchase_timestamp::DATE, 'YYYYMMDD')) AS order_date_key,

            -- Order
            o.order_id,
            o.order_status,
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,

            -- Order item
            oi.order_item_id,
            oi.seller_id,
            oi.shipping_limit_date,
            oi.price,
            oi.freight_value,
            (oi.price + oi.freight_value)                                    AS total_item_value,

            -- Product
            p.product_id,
            p.product_category_name,
            COALESCE(pcat.product_category_name_english,
                     p.product_category_name)                                AS product_category_name_english,
            p.product_weight_g,
            p.product_length_cm,
            p.product_height_cm,
            p.product_width_cm,
            (p.product_length_cm * p.product_height_cm * p.product_width_cm) AS product_volume_cm3,

            -- Customer
            c.customer_id,
            c.customer_unique_id,
            c.customer_city,
            c.customer_state,
            c.customer_zip_code_prefix,

            -- Customer geolocation
            cg.avg_lat AS customer_lat,
            cg.avg_lng AS customer_lng,

            -- Payment
            pt.payment_type,
            pa.payment_installments,
            pa.total_payment,

            -- Review
            r.review_id,
            r.review_score,
            r.review_comment_title,
            r.review_comment_message,
            r.review_creation_date,

            -- Derived metrics
            DATEDIFF('day', o.order_purchase_timestamp,
                            o.order_approved_at)                             AS approval_days,
            DATEDIFF('day', o.order_purchase_timestamp,
                            o.order_delivered_customer_date)                 AS delivery_days,
            DATEDIFF('day', o.order_estimated_delivery_date,
                            o.order_delivered_customer_date)                 AS delivery_delay_days,
            CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
                 THEN TRUE ELSE FALSE END                                    AS is_late

        FROM {si('olist_orders')}                       o
        LEFT JOIN {si('olist_order_items')}            oi   ON o.order_id              = oi.order_id
        LEFT JOIN {si('olist_products')}                p   ON oi.product_id           = p.product_id
        LEFT JOIN {si('product_category_name_translation')} pcat
                                                            ON p.product_category_name = pcat.product_category_name
        LEFT JOIN {si('olist_customers')}               c   ON o.customer_id           = c.customer_id
        LEFT JOIN {g('dim_customers')}                 dc   ON c.customer_id           = dc.customer_id
        LEFT JOIN {g('dim_geolocation')}               cg   ON c.customer_zip_code_prefix = cg.zip_code_prefix
        LEFT JOIN {g('dim_sellers')}                   ds   ON oi.seller_id            = ds.seller_id
        LEFT JOIN {g('dim_products')}                  dp   ON oi.product_id           = dp.product_id
        LEFT JOIN payment_agg                          pa   ON o.order_id              = pa.order_id
        LEFT JOIN payment_type_agg                     pt   ON o.order_id              = pt.order_id
        LEFT JOIN {si('olist_order_reviews')}           r   ON o.order_id              = r.order_id
    """)
    add_fk(cursor, 'master_table', 'customer_key',   'dim_customers', 'customer_key')
    add_fk(cursor, 'master_table', 'seller_key',     'dim_sellers',   'seller_key')
    add_fk(cursor, 'master_table', 'product_key',    'dim_products',  'product_key')
    add_fk(cursor, 'master_table', 'order_date_key', 'dim_date',      'date_key')
    print(f"    {cnt(cursor, 'master_table'):,} rows\n")


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(cursor):
    tables = [
        ("DIMENSIONS",   ['dim_date','dim_geolocation','dim_customers','dim_sellers','dim_products']),
        ("FACTS",        ['fact_orders','fact_order_items']),
        ("AGGREGATES",   ['agg_revenue_by_category','agg_revenue_by_state',
                          'agg_seller_performance','agg_customer_cohorts']),
        ("ML FEATURES",  ['ml_customer_features','ml_seller_features',
                          'ml_delivery_features','ml_review_features']),
        ("MASTER",       ['master_table']),
    ]
    print("\n-- GOLD Layer Summary ------------------------------------------")
    for section, tbls in tables:
        print(f"\n  {section}")
        for t in tbls:
            cursor.execute(f"SELECT COUNT(*) FROM {g(t)}")
            print(f"    {t:<45} {cursor.fetchone()[0]:>10,} rows")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    conn   = get_connection()
    cursor = conn.cursor()
    try:
        create_gold_schema(cursor)

        build_dim_date(cursor)
        build_dim_geolocation(cursor)
        build_dim_customers(cursor)
        build_dim_sellers(cursor)
        build_dim_products(cursor)

        build_fact_orders(cursor)
        build_fact_order_items(cursor)

        build_agg_revenue_by_category(cursor)
        build_agg_revenue_by_state(cursor)
        build_agg_seller_performance(cursor)
        build_agg_customer_cohorts(cursor)

        build_ml_customer_features(cursor)
        build_ml_seller_features(cursor)
        build_ml_delivery_features(cursor)
        build_ml_review_features(cursor)

        build_master_table(cursor)
        print_summary(cursor)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
