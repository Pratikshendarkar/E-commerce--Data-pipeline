"""
send_silver_report.py

Standalone script — run after bronze_to_silver.py.
Queries BRONZE and SILVER schemas directly, builds an HTML summary report
(row counts, rows removed, QC results), and emails it.

Usage:
    python data_ingestion/send_silver_report.py
"""

import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

DB     = os.getenv("SNOWFLAKE_DATABASE")
BRONZE = "BRONZE"
SILVER = "SILVER"

SNOWFLAKE_CONFIG = {
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database":  DB,
    "schema":    BRONZE,
}

SMTP_HOST      = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT      = int(os.getenv("SMTP_PORT", 587))
EMAIL_SENDER   = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_TO       = "pratik.shendarkar@rutgers.edu"

TABLES = [
    ("olist_customers",                  "olist_customers_stage",                  "No issues found"),
    ("olist_geolocation",                "olist_geolocation_stage",                "No issues found"),
    ("olist_orders",                     "olist_orders_stage",                     "Removed date-sequence violations"),
    ("olist_order_reviews",              "olist_order_reviews_stage",              "Removed duplicates + cascaded from orders"),
    ("olist_order_items",                "olist_order_items_stage",                "Removed price/freight outliers + cascaded"),
    ("olist_order_payments",             "olist_order_payments_stage",             "Removed invalid/outlier payments + cascaded"),
    ("olist_products",                   "olist_products_stage",                   "Removed dimension/weight outliers"),
    ("product_category_name_translation","product_category_name_translation_stage","No issues found"),
]


def b(t): return f"{DB}.{BRONZE}.{t}"
def s(t): return f"{DB}.{SILVER}.{t}"


# ── Snowflake data fetch ──────────────────────────────────────────────────────

def fetch_row_counts(cursor):
    rows = []
    for bronze_t, silver_t, reason in TABLES:
        cursor.execute(f"SELECT COUNT(*) FROM {b(bronze_t)}")
        bc = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM {s(silver_t)}")
        sc = cursor.fetchone()[0]
        rows.append({
            "bronze_table":  bronze_t,
            "silver_table":  silver_t,
            "bronze_count":  bc,
            "silver_count":  sc,
            "removed":       bc - sc,
            "reason":        reason,
        })
    return rows


def run_qc_checks(cursor):
    results = []

    def qc(name, sql):
        cursor.execute(sql)
        val = cursor.fetchone()[0]
        results.append({"status": "PASS" if val == 0 else "FAIL", "check": name, "violations": val})

    # Nulls
    qc("customers: no null customer_id",      f"SELECT COUNT(*) FROM {s('olist_customers_stage')} WHERE customer_id IS NULL")
    qc("orders: no null order_id",            f"SELECT COUNT(*) FROM {s('olist_orders_stage')} WHERE order_id IS NULL")
    qc("orders: no null customer_id",         f"SELECT COUNT(*) FROM {s('olist_orders_stage')} WHERE customer_id IS NULL")
    qc("order_items: no null order_id",       f"SELECT COUNT(*) FROM {s('olist_order_items_stage')} WHERE order_id IS NULL")
    qc("order_items: no null product_id",     f"SELECT COUNT(*) FROM {s('olist_order_items_stage')} WHERE product_id IS NULL")
    qc("order_payments: no null order_id",    f"SELECT COUNT(*) FROM {s('olist_order_payments_stage')} WHERE order_id IS NULL")
    qc("order_reviews: no null review_id",    f"SELECT COUNT(*) FROM {s('olist_order_reviews_stage')} WHERE review_id IS NULL")
    qc("products: no null product_id",        f"SELECT COUNT(*) FROM {s('olist_products_stage')} WHERE product_id IS NULL")

    # Duplicates
    qc("customers: no duplicate customer_id",
       f"SELECT COUNT(*) FROM (SELECT customer_id FROM {s('olist_customers_stage')} GROUP BY 1 HAVING COUNT(*)>1)")
    qc("orders: no duplicate order_id",
       f"SELECT COUNT(*) FROM (SELECT order_id FROM {s('olist_orders_stage')} GROUP BY 1 HAVING COUNT(*)>1)")
    qc("order_reviews: no duplicate review_id",
       f"SELECT COUNT(*) FROM (SELECT review_id FROM {s('olist_order_reviews_stage')} GROUP BY 1 HAVING COUNT(*)>1)")
    qc("products: no duplicate product_id",
       f"SELECT COUNT(*) FROM (SELECT product_id FROM {s('olist_products_stage')} GROUP BY 1 HAVING COUNT(*)>1)")
    qc("order_items: no duplicate (order_id, order_item_id)",
       f"SELECT COUNT(*) FROM (SELECT order_id, order_item_id FROM {s('olist_order_items_stage')} GROUP BY 1,2 HAVING COUNT(*)>1)")

    # Referential integrity
    qc("RI: orders.customer_id -> customers",
       f"SELECT COUNT(*) FROM {s('olist_orders_stage')} o LEFT JOIN {s('olist_customers_stage')} c ON o.customer_id=c.customer_id WHERE c.customer_id IS NULL")
    qc("RI: order_items.order_id -> orders",
       f"SELECT COUNT(*) FROM {s('olist_order_items_stage')} oi LEFT JOIN {s('olist_orders_stage')} o ON oi.order_id=o.order_id WHERE o.order_id IS NULL")
    qc("RI: order_payments.order_id -> orders",
       f"SELECT COUNT(*) FROM {s('olist_order_payments_stage')} op LEFT JOIN {s('olist_orders_stage')} o ON op.order_id=o.order_id WHERE o.order_id IS NULL")
    qc("RI: order_reviews.order_id -> orders",
       f"SELECT COUNT(*) FROM {s('olist_order_reviews_stage')} r LEFT JOIN {s('olist_orders_stage')} o ON r.order_id=o.order_id WHERE o.order_id IS NULL")
    qc("RI: order_items.product_id -> products",
       f"SELECT COUNT(*) FROM {s('olist_order_items_stage')} oi LEFT JOIN {s('olist_products_stage')} p ON oi.product_id=p.product_id WHERE p.product_id IS NULL")

    # Value checks
    qc("order_reviews: review_score 1-5",      f"SELECT COUNT(*) FROM {s('olist_order_reviews_stage')} WHERE review_score NOT BETWEEN 1 AND 5")
    qc("order_items: price > 0",               f"SELECT COUNT(*) FROM {s('olist_order_items_stage')} WHERE price <= 0")
    qc("order_items: freight_value >= 0",      f"SELECT COUNT(*) FROM {s('olist_order_items_stage')} WHERE freight_value < 0")
    qc("order_payments: payment_value > 0",    f"SELECT COUNT(*) FROM {s('olist_order_payments_stage')} WHERE payment_value <= 0")
    qc("order_payments: installments >= 1",    f"SELECT COUNT(*) FROM {s('olist_order_payments_stage')} WHERE payment_installments < 1")

    # Date sanity
    qc("orders: carrier_date >= approved_at",
       f"SELECT COUNT(*) FROM {s('olist_orders_stage')} WHERE order_delivered_carrier_date IS NOT NULL AND order_approved_at IS NOT NULL AND order_delivered_carrier_date < order_approved_at")
    qc("orders: customer_date >= carrier_date",
       f"SELECT COUNT(*) FROM {s('olist_orders_stage')} WHERE order_delivered_customer_date IS NOT NULL AND order_delivered_carrier_date IS NOT NULL AND order_delivered_customer_date < order_delivered_carrier_date")

    return results


# ── HTML builder ──────────────────────────────────────────────────────────────

def build_html(row_counts, qc_results, run_time):
    total_bronze  = sum(r["bronze_count"] for r in row_counts)
    total_silver  = sum(r["silver_count"] for r in row_counts)
    total_removed = sum(r["removed"] for r in row_counts)
    passed = sum(1 for r in qc_results if r["status"] == "PASS")
    failed = sum(1 for r in qc_results if r["status"] == "FAIL")

    # Row count table rows
    count_rows = ""
    for r in row_counts:
        removed_color = "#e53935" if r["removed"] > 0 else "#2e7d32"
        removed_text  = f'-{r["removed"]:,}' if r["removed"] > 0 else "0"
        count_rows += f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;font-family:monospace;font-size:12px;">{r['bronze_table']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;font-family:monospace;font-size:12px;">{r['silver_table']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:right;">{r['bronze_count']:,}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:right;"><strong>{r['silver_count']:,}</strong></td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:right;font-weight:700;color:{removed_color};">{removed_text}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;font-size:12px;color:#555;">{r['reason']}</td>
        </tr>"""

    # Totals row
    count_rows += f"""
        <tr style="background:#e3f2fd;font-weight:700;">
          <td style="padding:8px 12px;" colspan="2">TOTAL</td>
          <td style="padding:8px 12px;text-align:right;">{total_bronze:,}</td>
          <td style="padding:8px 12px;text-align:right;">{total_silver:,}</td>
          <td style="padding:8px 12px;text-align:right;color:#e53935;">-{total_removed:,}</td>
          <td style="padding:8px 12px;"></td>
        </tr>"""

    # QC rows
    qc_rows = ""
    for r in qc_results:
        color  = "#2e7d32" if r["status"] == "PASS" else "#e53935"
        badge  = f'<span style="background:{color};color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;">{r["status"]}</span>'
        detail = f'<span style="color:#e53935;font-size:12px;">  {r["violations"]} violations</span>' if r["status"] == "FAIL" else ""
        qc_rows += f"""
        <tr>
          <td style="padding:6px 12px;border-bottom:1px solid #eee;">{badge}</td>
          <td style="padding:6px 12px;border-bottom:1px solid #eee;font-size:13px;">{r['check']}{detail}</td>
        </tr>"""

    qc_summary_color = "#2e7d32" if failed == 0 else "#e53935"
    qc_summary_text  = "ALL CHECKS PASSED" if failed == 0 else f"{failed} CHECK(S) FAILED"

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f4f6f9;font-family:Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0">
  <tr><td align="center" style="padding:30px 20px;">
    <table width="780" cellpadding="0" cellspacing="0"
           style="background:#fff;border-radius:8px;overflow:hidden;
                  box-shadow:0 2px 8px rgba(0,0,0,.1);">

      <!-- Header -->
      <tr>
        <td style="background:#1b5e20;padding:28px 32px;">
          <h1 style="margin:0;color:#fff;font-size:22px;">Bronze to Silver Pipeline Report</h1>
          <p style="margin:6px 0 0;color:#a5d6a7;font-size:13px;">
            Olist E-Commerce &nbsp;|&nbsp; BRONZE -> SILVER &nbsp;|&nbsp; {run_time}
          </p>
        </td>
      </tr>

      <!-- KPI strip -->
      <tr>
        <td style="padding:20px 32px;background:#e8f5e9;">
          <table width="100%" cellpadding="0" cellspacing="0"><tr>
            <td align="center" style="padding:10px;">
              <div style="font-size:30px;font-weight:700;color:#1b5e20;">{total_silver:,}</div>
              <div style="font-size:12px;color:#555;margin-top:4px;">Total Silver Rows</div>
            </td>
            <td align="center" style="padding:10px;border-left:1px solid #a5d6a7;">
              <div style="font-size:30px;font-weight:700;color:#e53935;">{total_removed:,}</div>
              <div style="font-size:12px;color:#555;margin-top:4px;">Total Rows Removed</div>
            </td>
            <td align="center" style="padding:10px;border-left:1px solid #a5d6a7;">
              <div style="font-size:30px;font-weight:700;color:{qc_summary_color};">{passed}/{passed+failed}</div>
              <div style="font-size:12px;color:#555;margin-top:4px;">QC Checks Passed</div>
            </td>
            <td align="center" style="padding:10px;border-left:1px solid #a5d6a7;">
              <div style="font-size:14px;font-weight:700;color:{qc_summary_color};">{qc_summary_text}</div>
              <div style="font-size:12px;color:#555;margin-top:4px;">Overall Status</div>
            </td>
          </tr></table>
        </td>
      </tr>

      <!-- Body -->
      <tr><td style="padding:28px 32px;">

        <!-- Row count table -->
        <h2 style="margin:0 0 14px;font-size:16px;color:#1b5e20;
                    border-bottom:2px solid #1b5e20;padding-bottom:6px;">
          Table Summary: Bronze vs Silver
        </h2>
        <table width="100%" cellpadding="0" cellspacing="0"
               style="border-collapse:collapse;font-size:13px;">
          <thead>
            <tr style="background:#e8f5e9;">
              <th style="padding:10px 12px;text-align:left;">Bronze Table</th>
              <th style="padding:10px 12px;text-align:left;">Silver Table</th>
              <th style="padding:10px 12px;text-align:right;">Bronze Rows</th>
              <th style="padding:10px 12px;text-align:right;">Silver Rows</th>
              <th style="padding:10px 12px;text-align:right;">Removed</th>
              <th style="padding:10px 12px;text-align:left;">Reason</th>
            </tr>
          </thead>
          <tbody>{count_rows}</tbody>
        </table>

        <!-- QC results -->
        <h2 style="margin:28px 0 14px;font-size:16px;color:#1b5e20;
                    border-bottom:2px solid #1b5e20;padding-bottom:6px;">
          Quality Check Results (Silver Schema)
        </h2>
        <table width="100%" cellpadding="0" cellspacing="0"
               style="border-collapse:collapse;font-size:13px;">
          <tbody>{qc_rows}</tbody>
        </table>

      </td></tr>

      <!-- Footer -->
      <tr>
        <td style="background:#f4f6f9;padding:16px 32px;border-top:1px solid #e0e0e0;
                    font-size:11px;color:#888;">
          Automated report from Olist Data Pipeline &nbsp;|&nbsp;
          {DB}.BRONZE -> {DB}.SILVER &nbsp;|&nbsp; {run_time}
        </td>
      </tr>

    </table>
  </td></tr>
</table>
</body>
</html>"""


# ── Send ──────────────────────────────────────────────────────────────────────

def send_email(html, run_time):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[Olist Pipeline] Bronze to Silver Report - {run_time[:10]}"
    msg["From"]    = EMAIL_SENDER
    msg["To"]      = EMAIL_TO
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_TO, msg.as_string())


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("Connecting to Snowflake ...")
    conn   = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    try:
        print("Fetching row counts ...")
        row_counts = fetch_row_counts(cursor)
        for r in row_counts:
            tag = f"  (-{r['removed']:,} removed)" if r["removed"] else ""
            print(f"  {r['silver_table']:<55} {r['silver_count']:>9,}{tag}")

        print("\nRunning quality checks ...")
        qc_results = run_qc_checks(cursor)
        passed = sum(1 for r in qc_results if r["status"] == "PASS")
        failed = sum(1 for r in qc_results if r["status"] == "FAIL")
        print(f"  {passed} passed / {failed} failed out of {len(qc_results)} checks")

        print("\nBuilding HTML report ...")
        html = build_html(row_counts, qc_results, run_time)

        print(f"Sending email to {EMAIL_TO} ...")
        send_email(html, run_time)
        print("Email sent successfully.")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
