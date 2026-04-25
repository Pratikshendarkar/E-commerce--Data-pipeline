"""
send_report.py

Standalone script — run this after data_cleaning.py to send an HTML email
report to pratik.shendarkar@rutgers.edu.

It queries the _clean tables in Snowflake directly, so it has no dependency
on data_cleaning.py and can be re-run at any time.

Usage:
    python data_ingestion/send_report.py
"""

import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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

SMTP_HOST      = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT      = int(os.getenv("SMTP_PORT", 587))
EMAIL_SENDER   = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_TO       = "pratik.shendarkar@rutgers.edu"


# ── Snowflake queries ─────────────────────────────────────────────────────────

def fetch_report(cursor):
    issues   = []
    outliers = []

    def q(sql):
        cursor.execute(sql)
        return cursor.fetchone()[0]

    # 1. Duplicate reviews removed
    orig  = q("SELECT COUNT(*) FROM olist_order_reviews")
    clean = q("SELECT COUNT(*) FROM olist_order_reviews_clean")
    removed = orig - clean
    if removed:
        issues.append({
            "table":       "olist_order_reviews",
            "description": "Duplicate review_id keys",
            "count":       removed,
            "action":      f"Removed {removed:,} duplicate rows; kept most complete record",
            "severity":    "high",
        })

    # 2. Zero-value payments flagged
    zero = q("SELECT COUNT(*) FROM olist_order_payments_clean WHERE is_zero_payment = TRUE")
    if zero:
        issues.append({
            "table":       "olist_order_payments",
            "description": "payment_value <= 0 (vouchers / free orders)",
            "count":       zero,
            "action":      "Kept rows; flagged with is_zero_payment = TRUE",
            "severity":    "medium",
        })

    # 3. Corrected installments
    bad_inst = q("""
        SELECT COUNT(*) FROM olist_order_payments
        WHERE payment_installments < 1
    """)
    if bad_inst:
        issues.append({
            "table":       "olist_order_payments",
            "description": "payment_installments < 1",
            "count":       bad_inst,
            "action":      "Corrected to 1 using GREATEST(payment_installments, 1)",
            "severity":    "medium",
        })

    # 4 & 5. Date inconsistencies in orders
    date_issues = q("SELECT COUNT(*) FROM olist_orders_clean WHERE has_date_inconsistency = TRUE")
    if date_issues:
        issues.append({
            "table":       "olist_orders",
            "description": "Invalid delivery date sequence (carrier/customer dates out of order)",
            "count":       date_issues,
            "action":      "Nullified invalid dates; flagged with has_date_inconsistency = TRUE",
            "severity":    "high",
        })

    # 6. Order items outliers
    price_out   = q("SELECT COUNT(*) FROM olist_order_items_clean WHERE price_is_outlier = TRUE")
    freight_out = q("SELECT COUNT(*) FROM olist_order_items_clean WHERE freight_is_outlier = TRUE")

    cursor.execute("""
        SELECT
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price),
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price),
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY freight_value),
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY freight_value)
        FROM olist_order_items
    """)
    pq1, pq3, fq1, fq3 = cursor.fetchone()
    p_iqr = float(pq3 - pq1)
    f_iqr = float(fq3 - fq1)
    outliers.append({
        "table":  "olist_order_items",
        "column": "price",
        "count":  price_out,
        "bounds": f"[{float(pq1) - 1.5*p_iqr:.2f}, {float(pq3) + 1.5*p_iqr:.2f}]",
    })
    outliers.append({
        "table":  "olist_order_items",
        "column": "freight_value",
        "count":  freight_out,
        "bounds": f"[{float(fq1) - 1.5*f_iqr:.2f}, {float(fq3) + 1.5*f_iqr:.2f}]",
    })

    # 7. Payment value outliers
    pay_out = q("SELECT COUNT(*) FROM olist_order_payments_clean WHERE payment_is_outlier = TRUE")
    cursor.execute("""
        SELECT
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY payment_value),
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY payment_value)
        FROM olist_order_payments WHERE payment_value > 0
    """)
    vq1, vq3 = cursor.fetchone()
    v_iqr = float(vq3 - vq1)
    outliers.append({
        "table":  "olist_order_payments",
        "column": "payment_value",
        "count":  pay_out,
        "bounds": f"[0, {float(vq3) + 1.5*v_iqr:.2f}]",
    })

    # 8. Product dimension / weight outliers
    for col in ("product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"):
        flag_col = f"{col}_is_outlier"
        cnt = q(f"SELECT COUNT(*) FROM olist_products_clean WHERE {flag_col} = TRUE")
        cursor.execute(f"""
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col}),
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col})
            FROM olist_products WHERE {col} IS NOT NULL
        """)
        dq1, dq3 = (float(v) for v in cursor.fetchone())
        d_iqr = dq3 - dq1
        outliers.append({
            "table":  "olist_products",
            "column": col,
            "count":  cnt,
            "bounds": f"[{dq1 - 1.5*d_iqr:.1f}, {dq3 + 1.5*d_iqr:.1f}]",
        })

    clean_tables = [
        "olist_order_reviews_clean",
        "olist_order_payments_clean",
        "olist_orders_clean",
        "olist_order_items_clean",
        "olist_products_clean",
    ]

    return {
        "run_time":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "issues":       issues,
        "outliers":     outliers,
        "clean_tables": clean_tables,
    }


# ── HTML builder ──────────────────────────────────────────────────────────────

def _badge(text, color):
    return (
        f'<span style="background:{color};color:#fff;padding:2px 8px;'
        f'border-radius:4px;font-size:12px;font-weight:600;">{text}</span>'
    )


def build_html(report):
    run_time   = report["run_time"]
    issues     = report["issues"]
    outliers   = report["outliers"]
    clean_tbls = report["clean_tables"]

    total_violations = sum(i["count"] for i in issues)
    total_outliers   = sum(o["count"] for o in outliers)

    issue_rows = ""
    for i in issues:
        color = "#e53935" if i["severity"] == "high" else "#fb8c00"
        issue_rows += f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;">{i['table']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;">{i['description']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;">
              <strong style="color:{color};">{i['count']:,}</strong></td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;">{i['action']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;">
              {_badge(i['severity'].upper(), color)}</td>
        </tr>"""

    outlier_rows = ""
    for o in outliers:
        outlier_rows += f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;">{o['table']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;">{o['column']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;">
              <strong style="color:#e53935;">{o['count']:,}</strong></td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;font-family:monospace;">
              {o['bounds']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;">
              {_badge('CAPPED', '#1e88e5')}</td>
        </tr>"""

    table_list = "".join(
        f'<li style="padding:3px 0;font-family:monospace;">{t}</li>'
        for t in clean_tbls
    )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f4f6f9;font-family:Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0">
  <tr><td align="center" style="padding:30px 20px;">
    <table width="680" cellpadding="0" cellspacing="0"
           style="background:#fff;border-radius:8px;overflow:hidden;
                  box-shadow:0 2px 8px rgba(0,0,0,.1);">

      <!-- Header -->
      <tr>
        <td style="background:#1565c0;padding:28px 32px;">
          <h1 style="margin:0;color:#fff;font-size:22px;">Data Cleaning Report</h1>
          <p style="margin:6px 0 0;color:#bbdefb;font-size:13px;">
            Olist E-Commerce Pipeline &nbsp;|&nbsp; {run_time}
          </p>
        </td>
      </tr>

      <!-- KPI strip -->
      <tr>
        <td style="padding:20px 32px;background:#e3f2fd;">
          <table width="100%" cellpadding="0" cellspacing="0"><tr>
            <td align="center" style="padding:10px;">
              <div style="font-size:32px;font-weight:700;color:#e53935;">{total_violations:,}</div>
              <div style="font-size:12px;color:#555;margin-top:4px;">Inconsistencies Fixed</div>
            </td>
            <td align="center" style="padding:10px;border-left:1px solid #90caf9;">
              <div style="font-size:32px;font-weight:700;color:#fb8c00;">{total_outliers:,}</div>
              <div style="font-size:12px;color:#555;margin-top:4px;">Outliers Capped</div>
            </td>
            <td align="center" style="padding:10px;border-left:1px solid #90caf9;">
              <div style="font-size:32px;font-weight:700;color:#2e7d32;">{len(clean_tbls)}</div>
              <div style="font-size:12px;color:#555;margin-top:4px;">Clean Tables in Snowflake</div>
            </td>
          </tr></table>
        </td>
      </tr>

      <!-- Body -->
      <tr><td style="padding:28px 32px;">

        <h2 style="margin:0 0 14px;font-size:16px;color:#1565c0;
                    border-bottom:2px solid #1565c0;padding-bottom:6px;">
          Data Inconsistencies Fixed
        </h2>
        <table width="100%" cellpadding="0" cellspacing="0"
               style="border-collapse:collapse;font-size:13px;">
          <thead>
            <tr style="background:#e3f2fd;">
              <th style="padding:10px 12px;text-align:left;">Table</th>
              <th style="padding:10px 12px;text-align:left;">Issue</th>
              <th style="padding:10px 12px;text-align:center;">Rows Affected</th>
              <th style="padding:10px 12px;text-align:left;">Action Taken</th>
              <th style="padding:10px 12px;text-align:center;">Severity</th>
            </tr>
          </thead>
          <tbody>{issue_rows}</tbody>
        </table>

        <h2 style="margin:28px 0 14px;font-size:16px;color:#1565c0;
                    border-bottom:2px solid #1565c0;padding-bottom:6px;">
          Outliers Detected &amp; Capped (IQR Method)
        </h2>
        <table width="100%" cellpadding="0" cellspacing="0"
               style="border-collapse:collapse;font-size:13px;">
          <thead>
            <tr style="background:#e3f2fd;">
              <th style="padding:10px 12px;text-align:left;">Table</th>
              <th style="padding:10px 12px;text-align:left;">Column</th>
              <th style="padding:10px 12px;text-align:center;">Outlier Count</th>
              <th style="padding:10px 12px;text-align:left;">Valid Bounds</th>
              <th style="padding:10px 12px;text-align:center;">Action</th>
            </tr>
          </thead>
          <tbody>{outlier_rows}</tbody>
        </table>

        <h2 style="margin:28px 0 14px;font-size:16px;color:#1565c0;
                    border-bottom:2px solid #1565c0;padding-bottom:6px;">
          Clean Tables Available in Snowflake
        </h2>
        <ul style="margin:0;padding-left:20px;font-size:13px;color:#333;line-height:1.8;">
          {table_list}
        </ul>

      </td></tr>

      <!-- Footer -->
      <tr>
        <td style="background:#f4f6f9;padding:16px 32px;border-top:1px solid #e0e0e0;
                    font-size:11px;color:#888;">
          Automated alert from the Olist Data Pipeline &nbsp;|&nbsp;
          Snowflake: DATAENGINEER.BRONZE &nbsp;|&nbsp; {run_time}
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
    msg["Subject"] = f"[Olist Pipeline] Data Cleaning Report - {run_time[:10]}"
    msg["From"]    = EMAIL_SENDER
    msg["To"]      = EMAIL_TO
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_TO, msg.as_string())


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Connecting to Snowflake ...")
    conn   = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    try:
        print("Fetching cleaning stats from Snowflake ...")
        report = fetch_report(cursor)

        total_issues   = sum(i["count"] for i in report["issues"])
        total_outliers = sum(o["count"] for o in report["outliers"])
        print(f"  Inconsistencies : {total_issues:,}")
        print(f"  Outliers        : {total_outliers:,}")
        print(f"  Clean tables    : {len(report['clean_tables'])}")

        print(f"\nBuilding HTML report ...")
        html = build_html(report)

        print(f"Sending email to {EMAIL_TO} ...")
        send_email(html, report["run_time"])
        print("Email sent successfully.")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
