#!/usr/bin/env python3
"""
SWG Merchant — Report Generator
-------------------------------

Generates a Markdown and/or CSV report of sales, purchases, and customer
activity from swg_merchant.db for a chosen period:

- Rolling N months (default 12)
- Year-to-date (--ytd)
- All history (--all)

Usage:
  python report.py --db ./swg_merchant.db
  python report.py --db ./swg_merchant.db --out ./report_12mo.md
  python report.py --db ./swg_merchant.db --csv-dir ./report_12mo_csv
  python report.py --db ./swg_merchant.db --ytd
  python report.py --db ./swg_merchant.db --all
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from typing import Iterable, Sequence, Optional

DB_DEFAULT = "swg_merchant.db"


# ---------- Helpers ----------
def _format_value(v):
    """Format large numbers with commas, leave small/float values readable."""
    if isinstance(v, (int, float)):
        if isinstance(v, int):
            return f"{v:,}"
        # For floats, show 2 decimals if meaningful
        if abs(v - round(v)) < 0.01:
            return f"{int(round(v)):,}"
        return f"{v:,.2f}"
    return str(v)


def _print_table(headers: Sequence[str], rows: Iterable[Sequence[object]]) -> str:
    """Return a Markdown table as a string with formatted numeric values."""
    rows = [[_format_value(c) for c in r] for r in rows]
    headers = list(headers)
    cols = len(headers)
    widths = [len(str(h)) for h in headers]
    for r in rows:
        for c in range(cols):
            widths[c] = max(widths[c], len(str(r[c])) if c < len(r) else 0)

    def fmt_row(r: Sequence[object]) -> str:
        return "| " + " | ".join(str(r[c]).ljust(widths[c]) for c in range(cols)) + " |"

    out = []
    out.append(fmt_row(headers))
    out.append("| " + " | ".join("-" * widths[c] for c in range(cols)) + " |")
    for r in rows:
        out.append(fmt_row(r))
    out.append("")
    return "\n".join(out)


def _save_csv(path: Path, headers: Sequence[str], rows: Iterable[Sequence[object]]):
    """Write rows to CSV without formatting commas (for numeric processing)."""
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(list(headers))
        for r in rows:
            writer.writerow(list(r))


def _run(conn: sqlite3.Connection, sql: str, params: Sequence = ()):
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return cols, rows


def _month_clause(table: str) -> str:
    return f"strftime('%Y-%m', {table}.sale_date)"


def _since(col: str, start_expr: Optional[str]) -> str:
    """Return a WHERE clause like `WHERE col >= <start_expr>` or '' if no filter."""
    return "" if start_expr is None else f"WHERE {col} >= {start_expr}"


# ---------- Main report generator ----------
def generate_report(
    conn: sqlite3.Connection,
    *,
    months: int = 12,
    write_csv_dir: Optional[Path] = None,
    ytd: bool = False,
    all_history: bool = False,
) -> str:

    # --- Determine reporting window ---
    if all_history:
        start = None
        period_label = "All History"
        # Compute period bounds from BOTH sales and purchases so empty one doesn't break header
        start_date = conn.execute(
            """
            WITH bounds AS (
              SELECT MIN(date(sale_date)) AS d FROM sales
              UNION ALL
              SELECT MIN(date(sale_date))      FROM purchases
            )
            SELECT MIN(d) FROM bounds
            """
        ).fetchone()[0] or "(no data)"
        end_date = conn.execute(
            """
            WITH bounds AS (
              SELECT MAX(date(sale_date)) AS d FROM sales
              UNION ALL
              SELECT MAX(date(sale_date))      FROM purchases
            )
            SELECT MAX(d) FROM bounds
            """
        ).fetchone()[0] or "(no data)"
    elif ytd:
        start = "date('now','start of year')"
        period_label = "Year-To-Date"
        start_date = conn.execute(f"SELECT {start}").fetchone()[0]
        end_date   = conn.execute("SELECT date('now','start of day')").fetchone()[0]
    else:
        start = f"date('now','start of day','-{months} months')"
        period_label = f"Last {months} Months"
        start_date = conn.execute(f"SELECT {start}").fetchone()[0]
        end_date   = conn.execute("SELECT date('now','start of day')").fetchone()[0]

    # --- Header ---
    md = []
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    md.append(f"# SWG Merchant — {period_label} Report")
    md.append(f"_Generated {now_utc}_")
    md.append(f"_Period: {start_date} → {end_date} (UTC)_\n")

    # Conditional WHERE helpers
    where_sales     = _since("s.sale_date", start)   # for queries with alias s
    where_purchases = _since("p.sale_date", start)   # for queries with alias p
    where_plain     = _since("sale_date", start)     # for simple queries
    new_customers_where = "" if start is None else f"WHERE first_date >= {start}"

    # ---------- SALES OVERALL ----------
    sales_overall_sql = f"""
    WITH period AS (SELECT * FROM sales {where_plain})
    SELECT
      COALESCE(SUM(amount),0) AS total_revenue,
      COUNT(id) AS total_sales,
      ROUND(COALESCE(AVG(amount),0)) AS avg_sale,
      COALESCE(MAX(amount),0) AS max_sale,
      COUNT(DISTINCT item) AS distinct_items,
      COUNT(DISTINCT customer_id) AS distinct_customers
    FROM period;
    """
    _, rows = _run(conn, sales_overall_sql)
    total_revenue, total_sales, avg_sale, max_sale, distinct_items, distinct_customers = rows[0]

    # ---------- PURCHASES OVERALL ----------
    purchases_overall_sql = f"""
    WITH period AS (SELECT * FROM purchases {where_plain})
    SELECT
      COALESCE(SUM(amount),0) AS total_spent,
      COUNT(id) AS total_purchases,
      ROUND(COALESCE(AVG(amount),0)) AS avg_purchase,
      COUNT(DISTINCT vendor) AS distinct_vendors
    FROM period;
    """
    _, prow = _run(conn, purchases_overall_sql)
    total_spent, total_purchases, avg_purchase, distinct_vendors = prow[0]
    gross_margin = (total_revenue or 0) - (total_spent or 0)

    md.append("## Summary KPIs")
    md.append(
        _print_table(
            ["Metric", "Value"],
            [
                ("Total Revenue", total_revenue),
                ("Total Purchases (Spend)", total_spent),
                ("Gross Margin (Revenue − Spend)", gross_margin),
                ("Total Sales", total_sales),
                ("Avg Sale (credits)", avg_sale),
                ("Avg Purchase (credits)", avg_purchase),
                ("Distinct Items Sold", distinct_items),
                ("Active Customers", distinct_customers),
                ("Vendors Purchased From", distinct_vendors),
            ],
        )
    )

    # ---------- MONTHLY SALES TREND ----------
    monthly_sales_sql = f"""
    WITH m AS (
      SELECT {_month_clause('s')} AS month,
             COUNT(*) AS sales,
             SUM(amount) AS credits
      FROM sales s
      {where_sales}
      GROUP BY month
    ),
    prev AS (
      SELECT month, sales, credits,
             strftime('%Y-%m', date(month || '-01','-1 month')) AS prev_month
      FROM m
    )
    SELECT p.month, p.sales, p.credits,
           COALESCE(m2.sales, 0) AS prev_sales,
           COALESCE(m2.credits, 0) AS prev_credits,
           (p.sales - COALESCE(m2.sales,0)) AS mom_sales_delta,
           (p.credits - COALESCE(m2.credits,0)) AS mom_credits_delta
    FROM prev p
    LEFT JOIN m m2 ON m2.month = p.prev_month
    ORDER BY p.month;
    """
    monthly_cols, monthly_rows = _run(conn, monthly_sales_sql)
    md.append("## Monthly Sales Trend (with MoM deltas)")
    md.append(_print_table(monthly_cols, monthly_rows))
    if write_csv_dir:
        _save_csv(write_csv_dir / "monthly_sales.csv", monthly_cols, monthly_rows)

    # ---------- SALES BY CATEGORY ----------
    cat_cols, cat_rows = _run(
        conn,
        f"""
        SELECT COALESCE(category,'(uncategorized)') AS category,
               COUNT(*) AS sales, SUM(amount) AS credits,
               ROUND(AVG(amount)) AS avg_price
        FROM sales {where_plain}
        GROUP BY category ORDER BY credits DESC, sales DESC;
        """,
    )
    md.append("## Sales by Category")
    md.append(_print_table(cat_cols, cat_rows))

    # ---------- SALES BY PROFESSION ----------
    prof_cols, prof_rows = _run(
        conn,
        f"""
        SELECT COALESCE(profession,'(unknown)') AS profession,
               COUNT(*) AS sales, SUM(amount) AS credits,
               ROUND(AVG(amount)) AS avg_price
        FROM sales {where_plain}
        GROUP BY profession ORDER BY credits DESC, sales DESC;
        """,
    )
    md.append("## Sales by Profession")
    md.append(_print_table(prof_cols, prof_rows))

    # ---------- TOP ITEMS & VENDORS ----------
    for title, sql in [
        ("Top 10 Items (by quantity)",
         f"SELECT item, COUNT(*) AS sold, SUM(amount) AS credits, MAX(sale_date) AS last_sold "
         f"FROM sales {where_plain} GROUP BY item ORDER BY sold DESC, credits DESC LIMIT 10;"),
        ("Top 10 Items (by credits)",
         f"SELECT item, SUM(amount) AS credits, COUNT(*) AS sold, MAX(sale_date) AS last_sold "
         f"FROM sales {where_plain} GROUP BY item ORDER BY credits DESC, sold DESC LIMIT 10;"),
        ("Top 10 Vendors (by revenue)",
         f"SELECT vendor, SUM(amount) AS credits, COUNT(*) AS sales "
         f"FROM sales {where_plain} GROUP BY vendor ORDER BY credits DESC, sales DESC LIMIT 10;"),
    ]:
        c, r = _run(conn, sql)
        md.append(f"## {title}")
        md.append(_print_table(c, r))

    # ---------- PURCHASES: MONTHLY TREND & BY CATEGORY ----------
    purchases_trend_sql = f"""
    SELECT {_month_clause('p')} AS month,
           COUNT(*) AS purchases,
           SUM(amount) AS credits
    FROM purchases p
    {where_purchases}
    GROUP BY month
    ORDER BY month;
    """
    ptr_cols, ptr_rows = _run(conn, purchases_trend_sql)
    md.append("## Monthly Purchases Trend")
    md.append(_print_table(ptr_cols, ptr_rows))
    if write_csv_dir:
        _save_csv(write_csv_dir / "monthly_purchases.csv", ptr_cols, ptr_rows)

    purchases_by_cat_sql = f"""
    SELECT COALESCE(category,'(uncategorized)') AS category,
           COUNT(*) AS purchases,
           SUM(amount) AS credits,
           ROUND(AVG(amount)) AS avg_price
    FROM purchases
    {where_plain}
    GROUP BY category
    ORDER BY credits DESC, purchases DESC;
    """
    pbc_cols, pbc_rows = _run(conn, purchases_by_cat_sql)
    md.append("## Purchases by Category")
    md.append(_print_table(pbc_cols, pbc_rows))

    top_purchase_vendors_sql = f"""
    SELECT vendor, SUM(amount) AS credits, COUNT(*) AS purchases
    FROM purchases
    {where_plain}
    GROUP BY vendor
    ORDER BY credits DESC, purchases DESC
    LIMIT 10;
    """
    tpv_cols, tpv_rows = _run(conn, top_purchase_vendors_sql)
    md.append("## Top 10 Purchase Vendors (by spend)")
    md.append(_print_table(tpv_cols, tpv_rows))

    # ---------- CATEGORY-LEVEL MARGIN (Sales − Purchases) ----------
    cat_margin_sql_sqlite = f"""
    WITH s AS (
      SELECT COALESCE(category,'(uncategorized)') AS category, SUM(amount) AS sales_credits
      FROM sales {where_plain}
      GROUP BY category
    ),
    p AS (
      SELECT COALESCE(category,'(uncategorized)') AS category, SUM(amount) AS purchase_credits
      FROM purchases {where_plain}
      GROUP BY category
    ),
    lefty AS (
      SELECT s.category, s.sales_credits, COALESCE(p.purchase_credits,0) AS purchase_credits
      FROM s LEFT JOIN p ON p.category = s.category
    ),
    righty AS (
      SELECT p.category, COALESCE(s.sales_credits,0) AS sales_credits, p.purchase_credits
      FROM p LEFT JOIN s ON s.category = p.category
      WHERE s.category IS NULL
    ),
    merged AS (
      SELECT * FROM lefty
      UNION ALL
      SELECT * FROM righty
    )
    SELECT category, sales_credits, purchase_credits, (sales_credits - purchase_credits) AS margin
    FROM merged
    ORDER BY margin DESC, sales_credits DESC;
    """
    cm_cols, cm_rows = _run(conn, cat_margin_sql_sqlite)
    md.append("## Category Margin (Sales − Purchases)")
    md.append(_print_table(cm_cols, cm_rows))
    if write_csv_dir:
        _save_csv(write_csv_dir / "category_margin.csv", cm_cols, cm_rows)

    # ---------- CUSTOMERS (lifetime-based repeat rate) ----------
    customers_summary_sql = f"""
    WITH
      -- Sales within the report window
      period_sales AS (
        SELECT * FROM sales {where_plain}
      ),
      -- Active = bought at least once during the window
      active AS (
        SELECT DISTINCT customer_id FROM period_sales
      ),
      -- First-ever sale date per customer (for "new customers" in this window)
      first_ever AS (
        SELECT customer_id, MIN(sale_date) AS first_date
        FROM sales
        GROUP BY customer_id
      ),
      -- Lifetime total purchase counts (all time)
      lifetime_counts AS (
        SELECT customer_id, COUNT(*) AS lifetime_cnt
        FROM sales
        GROUP BY customer_id
      ),
      -- Period counts & spend (for avg spend per active customer)
      period_counts AS (
        SELECT customer_id, COUNT(*) AS cnt, SUM(amount) AS spent
        FROM period_sales
        GROUP BY customer_id
      ),
      -- Returning customers (lifetime): active in window AND lifetime_cnt > 1
      returning_lifetime AS (
        SELECT a.customer_id
        FROM active a
        JOIN lifetime_counts lc ON lc.customer_id = a.customer_id
        WHERE lc.lifetime_cnt > 1
      )
    SELECT
      (SELECT COUNT(*) FROM active) AS active_customers,
      (SELECT COUNT(*) FROM first_ever {new_customers_where}) AS new_customers,
      (SELECT COUNT(*) FROM returning_lifetime) AS returning_customers_lifetime,
      ROUND(
        (SELECT COALESCE(SUM(spent),0) FROM period_counts) * 1.0
        / NULLIF((SELECT COUNT(*) FROM active), 0),
        2
      ) AS avg_spend_per_customer
    ;
    """
    _, csum_rows = _run(conn, customers_summary_sql)
    active_customers, new_customers, returning_customers_lifetime, avg_spend_per_customer = csum_rows[0]

    # Repeat-purchase rate now based on lifetime totals for active customers
    repeat_rate = f"{round((returning_customers_lifetime or 0) * 100.0 / max(active_customers or 1, 1), 1)}%"

    md.append("## Customer Summary")
    md.append(
        _print_table(
            ["Metric", "Value"],
            [
                ("Active Customers", active_customers),
                ("New Customers", new_customers),
                ("Returning Customers (lifetime)", returning_customers_lifetime),
                ("Repeat-Purchase Rate (lifetime for active)", repeat_rate),
                ("Avg Spend per Active Customer", avg_spend_per_customer),
            ],
        )
    )

    # ---------- Top Customers ----------
    for title, order in [
        ("Top 10 Customers (by spend)", "ORDER BY credits DESC, orders DESC"),
        ("Top 10 Customers (by orders)", "ORDER BY orders DESC, credits DESC"),
    ]:
        sql = f"""
        SELECT c.name AS customer, SUM(s.amount) AS credits, COUNT(*) AS orders,
               MIN(s.sale_date) AS first, MAX(s.sale_date) AS last
        FROM sales s JOIN customers c ON c.id = s.customer_id
        {where_sales}
        GROUP BY c.id {order} LIMIT 10;
        """
        c, r = _run(conn, sql)
        md.append(f"## {title}")
        md.append(_print_table(c, r))

    return "\n".join(md)


# ---------- CLI ----------
def main():
    import argparse

    ap = argparse.ArgumentParser(description="Generate a sales/purchases/customers report from swg_merchant.db")
    ap.add_argument("--db", default=DB_DEFAULT, help="Path to SQLite DB (default: swg_merchant.db)")
    ap.add_argument("--months", type=int, default=12, help="Lookback window in months (default: 12)")
    ap.add_argument("--out", help="Optional path to write Markdown report")
    ap.add_argument("--csv-dir", help="Optional folder to write CSV extracts")
    ap.add_argument("--ytd", action="store_true", help="Generate a Year-To-Date report instead of a rolling-months window")
    ap.add_argument("--all", action="store_true", help="Report on all historical data (ignore date filters)")

    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    csv_dir = Path(args.csv_dir) if args.csv_dir else None
    report_md = generate_report(
        conn,
        months=args.months,
        write_csv_dir=csv_dir,
        ytd=args.ytd,
        all_history=args.all,
    )

    print(report_md)

    if args.out:
        outp = Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        outp.write_text(report_md, encoding="utf-8")
        print(f"\n[Saved] Markdown report → {outp}")


if __name__ == "__main__":
    main()
