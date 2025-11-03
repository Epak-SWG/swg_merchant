#!/usr/bin/env python3
"""
Import historical merchant sales from a CSV file into swg_merchant.db.

CSV format expected:
Date,Vendor,Customer,Item,Price,Profession,Category

Example:
02/13/23,Grind Kits,hara,Grind Kit: Droid Engineer,"150,000",Artisan,Grind Kit

python import_sales_csv.py SALESDATA.csv

"""

import csv
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path("swg_merchant.db")


# ---------- Reuse your DB helpers ----------

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    total_spent INTEGER NOT NULL DEFAULT 0 CHECK (total_spent >= 0),
    total_purchases INTEGER NOT NULL DEFAULT 0 CHECK (total_purchases >= 0)
);

CREATE TABLE IF NOT EXISTS sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_date TEXT NOT NULL,
    vendor TEXT NOT NULL,
    item TEXT NOT NULL,
    customer_id INTEGER NOT NULL,
    amount INTEGER NOT NULL CHECK (amount >= 0),
    profession TEXT,
    category TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS purchases (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_date  TEXT    NOT NULL,
    item       TEXT    NOT NULL,
    vendor     TEXT    NOT NULL,
    amount     INTEGER NOT NULL CHECK (amount >= 0),
    category   TEXT
);

/* ---------- Single-row ingest table (replaces processed_mails + mail_rows) ----------
   Exactly ONE row per .mail file.
   Each row points to either a sales.id OR a purchases.id via FK.
*/
CREATE TABLE IF NOT EXISTS mail_ingests (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    mail_id      TEXT,              -- first non-empty line in the file (if present)
    file_path    TEXT NOT NULL UNIQUE,     -- absolute path
    file_mtime   INTEGER NOT NULL,  -- os.stat().st_mtime
    inserted_at  TEXT NOT NULL DEFAULT (datetime('now')),
    sale_id      INTEGER,           -- FK to sales.id (nullable)
    purchase_id  INTEGER,           -- FK to purchases.id (nullable)

    CHECK (
      (sale_id IS NOT NULL AND purchase_id IS NULL) OR
      (sale_id IS NULL     AND purchase_id IS NOT NULL)
    ),

    UNIQUE (mail_id),

    FOREIGN KEY (sale_id)     REFERENCES sales(id)      ON DELETE CASCADE,
    FOREIGN KEY (purchase_id) REFERENCES purchases(id)  ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_mail_ingests_mtime ON mail_ingests(file_mtime);

CREATE INDEX IF NOT EXISTS idx_purchases_date     ON purchases(sale_date);
CREATE INDEX IF NOT EXISTS idx_purchases_category ON purchases(category);
CREATE INDEX IF NOT EXISTS idx_purchases_vendor   ON purchases(vendor);
CREATE INDEX IF NOT EXISTS idx_sales_date         ON sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_vendor       ON sales(vendor);
CREATE INDEX IF NOT EXISTS idx_sales_customer     ON sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_item         ON sales(item);
CREATE INDEX IF NOT EXISTS idx_sales_profession   ON sales(profession);
CREATE INDEX IF NOT EXISTS idx_sales_category     ON sales(category);
CREATE INDEX IF NOT EXISTS idx_customers_name     ON customers(name);
"""

def ensure_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA)
    return conn


def get_or_create_customer(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute("SELECT id FROM customers WHERE name = ?", (name,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur = conn.execute("INSERT INTO customers (name) VALUES (?)", (name,))
    conn.commit()
    return cur.lastrowid


def insert_sale(
    conn: sqlite3.Connection,
    *,
    sale_date: str,
    vendor: str,
    item: str,
    customer: str,
    amount: int,
    profession: str | None = None,
    category: str | None = None,
) -> int:
    cust_id = get_or_create_customer(conn, customer)

    cur = conn.execute(
        """INSERT INTO sales (sale_date, vendor, item, customer_id, amount, profession, category)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (sale_date, vendor, item, cust_id, amount, profession, category),
    )

    # Update customer totals
    conn.execute(
        """UPDATE customers
           SET total_spent = total_spent + ?, total_purchases = total_purchases + 1
           WHERE id = ?""",
        (amount, cust_id),
    )
    conn.commit()
    return cur.lastrowid


# ---------- Importer ----------

def import_csv(csv_path: Path, conn: sqlite3.Connection):
    count = 0
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # Parse date (MM/DD/YY → ISO)
                dt = datetime.strptime(row["Date"], "%m/%d/%y")
                sale_date = dt.strftime("%Y-%m-%d 00:00:00")

                # Parse amount (remove commas, quotes)
                price_raw = str(row["Price"]).replace(",", "").strip().replace('"', "")
                amount = int(float(price_raw))

                vendor = row["Vendor"].strip()
                customer = row["Customer"].strip()
                item = row["Item"].strip()
                profession = row.get("Profession", "").strip() or None
                category = row.get("Category", "").strip() or None

                insert_sale(
                    conn,
                    sale_date=sale_date,
                    vendor=vendor,
                    item=item,
                    customer=customer,
                    amount=amount,
                    profession=profession,
                    category=category,
                )

                count += 1
            except Exception as e:
                print(f"[WARN] Skipping row: {row}\n  Reason: {e}")
    print(f"✅ Imported {count} sales from {csv_path}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Import sales from CSV into swg_merchant.db")
    parser.add_argument("csv_file", help="Path to sales CSV file")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to database (default: swg_merchant.db)")
    args = parser.parse_args()

    csv_path = Path(args.csv_file)
    if not csv_path.exists():
        print(f"[ERROR] File not found: {csv_path}")
        return

    conn = ensure_db(Path(args.db))
    import_csv(csv_path, conn)
    conn.close()


if __name__ == "__main__":
    main()
