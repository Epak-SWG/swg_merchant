#!/usr/bin/env python3
"""
SWG Merchant Log Parser → SQLite
"""

import re
import sqlite3
from pathlib import Path
import datetime
from typing import Optional, Iterable, Sequence

# ---------- DB ----------
DEFAULT_DB = Path("swg_merchant.db")

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

/* Exactly ONE row per .mail file, pointing to either a sale or a purchase. */
CREATE TABLE IF NOT EXISTS mail_ingests (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    mail_id      TEXT,
    file_path    TEXT NOT NULL UNIQUE,
    file_mtime   INTEGER NOT NULL,
    inserted_at  TEXT NOT NULL DEFAULT (datetime('now')),
    sale_id      INTEGER,
    purchase_id  INTEGER,
    CHECK ((sale_id IS NOT NULL AND purchase_id IS NULL) OR (sale_id IS NULL AND purchase_id IS NOT NULL)),
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

def ensure_db(db_path: Path = DEFAULT_DB) -> sqlite3.Connection:
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

def get_mail_identity(p: Path) -> tuple[Optional[str], int]:
    mail_id: Optional[str] = None
    with p.open("r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            s = ln.strip()
            if s:
                mail_id = s
                break
    return mail_id, int(p.stat().st_mtime)

def is_mail_processed(conn: sqlite3.Connection, *, mail_id: Optional[str], file_path: str) -> bool:
    if mail_id:
        if conn.execute("SELECT 1 FROM mail_ingests WHERE mail_id = ? LIMIT 1", (mail_id,)).fetchone():
            return True
    return conn.execute("SELECT 1 FROM mail_ingests WHERE file_path = ? LIMIT 1", (file_path,)).fetchone() is not None

def upsert_mail_ingest(conn: sqlite3.Connection, *, mail_id: Optional[str], file_path: str, file_mtime: int,
                       sale_id: Optional[int] = None, purchase_id: Optional[int] = None) -> None:
    if (sale_id is None) == (purchase_id is None):
        raise ValueError("Provide exactly one of sale_id or purchase_id")
    cur = conn.execute(
        """UPDATE mail_ingests
               SET mail_id = COALESCE(?, mail_id),
                   file_mtime = ?,
                   sale_id = COALESCE(?, sale_id),
                   purchase_id = COALESCE(?, purchase_id),
                   inserted_at = datetime('now')
             WHERE file_path = ?""",
        (mail_id, file_mtime, sale_id, purchase_id, file_path),
    )
    if cur.rowcount == 0:
        conn.execute(
            "INSERT INTO mail_ingests (mail_id, file_path, file_mtime, sale_id, purchase_id) VALUES (?, ?, ?, ?, ?)",
            (mail_id, file_path, file_mtime, sale_id, purchase_id),
        )
    conn.commit()

def has_incomplete_rows_for_file(conn: sqlite3.Connection, *, file_path: str) -> bool:
    cur = conn.execute(
        """SELECT 1
               FROM mail_ingests mi
               JOIN sales s ON s.id = mi.sale_id
              WHERE mi.file_path = ? AND (s.profession IS NULL OR s.category IS NULL)
              LIMIT 1""", (file_path,)
    )
    if cur.fetchone():
        return True
    cur = conn.execute(
        """SELECT 1
               FROM mail_ingests mi
               JOIN purchases p ON p.id = mi.purchase_id
              WHERE mi.file_path = ? AND (p.category IS NULL)
              LIMIT 1""", (file_path,)
    )
    return cur.fetchone() is not None

# ---------- Parsing ----------
SALE_RE = re.compile(
    r"Vendor:\s*(?P<vendor>.+?)\s+has sold\s+(?P<item>.+?)\s+to\s+(?P<customer>.+?)\s+for\s+(?P<amount>\d+)\s+credits",
    re.IGNORECASE,
)
PURCHASE_RE = re.compile(
    r"You have won the auction of \"(?P<item>.+?)\" from \"(?P<vendor>.+?)\" for (?P<amount>\d+) credits.*",
    re.IGNORECASE,
)
TS_RE = re.compile(r"TIMESTAMP:\s*(?P<ts>\d+)", re.IGNORECASE)

def _read_nonempty_lines(p: Path) -> list[str]:
    with p.open("r", encoding="utf-8", errors="ignore") as f:
        return [ln.strip() for ln in f if ln.strip()]

def parse_mail_file(file_path: Path) -> Optional[dict]:
    try:
        lines = _read_nonempty_lines(file_path)
    except Exception as e:
        print(f"[WARN] Could not read {file_path}: {e}")
        return None
    if len(lines) < 6:
        print(f"[WARN] {file_path.name}: not enough lines to parse (got {len(lines)}).")
        return None

    event_line = lines[2].strip()
    timestamp_line = lines[3]
    sale_line = lines[4]

    ts_m = TS_RE.search(timestamp_line)
    if not ts_m:
        print(f"[WARN] {file_path.name}: missing TIMESTAMP line.")
        return None
    try:
        ts = int(ts_m.group("ts"))
    except ValueError:
        print(f"[WARN] {file_path.name}: invalid TIMESTAMP value.")
        return None

    sale_date = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

    if "Vendor Sale Complete" in event_line:
        sm = SALE_RE.search(sale_line)
        if not sm:
            print(f"[WARN] {file_path.name}: could not parse sale line:\n       {sale_line}")
            return None
        return {
            "type": "sale",
            "sale_date": sale_date,
            "vendor": sm.group("vendor").strip(),
            "item": sm.group("item").strip(),
            "customer": sm.group("customer").strip(),
            "amount": int(sm.group("amount")),
        }
    elif "Vendor Item Purchased" in event_line:
        pm = PURCHASE_RE.search(sale_line)
        if not pm:
            print(f"[WARN] {file_path.name}: could not parse purchase line:\n       {sale_line}")
            return None
        return {
            "type": "purchase",
            "sale_date": sale_date,
            "vendor": pm.group("vendor").strip(),
            "item": pm.group("item").strip(),
            "amount": int(pm.group("amount")),
        }
    else:
        print(f"[WARN] {file_path.name}: unknown event type: {event_line}")
        return None

def classify_vendor_and_item(entry_type: str, vendor: str, item: str) -> tuple[Optional[str], Optional[str]]:
    #Very lightweight heuristic to infer (profession, category) from vendor name and item words.
    profession, category = None, None
    v = (vendor or "").casefold()
    i = (item or "").casefold()
    if entry_type == "sale":
        if "armor and vehicles" in v:
            profession = "Armorsmith"
            if any(w in i for w in ["ab-1","eta-1","flare s swoop","xj-6","basilisk war droid"]):
                profession = "Artisan"; category = "Vehicle"
            elif "bone" in i: category = "Bone Armor"
            elif "composite" in i: category = "Composite"
            elif "psg" in i or "personal shield" in i: category = "PSG"
            elif "r.i.s." in i: category = "RIS"
            elif "tantel" in i: category = "Tantel"
        elif "buffbot" in v: profession = "Doctor"; category = "Buff"
        elif "chef" in v: profession = "Chef"; category = "Chef"
        elif "pets" in v:
            profession = "Bio-Engineer"; category = "Pet"
            if "egg" in i: category = "Incubation"
        elif "pharmaceuticals" in v:
            if any(w in i for w in ["active","coagulant","fear release","scent","tensile"]): profession = "Bio-Engineer"; category = "BE Tissue"
            elif any(w in i for w in ["buff","enhance","small stimpack"]):
                profession = "Doctor"; category = "Buff Packs" if any(w in i for w in ["buff","enhance"]) else "Stimpack"
            elif "hssiss" in i: profession = "Combat Medic"; category = "Dart"
            elif "pet stimpack" in i or "vitality" in i: profession = "Bio-Engineer"; category = "Stimpack"
        elif "resources" in v:
            profession = "Artisan"; category = "Resources"
        elif "weapons" in v:
            profession = "Weaponsmith"
            if "carbine" in i: category = "Carbine"
            elif "two-handed curved sword" in i: category = "Two Hand"
            elif "curved sword" in i: category = "One Hand"
            elif "rifle" in i: category = "Rifle"
            elif "vibro knuckler" in i: category = "Unarmed"
            elif "flame thrower" in i or "flamethrower" in i or "launcher pistol" in i: category = "Commando"
            elif "pistol" in i or "dl44 xt" in i: category = "Pistol"
            elif "long vibro axe" in i or "nightsister energy lance" in i:
                category = "Polearm"
        elif "world drops" in v:
            profession = "loot"
            if any(w in i for w in ["[ca]","[aa]"]): category = "Tapes"
            elif any(w in i for w in ["crystal","pearl"]): category = "Crystal"
            elif any(w in i for w in ["geonosian power","venom"]): category = "Component"
            elif "holocron" in i: category = "Misc"
            elif "nightsister clothing" in i: category = "Schematic"
            elif "schematic" in i: category = "Schematics"
            elif "treasure map" in i: category = "Treasure Map"
            elif "carbine" in i: category = "Carbine"
            elif "two-handed curved sword" in i: category = "Two Hand"
            elif "curved sword" in i: category = "One Hand"
            elif "rifle" in i: category = "Rifle"
            elif "pistol" in i or "dl44 xt" in i: category = "Pistol"
            elif "long vibro axe" in i or "nightsister energy lance" in i: category = "Polearm"
    elif entry_type == "purchase":
        if "geonosian power cube" in i: category = "Component"
        elif "blood" in i: category = "Blood"
        elif "aurilian plant" in i: category = "Aurilian"
        elif "cpu>" in i: category = "Resources"
    return profession, category

# ---------- File discovery & inserts ----------
def iter_mail_paths(target: Path) -> list[Path]:
    """
    Resolve all .mail files.
      - If `target` is a .mail file, return [target].
      - If `target` is a directory, recurse **by default** and return every *.mail under it.
    """
    if target.is_file() and target.suffix.lower() == ".mail":
        return [target]
    if not target.is_dir():
        return []
    return sorted(target.glob("**/*.mail"))

def insert_purchase(conn: sqlite3.Connection, *, sale_date: str, item: str, vendor: str,
                    amount: int, category: Optional[str] = None) -> int:
    cur = conn.execute(
        "INSERT INTO purchases (sale_date, item, vendor, amount, category) VALUES (?, ?, ?, ?, ?)",
        (sale_date, item, vendor, amount, category),
    )
    conn.commit()
    return cur.lastrowid

def insert_sale(conn: sqlite3.Connection, *, sale_date: str, vendor: str, item: str,
                customer: str, amount: int, profession: Optional[str] = None,
                category: Optional[str] = None) -> int:
    cust_id = get_or_create_customer(conn, customer)
    cur = conn.execute(
        "INSERT INTO sales (sale_date, vendor, item, customer_id, amount, profession, category) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (sale_date, vendor, item, cust_id, amount, profession, category),
    )
    conn.execute(
        "UPDATE customers SET total_spent = total_spent + ?, total_purchases = total_purchases + 1 WHERE id = ?",
        (amount, cust_id),
    )
    conn.commit()
    return cur.lastrowid

# ---------- Recommendation helpers with profession & category filters ----------
def _print_table(headers: Sequence[str], rows: Iterable[Sequence[object]]) -> None:
    rows = list(rows)
    cols = len(headers)
    widths = [len(str(h)) for h in headers]
    for r in rows:
        for c in range(cols):
            widths[c] = max(widths[c], len(str(r[c])) if c < len(r) else 0)
    def fmt_row(r: Sequence[object]) -> str:
        return "  " + "  |  ".join(str(r[c]).ljust(widths[c]) for c in range(cols))
    print(fmt_row(headers))
    print("  " + "  +  ".join("-" * w for w in widths))
    for r in rows:
        print(fmt_row(r))
    print()

def _build_in_clause(column: str, values: Optional[list[str]]) -> tuple[str, list]:
    if values:
        placeholders = ",".join("?" for _ in values)
        return f" AND {column} IN ({placeholders}) ", list(values)
    return "", []

def _filters_where(professions: Optional[list[str]], categories: Optional[list[str]]) -> tuple[str, list]:
    sql = ""
    params: list = []
    prof_sql, prof_params = _build_in_clause("profession", professions)
    cat_sql, cat_params   = _build_in_clause("category", categories)
    sql += prof_sql + cat_sql
    params += prof_params + cat_params
    return sql, params

def _filters_label(professions: Optional[list[str]], categories: Optional[list[str]]) -> str:
    bits = []
    if professions: bits.append("professions=" + ",".join(professions))
    if categories:  bits.append("categories=" + ",".join(categories))
    return (" (" + "; ".join(bits) + ")") if bits else ""

def suggest_items_to_restock(conn: sqlite3.Connection, *, days:int=30, min_sales:int=2, top:int=20,
                             professions: Optional[list[str]] = None, categories: Optional[list[str]] = None):
    filt_sql, filt_params = _filters_where(professions, categories)
    sql = f"""
        SELECT item, COUNT(*) AS sold, SUM(amount) AS credits, MAX(sale_date) AS last_sold
        FROM sales
        WHERE sale_date >= date('now', ?)
        {filt_sql}
        GROUP BY item
        HAVING sold >= ?
        ORDER BY datetime(last_sold) DESC, sold DESC, credits DESC
        LIMIT ?
    """
    params = [f"-{days} days", *filt_params, min_sales, top]
    return list(conn.execute(sql, params))

def top_categories_recent(conn: sqlite3.Connection, *, days:int=30, top:int=20,
                          professions: Optional[list[str]] = None, categories: Optional[list[str]] = None):
    filt_sql, filt_params = _filters_where(professions, categories)
    sql = f"""
        SELECT COALESCE(category, '(uncategorized)') AS category,
               COUNT(*) AS sales,
               SUM(amount) AS credits,
               ROUND(AVG(amount)) AS avg_price
        FROM sales
        WHERE sale_date >= date('now', ?)
        {filt_sql}
        GROUP BY category
        ORDER BY sales DESC, credits DESC
        LIMIT ?
    """
    params = [f"-{days} days", *filt_params, top]
    return list(conn.execute(sql, params))

def safe_trending_categories(conn: sqlite3.Connection,
                             professions: Optional[list[str]] = None, categories: Optional[list[str]] = None):
    filt_sql, filt_params = _filters_where(professions, categories)
    sql = f"""
        WITH monthly AS (
          SELECT COALESCE(category, '(uncategorized)') AS category,
                 strftime('%Y-%m', sale_date) AS month,
                 COUNT(*) AS sold
          FROM sales
          WHERE 1=1
          {filt_sql}
          GROUP BY category, month
        ),
        last AS (
          SELECT category, sold FROM monthly WHERE month = strftime('%Y-%m', 'now', '-1 month')
        ),
        prev AS (
          SELECT category, sold FROM monthly WHERE month = strftime('%Y-%m', 'now', '-2 month')
        ),
        merged AS (
          SELECT l.category AS category, l.sold AS last_month, COALESCE(p.sold, 0) AS prev_month
          FROM last l LEFT JOIN prev p ON p.category = l.category
          UNION
          SELECT p.category AS category, 0 AS last_month, p.sold AS prev_month
          FROM prev p LEFT JOIN last l ON l.category = p.category
          WHERE l.category IS NULL
        )
        SELECT category, last_month, prev_month, (last_month - prev_month) AS delta
        FROM merged
        ORDER BY delta DESC, last_month DESC
    """
    return list(conn.execute(sql, filt_params))

def run_recommendations(conn: sqlite3.Connection, *, days:int=30, min_sales:int=2, top:int=20,
                        professions: Optional[list[str]] = None, categories: Optional[list[str]] = None) -> None:
    print("\n=== Recommendations ===\n")
    label_tail = _filters_label(professions, categories)

    # 1) Items to restock
    items = suggest_items_to_restock(conn, days=days, min_sales=min_sales, top=top,
                                     professions=professions, categories=categories)
    print(f"Top items to restock (last {days} days, min {min_sales} sales){label_tail}")
    if items:
        _print_table(["Item", "Sold", "Credits", "Last Sold (UTC)"], [(r[0], r[1], r[2], r[3]) for r in items])
    else:
        print("  (No qualifying items yet)\n")

    # 2) Hottest categories recently
    cats = top_categories_recent(conn, days=days, top=top, professions=professions, categories=categories)
    print(f"Hottest categories{label_tail}")
    if cats:
        _print_table(["Category", "Sales", "Credits", "Avg Price"], [(r[0], r[1], r[2], r[3]) for r in cats])
    else:
        print("  (No category data yet)\n")

    # 3) Trending categories (MoM)
    print(f"Trending categories (last month vs. previous month){label_tail}")
    trend = safe_trending_categories(conn, professions=professions, categories=categories)
    if trend:
        _print_table(["Category", "Last Mo", "Prev Mo", "Δ"], [(r[0], r[1], r[2], r[3]) for r in trend])
    else:
        print("  (Not enough history yet)\n")

# ---------- CLI ----------
def main():
    import argparse
    ap = argparse.ArgumentParser(description="Parse SWG .mail vendor sales into SQLite and analyze recommendations.")
    ap.add_argument("path", nargs="?", help="Path to a .mail file or a folder (directory is searched recursively)")
    ap.add_argument("--db", default=str(DEFAULT_DB), help="Path to SQLite DB (default: swg_merchant.db)")

    # Recommendation flags
    ap.add_argument("--recommend", action="store_true", help="Print craft/list recommendations from recent sales")
    ap.add_argument("--days", type=int, default=30, help="Lookback window in days for recent sales (default: 30)")
    ap.add_argument("--min-sales", type=int, default=2, help="Minimum sales per item to be considered (default: 2)")
    ap.add_argument("--top", type=int, default=20, help="Max rows to show in each section (default: 20)")
    ap.add_argument("--profession", nargs="*", help="Filter by profession (e.g. --profession Doctor Chef)")
    ap.add_argument("--category", nargs="*", help="Filter by category (e.g. --category Buff PSG Vehicle)")

    args = ap.parse_args()
    conn = ensure_db(Path(args.db))

    # Optional parse pass
    if args.path:
        target = Path(args.path)
        mail_files = iter_mail_paths(target)
        if not mail_files:
            print(f"[INFO] No .mail files found at: {target}")
        else:
            inserted = 0
            failed = 0
            skipped = 0
            print(f"[INFO] Found {len(mail_files)} .mail file(s). Parsing and inserting into {args.db} ...")
            for pth in mail_files:
                file_path = str(pth)
                mail_id, mtime = get_mail_identity(pth)
                already = is_mail_processed(conn, mail_id=mail_id, file_path=file_path)
                # If incomplete linked data, purge and reparse
                incomplete = has_incomplete_rows_for_file(conn, file_path=file_path)

                if already and not incomplete:
                    skipped += 1
                    continue
                if already and incomplete:
                    print(f"[REPARSE] Incomplete data found; purging and reparsing: {pth.name}")
                    conn.execute("DELETE FROM sales WHERE id IN (SELECT sale_id FROM mail_ingests WHERE file_path = ?)", (file_path,))
                    conn.execute("DELETE FROM purchases WHERE id IN (SELECT purchase_id FROM mail_ingests WHERE file_path = ?)", (file_path,))
                    conn.execute("DELETE FROM mail_ingests WHERE file_path = ?", (file_path,))
                    conn.commit()

                entry = parse_mail_file(pth)
                if not entry:
                    failed += 1
                    continue

                try:
                    if entry["type"] == "sale":
                        entry["item"] = re.sub(r"\s*\|\s*Epak\s*$", "", entry["item"], flags=re.IGNORECASE)
                        profession, category = classify_vendor_and_item(entry["type"], entry["vendor"], entry["item"])
                        row_id = insert_sale(conn, sale_date=entry["sale_date"], vendor=entry["vendor"], item=entry["item"],
                                            customer=entry["customer"], amount=entry["amount"],
                                            profession=profession, category=category)
                        upsert_mail_ingest(conn, mail_id=mail_id, file_path=file_path, file_mtime=mtime, sale_id=row_id)
                    else:
                        profession, category = classify_vendor_and_item(entry["type"], entry["vendor"], entry["item"])
                        row_id = insert_purchase(conn, sale_date=entry["sale_date"], item=entry["item"], vendor=entry["vendor"],
                                                 amount=entry["amount"], category=category)
                        upsert_mail_ingest(conn, mail_id=mail_id, file_path=file_path, file_mtime=mtime, purchase_id=row_id)
                    inserted += 1
                    print(f"  ✔ Ingested from {pth.name}")
                except sqlite3.IntegrityError as e:
                    failed += 1
                    print(f"  ✖ Failed to ingest from {pth.name}: {e}")
                except Exception as e:
                    failed += 1
                    print(f"  ✖ Failed to ingest from {pth.name}: {e}")

            print(f"[DONE] Inserted: {inserted}, Skipped: {skipped}, Failed: {failed}")

    # Recommendations
    if args.recommend:
        run_recommendations(conn, days=args.days, min_sales=args.min_sales, top=args.top,
                            professions=args.profession, categories=args.category)

    if not args.path and not args.recommend:
        print("Nothing to do. Provide a PATH to parse or use --recommend to see suggestions.")
        print("Examples:")
        print("  python swg_merchant.py C:/SWGInfinity2/profiles/Epak/Inbox")
        print("  python swg_merchant.py --recommend --days 30 --min-sales 2 --top 20")
        print("  python swg_merchant.py --recommend --profession Doctor --category Buff --days 60")
        return

if __name__ == "__main__":
    main()
