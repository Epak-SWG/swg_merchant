#!/usr/bin/env python3
"""
SWG Merchant Log Parser → SQLite
--------------------------------

Parses `.mail` files that contain vendor sale entries in this format:

Usage:
  # Parse all .mail files in a folder (non-recursive)
  python swg_merchant.py /path/to/folder

  # Parse all .mail files recursively
  python swg_merchant.py /path/to/folder -r

  # Parse a single .mail file
  python swg_merchant.py /path/to/file.mail

  # Choose a custom DB path
  python swg_merchant.py /path/to/folder --db ./swg_merchant.db
"""

import re
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

# ---------- SQLite setup ----------

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

def ensure_db(db_path: Path = DEFAULT_DB) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA)
    return conn

def get_or_create_customer(conn: sqlite3.Connection, name: str) -> int:
    """Return existing customer ID or insert a new one."""
    cur = conn.execute("SELECT id FROM customers WHERE name = ?", (name,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur = conn.execute("INSERT INTO customers (name) VALUES (?)", (name,))
    conn.commit()
    return cur.lastrowid

def get_mail_identity(p: Path) -> tuple[Optional[str], int]:
    """
    Returns (mail_id, file_mtime).
    Assumes first non-empty line is the mail id (the numeric line at the top).
    """
    mail_id: Optional[str] = None
    with p.open("r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            s = ln.strip()
            if s:
                mail_id = s  # first non-empty line
                break
    mtime = int(p.stat().st_mtime)
    return mail_id, mtime

# ---------- Unified helpers (single-row design) ----------

def is_mail_processed(conn: sqlite3.Connection, *, mail_id: Optional[str], file_path: str) -> bool:
    """
    Returns True if we already have an ingest row for this file or mail_id.
    """
    if mail_id:
        cur = conn.execute("SELECT 1 FROM mail_ingests WHERE mail_id = ? LIMIT 1", (mail_id,))
        if cur.fetchone():
            return True
    cur = conn.execute("SELECT 1 FROM mail_ingests WHERE file_path = ? LIMIT 1", (file_path,))
    return cur.fetchone() is not None


def upsert_mail_ingest(
    conn: sqlite3.Connection,
    *,
    mail_id: Optional[str],
    file_path: str,
    file_mtime: int,
    sale_id: Optional[int] = None,
    purchase_id: Optional[int] = None,
) -> None:
    """
    Insert or update the single row for this mail.
    Exactly one of sale_id/purchase_id must be set (enforced by CHECK).
    """
    if (sale_id is None) == (purchase_id is None):
        raise ValueError("Provide exactly one of sale_id or purchase_id")

    # UPDATE first to keep idempotency
    cur = conn.execute(
        """
        UPDATE mail_ingests
           SET mail_id     = COALESCE(?, mail_id),
               file_mtime  = ?,
               sale_id     = COALESCE(?, sale_id),
               purchase_id = COALESCE(?, purchase_id),
               inserted_at = datetime('now')
         WHERE file_path = ?
        """,
        (mail_id, file_mtime, sale_id, purchase_id, file_path),
    )
    if cur.rowcount == 0:
        # INSERT if not present
        conn.execute(
            """
            INSERT INTO mail_ingests (mail_id, file_path, file_mtime, sale_id, purchase_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (mail_id, file_path, file_mtime, sale_id, purchase_id),
        )
    conn.commit()


def has_incomplete_rows_for_file(conn: sqlite3.Connection, *, file_path: str) -> bool:
    """
    Return True if the linked sales/purchases row has NULLs we plan to fill later.
    """
    # SALES with missing profession/category
    cur = conn.execute(
        """
        SELECT 1
          FROM mail_ingests mi
          JOIN sales s ON s.id = mi.sale_id
         WHERE mi.file_path = ?
           AND (s.profession IS NULL OR s.category IS NULL)
         LIMIT 1
        """,
        (file_path,),
    )
    if cur.fetchone():
        return True

    # PURCHASES with missing category
    cur = conn.execute(
        """
        SELECT 1
          FROM mail_ingests mi
          JOIN purchases p ON p.id = mi.purchase_id
         WHERE mi.file_path = ?
           AND (p.category IS NULL)
         LIMIT 1
        """,
        (file_path,),
    )
    return cur.fetchone() is not None


def purge_rows_for_file(conn: sqlite3.Connection, *, file_path: str) -> None:
    """
    Delete the sales/purchases row referenced by this mail, then remove the ingest row.
    """
    row = conn.execute(
        "SELECT sale_id, purchase_id FROM mail_ingests WHERE file_path = ?",
        (file_path,),
    ).fetchone()

    if row:
        sale_id, purchase_id = row
        if sale_id:
            conn.execute("DELETE FROM sales WHERE id = ?", (sale_id,))
        if purchase_id:
            conn.execute("DELETE FROM purchases WHERE id = ?", (purchase_id,))
        conn.execute("DELETE FROM mail_ingests WHERE file_path = ?", (file_path,))

    conn.commit()


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
    """
    Parse a single .mail file into a dict.
    Returns:
      {"type": "sale"|"purchase", "sale_date": ..., "vendor": ..., "item": ..., "customer": ..., "amount": ...}
    """
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

    sale_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Detect sale or purchase
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
        # Example: You have won the auction of "Geonosian Power Cube (Red)" from "BobCraft" for 40000 credits.  See the attached waypoint for location.
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
    """
    Determine (profession, category) based on vendor and item name.

    """
    profession, category = None, None

    v = (vendor or "").casefold()
    i = (item or "").casefold()
    # --- item classifications ---
    if entry_type == "sale":
        if "armor and vehicles" in v:
            profession = "Armorsmith"
            if "ab-1" in i:
                profession = "Artisan"
                category = "Vehicle"
            elif "basilisk war droid" in i:
                profession = "Artisan"
                category = "Vehicle"
            elif "bone" in i:
                category = "Bone Armor"
            elif "composite" in i:
                category = "Composite"
            elif "eta-1" in i:
                profession = "Artisan"
                category = "Vehicle"
            elif "flare s swoop" in i:
                profession = "Artisan"
                category = "Vehicle"
            elif "personal shield" in i:
                category = "PSG"
            elif "psg" in i:
                category = "PSG"
            elif "r.i.s." in i:
                category = "RIS"
            elif "tantel" in i:
                category = "Tantel"
            elif "xj-6" in i:
                profession = "Artisan"
                category = "Vehicle"

        elif "buffbot" in v:
            profession = "Doctor"
            category = "Buff"

        elif "chef" in v:
            profession = "Chef"
            category = "Chef"

        elif "pets" in v:
            profession = "Bio-Engineer"
            category = "Pet"
            if "egg" in i:
                category = "Incubation"

        elif "pharmaceuticals" in v:
            if "active" in i:
                profession = "Bio-Engineer"
                category = "BE Tissue"
            elif "buff" in i:
                profession = "Doctor"
                category = "Buff Packs"
            elif "coagulant" in i:
                profession = "Bio-Engineer"
                category = "BE Tissue"
            elif "cure" in i:
                profession = "Doctor"
                category = "Cure Packs"
            elif "enhance" in i:
                profession = "Doctor"
                category = "Buff Packs"
            elif "fear release" in i:
                profession = "Bio-Engineer"
                category = "BE Tissue"
            elif "hssiss" in i:
                profession = "Combat Medic"
                category = "Dart"
            elif "pet stimpack" in i:
                profession = "Bio-Engineer"
                category = "Stimpack"
            elif "scent neutralization" in i:
                profession = "Bio-Engineer"
                category = "BE Tissue"
            elif "small stimpack" in i:
                profession = "Doctor"
                category = "Stimpack"
            elif "tensile resistance" in i:
                profession = "Bio-Engineer"
                category = "BE Tissue"
            elif "vitality" in i:
                profession = "Bio-Engineer"
                category = "Stimpack"

        elif "resources" in v:
            profession = "Artisan"
            category = "Resources"

        elif "weapons" in v:
            profession = "Weaponsmith"
            if "carbine" in i:
                category = "Carbine"
            elif "two-handed curved sword" in i:
                category = "Two Hand"
            elif "curved sword" in i:
                category = "One Hand"
            elif "dl44 xt" in i:
                category = "Pistol"
            elif "flame thrower" in i or "flamethrower" in i:
                category = "Commando"
            elif "launcher pistol" in i:
                category = "Commando"
            elif "long vibro axe" in i:
                category = "Polearm"
            elif "nightsister energy lance" in i:
                category = "Polearm"
            elif "pistol" in i:
                category = "Pistol"
            elif "power hammer" in i:
                category = "Two Hand"
            elif "republic blaster" in i:
                category = "Pistol"
            elif "scout blaster" in i:
                category = "Pistol"
            elif "stun baton" in i:
                category = "One Hand"
            elif "rifle" in i:
                category = "Rifle"
            elif "vibro knuckler" in i:
                category = "Unarmed"

        elif "world drops" in v:
            profession = "Loot"
            if "[ca]" in i:
                category = "Tapes"
            elif "[aa]" in i:
                category = "Tapes"
            elif "crystal" in i:
                category = "Crystal"
            elif "geonosian power" in i:
                category = "Component"
            elif "holocron" in i:
                category = "Misc"
            elif "nightsister clothing" in i:
                category = "Schematic"
            elif "pearl" in i:
                category = "Crystal"
            elif "schematic" in i:
                category = "Schematics"
            elif "venom" in i:
                category = "Component"
            elif "treasure map" in i:
                category = "Treasure Map"

            ############# Looted Weapons #############
            elif "carbine" in i:
                category = "Carbine"
            elif "two-handed curved sword" in i:
                category = "Two Hand"
            elif "curved sword" in i:
                category = "One Hand"
            elif "dl44 xt" in i:
                category = "Pistol"
            elif "flame thrower" in i or "flamethrower" in i:
                category = "Commando"
            elif "launcher pistol" in i:
                category = "Commando"
            elif "long vibro axe" in i:
                category = "Polearm"
            elif "nightsister energy lance" in i:
                category = "Polearm"
            elif "pistol" in i:
                category = "Pistol"
            elif "power hammer" in i:
                category = "Two Hand"
            elif "republic blaster" in i:
                category = "Pistol"
            elif "scout blaster" in i:
                category = "Pistol"
            elif "stun baton" in i:
                category = "One Hand"
            elif "rifle" in i:
                category = "Rifle"
            elif "vibro knuckler" in i:
                category = "Unarmed"

    elif entry_type == "purchase":
        if "geonosian power cube" in i:
            category = "Component"

    return profession, category



# ---------- CLI / Orchestration ----------

def iter_mail_paths(target: Path, recursive: bool) -> list[Path]:
    if target.is_file() and target.suffix.lower() == ".mail":
        return [target]
    if not target.is_dir():
        return []
    pattern = "**/*.mail" if recursive else "*.mail"
    return sorted(target.glob(pattern))


def insert_purchase(conn: sqlite3.Connection, *, sale_date: str, item: str, vendor: str,
                    amount: int, category: Optional[str] = None) -> int:
    """Insert a purchase into the purchases table."""
    cur = conn.execute(
        """INSERT INTO purchases (sale_date, item, vendor, amount, category)
           VALUES (?, ?, ?, ?, ?)""",
        (sale_date, item, vendor, amount, category),
    )
    conn.commit()
    return cur.lastrowid

def insert_sale(conn: sqlite3.Connection, *, sale_date: str, vendor: str, item: str,
                customer: str, amount: int, profession: Optional[str] = None,
                category: Optional[str] = None) -> int:
    """Insert a sale linked to a customer."""
    cust_id = get_or_create_customer(conn, customer)

    cur = conn.execute(
        """INSERT INTO sales (sale_date, vendor, item, customer_id, amount, profession, category)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (sale_date, vendor, item, cust_id, amount, profession, category),
    )

    # Update customer aggregates
    conn.execute(
        """UPDATE customers
           SET total_spent = total_spent + ?, total_purchases = total_purchases + 1
           WHERE id = ?""",
        (amount, cust_id),
    )

    conn.commit()
    return cur.lastrowid


def main():
    import argparse

    ap = argparse.ArgumentParser(description="Parse SWG .mail vendor sales into SQLite.")
    ap.add_argument("path", help="Path to a .mail file or a folder containing .mail files")
    ap.add_argument("-r", "--recursive", action="store_true", help="Recurse into subfolders")
    ap.add_argument("--db", default=str(DEFAULT_DB), help="Path to SQLite DB (default: swg_merchant.db)")
    args = ap.parse_args()

    target = Path(args.path)
    mail_files = iter_mail_paths(target, args.recursive)

    if not mail_files:
        print(f"[INFO] No .mail files found at: {target}")
        return

    conn = ensure_db(Path(args.db))
    inserted = 0
    failed = 0
    skipped = 0  # <--- new counter

    print(f"[INFO] Found {len(mail_files)} .mail file(s). Parsing and inserting into {args.db} ...")

    for p in mail_files:
        file_path = str(p)
        mail_id, mtime = get_mail_identity(p)

        already = is_mail_processed(conn, mail_id=mail_id, file_path=file_path)
        incomplete = has_incomplete_rows_for_file(conn, file_path=file_path)

        # --- skip or reparse handling ---
        if already and not incomplete:
            skipped += 1
            continue

        if already and incomplete:
            print(f"[REPARSE] Incomplete data found; purging and reparsing: {p.name}")
            purge_rows_for_file(conn, file_path=file_path)

        # ---- parse and insert fresh -------------------------------------------
        entry = parse_mail_file(p)
        if not entry:
            failed += 1
            continue

        try:
            if entry["type"] == "sale":
                # trim trailing " | Epak"
                entry["item"] = re.sub(r"\s*\|\s*Epak\s*$", "", entry["item"], flags=re.IGNORECASE)
                profession, category = classify_vendor_and_item(
                    entry["type"], entry["vendor"], entry["item"]
                )
                row_id = insert_sale(
                    conn,
                    sale_date=entry["sale_date"],
                    vendor=entry["vendor"],
                    item=entry["item"],
                    customer=entry["customer"],
                    amount=entry["amount"],
                    profession=profession,
                    category=category,
                )
                upsert_mail_ingest(
                    conn,
                    mail_id=mail_id,
                    file_path=file_path,
                    file_mtime=mtime,
                    sale_id=row_id,
                )

            elif entry["type"] == "purchase":
                profession, category = classify_vendor_and_item(
                    entry["type"], entry["vendor"], entry["item"]
                )
                row_id = insert_purchase(
                    conn,
                    sale_date=entry["sale_date"],
                    item=entry["item"],
                    vendor=entry["vendor"],
                    amount=entry["amount"],
                    category=category,
                )
                upsert_mail_ingest(
                    conn,
                    mail_id=mail_id,
                    file_path=file_path,
                    file_mtime=mtime,
                    purchase_id=row_id,
                )

            inserted += 1
            print(f"  ✔ Ingested from {p.name}")

        except sqlite3.IntegrityError as e:
            failed += 1
            print(f"  ✖ Failed to ingest from {p.name}: {e}")
        except Exception as e:
            failed += 1
            print(f"  ✖ Failed to ingest from {p.name}: {e}")

    print(f"[DONE] Inserted: {inserted}, Skipped: {skipped}, Failed: {failed}")


if __name__ == "__main__":
    main()
