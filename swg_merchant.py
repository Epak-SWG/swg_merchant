#!/usr/bin/env python3
"""
SWG Merchant Log Parser → SQLite
--------------------------------

Parses `.mail` files that contain vendor sale entries in this format:

393175791
SWG.SWG Infinity.auctioner
Vendor Sale Complete
TIMESTAMP: 1761901790
Vendor: World Drops has sold UltraCon Schematic to braylee for 1000000 credits.
The sale took place at Epak's Emporium, on Corellia.

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

CREATE INDEX IF NOT EXISTS idx_sales_date        ON sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_vendor      ON sales(vendor);
CREATE INDEX IF NOT EXISTS idx_sales_customer    ON sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_item        ON sales(item);
CREATE INDEX IF NOT EXISTS idx_sales_profession  ON sales(profession);
CREATE INDEX IF NOT EXISTS idx_sales_category    ON sales(category);
CREATE INDEX IF NOT EXISTS idx_customers_name    ON customers(name);
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

def insert_sale(conn: sqlite3.Connection, *, sale_date: str, vendor: str, item: str,
                customer: str, amount: int, profession: str | None = None,
                category: str | None = None) -> int:
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


# ---------- Parsing ----------

SALE_RE = re.compile(
    r"Vendor:\s*(?P<vendor>.+?)\s+has sold\s+(?P<item>.+?)\s+to\s+(?P<customer>.+?)\s+for\s+(?P<amount>\d+)\s+credits",
    re.IGNORECASE,
)
TS_RE = re.compile(r"TIMESTAMP:\s*(?P<ts>\d+)", re.IGNORECASE)


def _read_nonempty_lines(p: Path) -> list[str]:
    with p.open("r", encoding="utf-8", errors="ignore") as f:
        return [ln.strip() for ln in f if ln.strip()]


def parse_mail_file(file_path: Path) -> Optional[dict]:
    """
    Parse a single .mail file into a sale dict:
      { sale_date (ISO), vendor, item, customer, amount }
    Returns None if parsing fails.
    """
    try:
        lines = _read_nonempty_lines(file_path)
    except Exception as e:
        print(f"[WARN] Could not read {file_path}: {e}")
        return None

    if len(lines) < 6:
        print(f"[WARN] {file_path.name}: not enough lines to parse (got {len(lines)}).")
        return None

    # Expected fields by position based on your sample
    # 0: record_id
    # 1: source
    # 2: event
    # 3: TIMESTAMP: <unix>
    # 4: Vendor: <vendor> has sold <item> to <customer> for <amount> credits.
    # 5: The sale took place at <shop>, on <planet>.
    timestamp_line = lines[3]
    sale_line = lines[4]
    # location_line = lines[5]  # Parsed if you want extra validation; not needed for DB insert

    # Timestamp
    ts_m = TS_RE.search(timestamp_line)
    if not ts_m:
        print(f"[WARN] {file_path.name}: missing TIMESTAMP line.")
        return None
    try:
        ts = int(ts_m.group("ts"))
    except ValueError:
        print(f"[WARN] {file_path.name}: invalid TIMESTAMP value.")
        return None

    # Convert to ISO in UTC (consistent, sortable)
    sale_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Sale line
    sm = SALE_RE.search(sale_line)
    if not sm:
        print(f"[WARN] {file_path.name}: could not parse sale line:\n       {sale_line}")
        return None

    vendor = sm.group("vendor").strip()
    item = sm.group("item").strip()
    customer = sm.group("customer").strip()
    try:
        amount = int(sm.group("amount"))
    except ValueError:
        print(f"[WARN] {file_path.name}: amount is not an integer.")
        return None

    return {
        "sale_date": sale_date,
        "vendor": vendor,
        "item": item,
        "customer": customer,
        "amount": amount,
    }

def classify_vendor_and_item(vendor: str, item: str) -> tuple[str | None, str | None]:
    """
    Determine (profession, category) based on vendor and item name.

    Rules:
      Vendor name:
        "world drops"       -> category = "Loot"
        "weapons"           -> profession = "weaponsmith"
        "armor and vehicles"-> profession = "armorsmith"
        "pets"              -> profession = "Bio-Engineer"

      Item name (used if category not already set):
        contains "pistol"        -> category = "Pistol"
        contains "carbine"       -> category = "Carbine"
        contains "rifle"         -> category = "Rifle"
        contains "flame thrower" -> category = "Commando"
    """
    profession, category = None, None

    v = (vendor or "").casefold()
    i = (item or "").casefold()

    # --- item classifications ---
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
        elif "Long Vibro Axe" in i:
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
        elif "Vibro Knuckler" in i:
            category = "Unarmed"

    elif "world drops" in v:
        profession = "loot"
        if "[ca]" in i:
            category = "Tapes"
        elif "[aa]" in i:
            category = "Tapes"
        elif "crystal" in i:
            category = "Crystal"
        elif "Geonosian Power" in i:
            category = "Component"
        elif "holocron" in i:
            category = "Misc"
        elif "nightsister clothing" in i:
            category = "Schematic"
        elif "pearl" in i:
            category = "Pearl"
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
        elif "Long Vibro Axe" in i:
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
        elif "Vibro Knuckler" in i:
            category = "Unarmed"

    return profession, category



# ---------- CLI / Orchestration ----------

def iter_mail_paths(target: Path, recursive: bool) -> list[Path]:
    if target.is_file() and target.suffix.lower() == ".mail":
        return [target]
    if not target.is_dir():
        return []
    pattern = "**/*.mail" if recursive else "*.mail"
    return sorted(target.glob(pattern))


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

    print(f"[INFO] Found {len(mail_files)} .mail file(s). Parsing and inserting into {args.db} ...")

    for p in mail_files:
        sale = parse_mail_file(p)
        if not sale:
            failed += 1
            continue

        # Trim suffix from item name (e.g., " | Epak")
        sale["item"] = re.sub(r"\s*\|\s*Epak\s*$", "", sale["item"], flags=re.IGNORECASE)

        # Auto-classify based on vendor and item
        profession, category = classify_vendor_and_item(sale["vendor"], sale["item"])

        try:
            row_id = insert_sale(
                conn,
                sale_date=sale["sale_date"],
                vendor=sale["vendor"],
                item=sale["item"],
                customer=sale["customer"],
                amount=sale["amount"],
                profession=profession,
                category=category,
            )
            inserted += 1
            print(
                f"  ✔ Inserted id={row_id} :: {sale['vendor']} → {sale['customer']} | {sale['item']} | {profession or ''}/{category or ''}")
        except Exception as e:
            failed += 1
            print(f"  ✖ Failed to insert from {p.name}: {e}")


if __name__ == "__main__":
    main()
