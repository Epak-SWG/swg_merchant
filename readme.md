# SWG Merchant Log Parser → SQLite

A Python 3 utility that parses Star Wars Galaxies `.mail` vendor transaction logs into a normalized SQLite database.

## Features

- Parses both **sales** (`Vendor Sale Complete`) and **purchases** (`Vendor Item Purchased`) from `.mail` files.
- Creates and maintains SQLite tables: `customers`, `sales`, `purchases`, and `mail_ingests`.
- Prevents duplicate parsing and auto-reparses incomplete records.
- Supports recursive scanning with `--recursive`.
- Automatically classifies vendors and items by profession and category.
- Trims trailing suffixes like `| Epak` from item names.

## Usage

```bash
# Parse all .mail files in a folder (non-recursive)
python swg_merchant.py /path/to/folder

# Parse recursively
python swg_merchant.py /path/to/folder -r

# Parse a single .mail file
python swg_merchant.py /path/to/file.mail

# Use a custom database path
python swg_merchant.py /path/to/folder --db ./swg_merchant.db
```

## Database Schema

### customers
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| name | TEXT | Unique customer name |
| total_spent | INTEGER | Total credits spent |
| total_purchases | INTEGER | Number of purchases |

### sales
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| sale_date | TEXT | UTC timestamp |
| vendor | TEXT | Vendor name |
| item | TEXT | Item sold |
| customer_id | INTEGER | FK to customers |
| amount | INTEGER | Sale amount |
| profession | TEXT | Derived profession |
| category | TEXT | Derived item category |

### purchases
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| sale_date | TEXT | UTC timestamp |
| item | TEXT | Purchased item |
| vendor | TEXT | Vendor name |
| amount | INTEGER | Purchase amount |
| category | TEXT | Item category |

### mail_ingests
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| mail_id | TEXT | First non-empty line in file |
| file_path | TEXT | Absolute path (unique) |
| file_mtime | INTEGER | Last modified timestamp |
| inserted_at | TEXT | Auto timestamp |
| sale_id | INTEGER | FK to sales.id (nullable) |
| purchase_id | INTEGER | FK to purchases.id (nullable) |

## Example Query

```sql
SELECT strftime('%Y', sale_date) AS year, SUM(amount) AS total_credits
FROM sales
GROUP BY year
UNION ALL
SELECT strftime('%Y', sale_date) AS year, SUM(amount) AS total_credits
FROM purchases
GROUP BY year;
```

## Requirements

- Python ≥ 3.9
- Standard library only (`sqlite3`, `argparse`, `pathlib`, `re`, `datetime`)

## License

MIT License — feel free to modify and extend.
