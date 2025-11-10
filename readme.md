# SWG Merchant Log Parser → SQLite

A Python 3 utility that parses Star Wars Galaxies `.mail` vendor transaction logs into a normalized SQLite database and generates both **data-driven vendor recommendations** and **sales analytics reports**.

---

## ✨ Features

- Parses both **sales** (`Vendor Sale Complete`) and **purchases** (`Vendor Item Purchased`) from `.mail` files.  
- Creates and maintains SQLite tables: `customers`, `sales`, `purchases`, and `mail_ingests`.  
- Prevents duplicate parsing and auto-reparses incomplete records.  
- Recursive scanning to fill missing information from prior logs.  
- Automatically classifies vendors and items by **profession** and **category**.  
- Supports **crafting and stocking recommendations** via the `--recommend` CLI.  
-- New filters: `--profession` and `--category` for focused analytics.  
- Trims trailing suffixes like `| Epak` from item names.  
- Includes a powerful **reporting engine (`report.py`)** for 12-month, year-to-date, or all-history summaries in Markdown and CSV.  

---

## 🧭 Usage

### Parse Mail Logs
```bash
# Parse all .mail files in a folder (recursive)
python swg_merchant.py "C:\SWGInfinity2\profiles\Epak\SWG Infinity"

# Parse a single .mail file
python swg_merchant.py "C:\SWGInfinity2\profiles\Epak\SWG Infinity\mail_Epak-Inc\250006499.mail"

# Use a custom database path
python swg_merchant.py /path/to/folder --db ./swg_merchant.db
```

---

### Generate Crafting Recommendations
The `--recommend` command analyzes recent sales and suggests what to restock or produce next.

```bash
# General recommendations for the last 30 days
python swg_merchant.py --recommend

# Focus on a specific profession
python swg_merchant.py --recommend --profession Doctor

# Combine multiple professions
python swg_merchant.py --recommend --profession "Bio-Engineer" Doctor

# Focus on certain item categories
python swg_merchant.py --recommend --category Buff PSG Vehicle

# Combine both filters for very specific insights
python swg_merchant.py --recommend --profession Doctor --category Buff "Buff Packs"

# Longer lookback and larger output
python swg_merchant.py --recommend --days 90 --top 30
```

#### 🔍 Recommendation Output Sections
1. **Top Items to Restock** — items sold frequently in the past `--days` days  
2. **Hottest Categories** — categories with the most recent volume and credits  
3. **Trending Categories** — month-over-month category growth or decline  

Each section automatically respects `--profession` and `--category` filters if provided.

---

## 📊 Generate Analytics Reports

The standalone **`report.py`** script produces rich Markdown or CSV summaries from the SQLite database.

### Run Examples

```bash
# Rolling 12-month report (default)
python report.py --db ./swg_merchant.db

# Year-to-date report (Jan 1 → today)
python report.py --db ./swg_merchant.db --ytd

# All historical data (no date filters)
python report.py --db ./swg_merchant.db --all

# Custom lookback window
python report.py --db ./swg_merchant.db --months 6

# Save report and CSV extracts
python report.py --db ./swg_merchant.db --out ./report.md --csv-dir ./report_csv
```

### 📄 Report Sections

| Section | Description |
|----------|--------------|
| **Summary KPIs** | Revenue, purchases, margin, sales counts, customer and vendor totals |
| **Monthly Sales Trend** | Month-by-month totals with MoM deltas |
| **Sales by Category** | Top categories by sales and revenue |
| **Sales by Profession** | Breakdown by vendor profession |
| **Top Items & Vendors** | Top 10 items and vendors by quantity and credits |
| **Purchases Overview** | Monthly purchase trends and vendor spend |
| **Category Margin** | Sales − Purchases per category |
| **Customer Summary** | Active, new, and lifetime-returning customers, repeat-purchase rate |
| **Top Customers** | Highest spenders and most frequent buyers |

Reports can be printed to the console, saved as Markdown, or exported as CSVs for use in spreadsheets or BI tools.

---

## 🗄️ Database Schema

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

---

## 🧮 Example Query

Total credits earned per year (sales + purchases):

```sql
SELECT strftime('%Y', sale_date) AS year, SUM(amount) AS total_credits
FROM sales
GROUP BY year
UNION ALL
SELECT strftime('%Y', sale_date) AS year, SUM(amount) AS total_credits
FROM purchases
GROUP BY year;
```

---

## ⚙️ Requirements

- Python ≥ 3.9  
- Standard library only (`sqlite3`, `argparse`, `pathlib`, `re`, `datetime`)  

---

## 📜 License

MIT License — free to modify, extend, and share.
