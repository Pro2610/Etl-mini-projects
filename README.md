# 🚀 ETL Mini-Projects

A collection of small ETL (Extract, Transform, Load) projects to practice data engineering and analytics workflows.

---

## 🧠 Skills & Tools

**Languages & Databases:**
- SQL (PostgreSQL, BigQuery, SQLite, DuckDB)
- Python (pandas, Jupyter)

**Data Tools:**
- Tableau, Power BI, Looker Studio, Google Analytics 4
- Google Sheets, Excel (Pivot Tables, Formulas, Charts)
- Power Query, Prefect

**Business Knowledge:**
- KPI reporting, E-commerce analytics, A/B Testing, Data cleaning

---

## 📂 Projects

### 01 — CSV Folder to SQLite
- **Goal:** Convert multiple CSV files into a single SQLite database.  
- **Tools:** Python, Pandas, SQLite, Prefect  
- **Result:**  
  - Database: `etl_demo.sqlite`  
  - Processed files: `silver_YYYYMMDD_HHMMSS.parquet`

---

### 02 — API to Parquet Daily (offline mode)
- **Goal:** Extract exchange rates data (or load sample JSON) and store it in raw JSON and processed Parquet files.  
- **Tools:** Python, Requests, Pandas, Prefect  
- **Result:**  
  - Raw data: `data_lake/raw/YYYY-MM-DD.json`  
  - Processed data: `data_lake/silver/date=YYYY-MM-DD/rates.parquet`  

---

### 03 — Excel to Postgres (SQLite demo)
- **Goal:** Load an Excel file into a SQL database.  
- **Tools:** Python, Pandas, SQLite, Prefect  
- **Result:**  
  - Excel source: `sample.xlsx`  
  - Database: `orders.sqlite` with table `orders`

---

### 04 — Parquet to DuckDB (Mini Warehouse)
- **Goal:** Load Parquet snapshots (from Project 02) into a local DuckDB “warehouse” with a simple star schema.  
- **Tools:** Python, DuckDB, Parquet  
- **Schema:**  
  - `dim_currency(symbol, currency_key)`  
  - `fact_rates(date, base, currency_key, rate)`  
- **Result:**  
  - Database: `warehouse.duckdb`  
  - Example queries in `sql/01_sample_queries.sql`

---

### 05 — JSON Logs to DuckDB (Product Analytics)
- **Goal:** Ingest JSON event logs, transform them into Parquet (silver), and load into a DuckDB warehouse for simple product analytics.  
- **Tools:** Python, Pandas, DuckDB, Parquet  
- **Result:**  
  - `fact_events` table in `warehouse.duckdb`  
  - Example queries in `sql/queries.sql` (DAU, event counts, retention, simple funnel)
- **Run:** `python 05_json_logs_to_duckdb/flows/flow.py`

---

## 📌 Next Steps
- Extend Project 03 to connect with a real Postgres database.  
- Automate daily API ingestion in Project 02.  
- Add visualization dashboards for processed datasets.  
- Explore advanced transformations and orchestration with Prefect.
