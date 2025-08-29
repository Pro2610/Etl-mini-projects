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

### 06 — Merge CSV + JSON → DuckDB (EUR)
- **Goal:** Merge transactions from CSV with FX rates (JSON, base=EUR), convert all amounts into EUR, and load into a DuckDB warehouse.  
- **Tools:** Python, Pandas, DuckDB, Parquet  
- **Schema:**  
  - `dim_currency(symbol, rate_to_eur)`  
  - `fact_transactions(user_id, amount_eur, date)`  
- **Result:**  
  - Normalized transactions in `warehouse.duckdb`  
  - Example queries in `sql/queries.sql` (total revenue, revenue by user, daily trend)  
- **Run:** `python 06_merge_csv_api_duckdb/flows/flow.py`

---

### 07 — Orchestration & Scheduling (Prefect)
- **Goal:** Wrap CSV+JSON → DuckDB pipeline in a Prefect 2 flow with retries, logging, and parameters.  
- **Tools:** Python, Prefect, DuckDB, Pandas  
- **Result:** `warehouse.duckdb` built via orchestrated flow; ready for scheduling.  
- **Run:** `python 07_orchestration_prefect/flows/flow.py`

---

### 08 — BI Dashboard (Power BI / Tableau)
- **Goal:** Build a simple analytics dashboard from curated exports of the DuckDB warehouse (Projects 06/07).  
- **Tools:** Power BI / Tableau, DuckDB, Python, CSV/Parquet  
- **Data Prep:** `python 08_dashboard/flows/export.py` → outputs in `data_export/`  
- **Visuals:** KPIs (Total Revenue, Users, Transactions), Daily Revenue (line), Revenue by User (bar)

---

### 09 — Data Quality (Basic Validator)
- **Goal:** Validate datasets before loading into the warehouse: check non-nulls, ranges, dates, and uniqueness.  
- **Tools:** Python, Pandas, DuckDB  
- **Checks:**  
  - `fact_transactions`: user_id not null/int, amount_eur ≥ 0, date not in future  
  - `dim_currency`: symbol unique, allowed values, rate_to_eur > 0  
- **Run:** `python 09_data_quality_basic/flows/validate.py`

---

## 📌 Next Steps
- Extend Project 03 to connect with a real Postgres database.  
- Automate daily API ingestion in Project 02.  
- Add visualization dashboards for processed datasets (Power BI/Tableau).  
- Explore advanced transformations and orchestration with Prefect.  
- Experiment with larger datasets and performance tuning in DuckDB.  
- Upgrade Data Quality with Great Expectations once compatible with Python 3.13.
