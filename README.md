# 🚀 ETL Mini-Projects

A collection of small ETL (Extract, Transform, Load) projects to practice data engineering and analytics workflows.

---

## 🧠 Skills & Tools

**Languages & Databases:**
- SQL (PostgreSQL, BigQuery, SQLite)
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

### 02 — API to Parquet Daily (in progress)
- **Goal:** Extract exchange rates data from an API and store it in raw JSON and processed Parquet files.  
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

## 📌 Next Steps
- Extend Project 03 to connect with a real Postgres database.  
- Automate daily API ingestion in Project 02.  
- Add visualization dashboards for processed datasets.  
