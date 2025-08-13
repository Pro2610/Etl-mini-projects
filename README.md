# ETL Mini-Projects

A collection of small **ETL pipelines** (Extract, Transform, Load) to showcase practical data engineering and analytics workflows.  
Each mini-project demonstrates different data sources, formats, transformations, and loading targets.

---

## 📂 Projects

### 1️⃣ CSV Folder to SQLite
**Folder:** [`01_csv_folder_to_sqlite`](01_csv_folder_to_sqlite)  
Batch ingestion of multiple monthly CSV files, data cleaning, data quality checks, and loading into SQLite.  
- **Tech stack:** Python, Pandas, Prefect 2, SQLite, Pytest
- **Skills:** Batch ingestion, schema enforcement, data quality validation, historical snapshots
- **Result:** Processed Parquet snapshots + populated `sales` table in SQLite

---

### 2️⃣ API to Parquet Daily *(Coming Soon)*
**Folder:** `02_api_to_parquet_daily`  
Daily incremental data extraction from a public API, stored as raw JSON and transformed into Parquet files partitioned by date.  
- **Tech stack:** Python, Pandas, Prefect 2, PyArrow
- **Skills:** Incremental extraction, API requests, scheduling, retries

---

### 3️⃣ Excel Cleanup & Merge *(Planned)*
**Folder:** `03_excel_cleanup_merge`  
Combining and cleaning multiple Excel files into a single dataset.  
- **Tech stack:** Python, Pandas, OpenPyXL
- **Skills:** Data wrangling, unpivoting, deduplication, type normalization

---

## ▶️ How to Run a Project

1. **Clone the repository**
```bash
git clone https://github.com/<your-username>/Etl-mini-projects.git
cd Etl-mini-projects
Navigate to the desired project folder and follow its README.md instructions.

Install dependencies

pip install -r 00_common/requirements.txt
Run the flow

python <project-folder>/flows/flow.py

🛠 Tech Stack

Python 3.10+

Pandas for data transformations

Prefect 2 for orchestration

SQLite for local storage

PyArrow for Parquet handling

Pytest for unit testing

📜 License

This project is licensed under the MIT License — see the LICENSE file for details.
