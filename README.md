# ETL Mini-Projects

A collection of small ETL (Extract, Transform, Load) pipelines to showcase practical data engineering and analytics workflows.
Each mini-project demonstrates different data sources, formats, and transformation techniques.

## Projects

1. **CSV Folder to SQLite**  
   Batch ingestion of multiple monthly CSV files, data cleaning, data quality checks, and loading into SQLite.
   - Folder: `01_csv_folder_to_sqlite`
   - Skills: Batch ingestion, schema enforcement, basic DQ

2. **API to Parquet Daily**  
   Fetches data from a public API daily, stores raw JSON, transforms to Parquet, and partitions by date.
   - Folder: `02_api_to_parquet_daily`
   - Skills: Incremental loads, scheduling, retry logic

3. **Excel Cleanup & Merge**  
   Combines and cleans multiple Excel files into a single dataset.
   - Folder: `03_excel_cleanup_merge`
   - Skills: Data wrangling, unpivot, deduplication

## How to Run
```bash
pip install -r 00_common/requirements.txt
python <project-folder>/flows/flow.py

Tech Stack

Python (Pandas, PyArrow)

Prefect 2

SQLite

Pytest
