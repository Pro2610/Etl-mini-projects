# Project 01 — CSV Folder to SQLite

## Goal
Load multiple monthly CSV files, clean and validate them, store processed data as Parquet snapshots, and load into SQLite for analysis.

## Dataset
Sample sales data (`data_raw/*.csv`), each file contains:
- `order_id`
- `product_id`
- `order_date`
- `quantity`
- `price`

## Steps
1. Read all CSV files from `data_raw/`.
2. Standardize column names and types.
3. Perform data quality checks (no nulls in keys, unique rows).
4. Save cleaned data as Parquet in `data_processed/`.
5. Load data into SQLite database.

## Run
```bash
pip install -r ../../00_common/requirements.txt
bash run_local.sh
