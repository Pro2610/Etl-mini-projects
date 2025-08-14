# Project 02 — API to Parquet (Daily)

## Goal
Fetch daily FX rates from a public API, store raw JSON by date, and transform into Parquet partitioned by `date=YYYY-MM-DD`.

## Flow
1. **Extract**: GET `/latest` from exchangerate.host (base=EUR, selected symbols).
2. **Raw**: Save JSON to `data_lake/raw/YYYY-MM-DD.json`.
3. **Transform**: Normalize to columns: `date, base, symbol, rate`.
4. **Silver**: Save Parquet to `data_lake/silver/date=YYYY-MM-DD/rates.parquet`.

## How to Run
```bash
pip install -r ../../00_common/requirements.txt
python flows/flow.py
Notes
Retries are enabled for the API call.

Easy to schedule daily via Prefect deployments / OS scheduler later.

02_api_to_parquet_daily/data_lake/raw/2025-08-13.json

02_api_to_parquet_daily/data_lake/silver/date=2025-08-13/rates.parquet
