# Project 05 — JSON Logs → Parquet → DuckDB

## Goal
Ingest JSON event logs, normalize to Parquet (silver), and load into a DuckDB warehouse for simple product analytics.

## How it works
1) Read `data_raw/logs.json`  
2) Write `data_silver/events.parquet`  
3) Load into `warehouse.duckdb` (table: `fact_events`)  
4) Run SQL from `sql/queries.sql` for metrics (DAU, event counts, simple funnel)

## Run
```bash
python flows/flow.py
