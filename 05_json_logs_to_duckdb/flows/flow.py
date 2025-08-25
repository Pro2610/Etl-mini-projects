import pandas as pd
import json
from pathlib import Path
import duckdb

RAW_FILE = Path("05_json_logs_to_duckdb/data_raw/logs.json")
SILVER_DIR = Path("05_json_logs_to_duckdb/data_silver")
DB_PATH = "05_json_logs_to_duckdb/warehouse.duckdb"

def main():
    if not RAW_FILE.exists():
        print(f"❌ Raw file not found: {RAW_FILE}")
        return

    # Read JSON
    with open(RAW_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Save to Parquet (silver)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    parquet_file = SILVER_DIR / "events.parquet"
    df.to_parquet(parquet_file, index=False)
    print(f"✅ Silver saved: {parquet_file}")

    # Load into DuckDB
    con = duckdb.connect(DB_PATH)
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_events (
      user_id INTEGER,
      event TEXT,
      timestamp TIMESTAMP
    );
    """)

    con.execute("DELETE FROM fact_events;")  # overwrite
    con.execute(f"INSERT INTO fact_events SELECT * FROM read_parquet('{parquet_file}');")

    cnt = con.execute("SELECT COUNT(*) FROM fact_events;").fetchone()[0]
    print(f"✅ Loaded into DuckDB: {cnt} rows")
    con.close()

if __name__ == "__main__":
    main()
