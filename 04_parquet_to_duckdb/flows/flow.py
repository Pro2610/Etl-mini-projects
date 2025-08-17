from pathlib import Path
import duckdb
import glob

SILVER_GLOB = r"02_api_to_parquet_daily/data_lake/silver/date=*/rates.parquet"
DB_PATH = "04_parquet_to_duckdb/warehouse.duckdb"

def main():
    # знайдемо всі партиції Parquet
    files = sorted(glob.glob(SILVER_GLOB))
    if not files:
        print("❌ No Parquet snapshots found. Run Project 02 first.")
        return
    print("Found snapshots:", len(files))

    db_path = Path(DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB_PATH)

    # створимо таблиці (idempotent)
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_currency (
      currency_key INTEGER PRIMARY KEY,
      symbol TEXT UNIQUE
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_rates (
      date DATE,
      base TEXT,
      currency_key INTEGER,
      rate DOUBLE,
      FOREIGN KEY(currency_key) REFERENCES dim_currency(currency_key)
    );
    """)

    # підвантажимо всі parquet як один віртуальний набір
    # (DuckDB вміє читати напряму з Parquet)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW v_rates AS
        SELECT * FROM read_parquet('{SILVER_GLOB}');
    """)

    # заповнимо dim_currency (символ → surrogate key)
    con.execute("""
        INSERT OR IGNORE INTO dim_currency (currency_key, symbol)
        SELECT row_number() OVER (ORDER BY symbol) AS currency_key, symbol
        FROM (SELECT DISTINCT symbol FROM v_rates)
        ON CONFLICT(symbol) DO NOTHING;
    """)

    # створимо маппінг symbol → currency_key
    con.execute("CREATE OR REPLACE TEMP VIEW v_map AS SELECT currency_key, symbol FROM dim_currency;")

    # fact: приєднуємо ключі
    con.execute("DELETE FROM fact_rates;")
    con.execute("""
        INSERT INTO fact_rates (date, base, currency_key, rate)
        SELECT CAST(v.date AS DATE), v.base, m.currency_key, v.rate
        FROM v_rates v
        JOIN v_map m ON v.symbol = m.symbol;
    """)

    # перевірка
    cnt = con.execute("SELECT COUNT(*) FROM fact_rates;").fetchone()[0]
    cur_cnt = con.execute("SELECT COUNT(*) FROM dim_currency;").fetchone()[0]
    print(f"✅ Loaded: fact_rates rows={cnt}, dim_currency rows={cur_cnt}")
    con.close()

if __name__ == "__main__":
    main()
