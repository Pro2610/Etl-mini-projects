from pathlib import Path
import duckdb
import pandas as pd

# Джерело: вибери те, що актуальніше
DB_PATH_06 = r"06_merge_csv_api_duckdb\warehouse.duckdb"
DB_PATH_07 = r"07_orchestration_prefect\warehouse.duckdb"

# якщо 07 існує — беремо його; інакше 06
DB_PATH = DB_PATH_07 if Path(DB_PATH_07).exists() else DB_PATH_06

OUT_DIR = Path("08_dashboard/data_export")
OUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(DB_PATH)

# 1) Факти + валюта (на випадок, якщо схему змінювали)
fact = con.execute("SELECT user_id, amount_eur, date FROM fact_transactions").fetchdf()
dim  = con.execute("SELECT symbol, rate_to_eur FROM dim_currency").fetchdf()

# 2) Daily revenue (агрегат)
daily = con.execute("""
    SELECT date, SUM(amount_eur) AS revenue_eur
    FROM fact_transactions
    GROUP BY date
    ORDER BY date
""").fetchdf()

# 3) Збереження в CSV (можна додати .parquet, якщо хочеш)
fact.to_csv(OUT_DIR / "fact_transactions.csv", index=False)
dim.to_csv(OUT_DIR / "dim_currency.csv", index=False)
daily.to_csv(OUT_DIR / "daily_revenue.csv", index=False)

print("✅ Exported:",
      OUT_DIR / "fact_transactions.csv",
      OUT_DIR / "dim_currency.csv",
      OUT_DIR / "daily_revenue.csv", sep="\n")
con.close()
