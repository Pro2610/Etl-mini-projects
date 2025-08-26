from pathlib import Path
import json
import pandas as pd
import duckdb

CSV_PATH = Path("06_merge_csv_api_duckdb/data_raw/transactions.csv")
RATES_PATH = Path("06_merge_csv_api_duckdb/data_raw/rates.json")
DB_PATH = Path("06_merge_csv_api_duckdb/warehouse.duckdb")

def load_inputs():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Missing CSV: {CSV_PATH}")
    if not RATES_PATH.exists():
        raise FileNotFoundError(f"Missing JSON: {RATES_PATH}")

    tx = pd.read_csv(CSV_PATH)
    tx["date"] = pd.to_datetime(tx["date"]).dt.date
    tx["currency"] = tx["currency"].str.upper()

    with open(RATES_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)

    base = payload.get("base", "EUR")
    rates = payload.get("rates") or {}
    if base != "EUR":
        raise ValueError("This demo expects base=EUR in rates.json")
    if not rates:
        raise ValueError("No rates found in rates.json")
    return tx, rates

def transform_to_eur(tx: pd.DataFrame, rates: dict) -> pd.DataFrame:
    # перевіримо, чи всі валюти покриті
    missing = sorted(set(tx["currency"]) - set(rates.keys()))
    if missing:
        raise ValueError(f"Missing rates for currencies: {missing}")

    # amount_eur = amount / rate (бо base=EUR → rates[currency] = currency_per_EUR)
    tx["amount_eur"] = tx.apply(lambda r: float(r["amount"]) / float(rates[r["currency"]]), axis=1)
    out = tx[["user_id", "amount_eur", "date"]].copy()
    out["amount_eur"] = out["amount_eur"].round(2)
    return out

def upsert_warehouse(fact_df: pd.DataFrame, rates: dict):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_currency (
        symbol TEXT PRIMARY KEY,
        rate_to_eur DOUBLE
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_transactions (
        user_id INTEGER,
        amount_eur DOUBLE,
        date DATE
    );
    """)

    # оновимо dim_currency (rate_to_eur — множник для конвертації currency→EUR)
    # тут rate_to_eur = 1 / rates[currency per EUR]  (для EUR → 1.0)
    rows = []
    for sym, rate in rates.items():
        if rate == 0:
            continue
        rate_to_eur = 1.0 / float(rate) if sym != "EUR" else 1.0
        rows.append((sym, rate_to_eur))
    dim_df = pd.DataFrame(rows, columns=["symbol", "rate_to_eur"])

    con.execute("DELETE FROM dim_currency;")
    con.register("dim_df", dim_df)
    con.execute("INSERT INTO dim_currency SELECT * FROM dim_df;")
    con.unregister("dim_df")

    # перезапис фактів для простоти
    con.execute("DELETE FROM fact_transactions;")
    con.register("facts", fact_df)
    con.execute("INSERT INTO fact_transactions SELECT * FROM facts;")
    con.unregister("facts")

    cnt = con.execute("SELECT COUNT(*) FROM fact_transactions;").fetchone()[0]
    cur_cnt = con.execute("SELECT COUNT(*) FROM dim_currency;").fetchone()[0]
    print(f"✅ Loaded: fact_transactions={cnt}, dim_currency={cur_cnt}")
    con.close()

def main():
    tx, rates = load_inputs()
    fact_df = transform_to_eur(tx, rates)
    upsert_warehouse(fact_df, rates)

if __name__ == "__main__":
    main()
