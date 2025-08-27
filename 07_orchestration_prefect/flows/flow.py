from __future__ import annotations
from pathlib import Path
import json
import pandas as pd
import duckdb
from prefect import flow, task, get_run_logger

# ---------- ПАРАМЕТРИ ЗА ЗАМОВЧУВАННЯМ (можна змінювати під себе)
CSV_PATH = Path("06_merge_csv_api_duckdb/data_raw/transactions.csv")
RATES_PATH = Path("06_merge_csv_api_duckdb/data_raw/rates.json")
DB_PATH = Path("07_orchestration_prefect/warehouse.duckdb")

# ---------- TASKS

@task(retries=2, retry_delay_seconds=5)
def load_inputs(csv_path: Path, rates_path: Path) -> tuple[pd.DataFrame, dict]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing CSV: {csv_path}")
    if not rates_path.exists():
        raise FileNotFoundError(f"Missing JSON: {rates_path}")

    tx = pd.read_csv(csv_path)
    tx["date"] = pd.to_datetime(tx["date"]).dt.date
    tx["currency"] = tx["currency"].str.upper()

    with open(rates_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    base = payload.get("base", "EUR")
    rates = payload.get("rates") or {}
    if base != "EUR":
        raise ValueError("This demo expects base=EUR in rates.json")
    if not rates:
        raise ValueError("No rates found in rates.json")

    return tx, rates

@task
def transform_to_eur(tx: pd.DataFrame, rates: dict) -> pd.DataFrame:
    missing = sorted(set(tx["currency"]) - set(rates.keys()))
    if missing:
        raise ValueError(f"Missing rates for currencies: {missing}")

    # base=EUR → rates[currency] = units of currency per EUR
    # amount_eur = amount / rate
    tx = tx.copy()
    tx["amount_eur"] = tx.apply(
        lambda r: float(r["amount"]) / float(rates[r["currency"]]),
        axis=1
    )
    out = tx[["user_id", "amount_eur", "date"]].copy()
    out["amount_eur"] = out["amount_eur"].round(2)
    return out

@task
def load_to_duckdb(fact_df: pd.DataFrame, rates: dict, db_path: Path) -> tuple[int, int]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))

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

    # оновлюємо довідник валют
    rows = []
    for sym, rate in rates.items():
        if float(rate) == 0.0:
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

    fact_cnt = con.execute("SELECT COUNT(*) FROM fact_transactions;").fetchone()[0]
    dim_cnt  = con.execute("SELECT COUNT(*) FROM dim_currency;").fetchone()[0]
    con.close()
    return fact_cnt, dim_cnt

# ---------- FLOW

@flow(name="orchestrate-csv-json-to-duckdb")
def orchestrate_tx_to_duckdb(
    csv_path: str = str(CSV_PATH),
    rates_path: str = str(RATES_PATH),
    db_path: str = str(DB_PATH),
):
    logger = get_run_logger()
    logger.info("Starting Project 07 flow…")

    tx, rates = load_inputs(Path(csv_path), Path(rates_path))
    logger.info(f"Loaded inputs: rows={len(tx)}, rates={len(rates)}")

    fact_df = transform_to_eur(tx, rates)
    logger.info(f"Transformed records: {len(fact_df)}")

    fact_cnt, dim_cnt = load_to_duckdb(fact_df, rates, Path(db_path))
    logger.info(f"Loaded to DuckDB: facts={fact_cnt}, dim={dim_cnt} → {db_path}")
    logger.info("Flow finished successfully.")

if __name__ == "__main__":
    orchestrate_tx_to_duckdb()
