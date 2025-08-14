from __future__ import annotations
from pathlib import Path
from datetime import date
import json
import requests
import pandas as pd

from prefect import flow, task, get_run_logger

# ---- repo-root aware imports for common utils
import sys
ROOT = Path(__file__).resolve().parents[2]  # repo root
sys.path.append(str(ROOT / "00_common"))
from utils.io import write_parquet  # noqa

BASE_URL = "https://api.exchangerate.host"  # публічний без ключа
DEFAULT_BASE = "EUR"
SYMBOLS = ["USD", "GBP", "PLN", "UAH", "BRL", "JPY"]

@task(retries=3, retry_delay_seconds=5)
def fetch_rates(run_date: date, base: str = DEFAULT_BASE, symbols: list[str] = SYMBOLS) -> dict:
    """Забрати курси на конкретну дату (якщо підставити 'latest', візьме сьогодні)."""
    # Використаємо endpoint /latest (можна /{YYYY-MM-DD} для історії)
    url = f"{BASE_URL}/latest"
    params = {"base": base, "symbols": ",".join(symbols)}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    # Підстрахуємось: якщо в API інша дата, підставимо наш run_date
    data["run_date"] = run_date.isoformat()
    return data

@task
def save_raw_json(data: dict, raw_dir: Path) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    out = raw_dir / f"{data.get('run_date', 'unknown')}.json"
    with out.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return out

@task
def transform_to_parquet(raw_json_path: Path, silver_dir: Path) -> Path:
    with raw_json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # базові перевірки
    base = data.get("base")
    rates = data.get("rates") or {}
    run_date = data.get("run_date") or data.get("date")  # деякі API повертають 'date'

    if not base or not rates or not run_date:
        raise ValueError("Incomplete payload: base/rates/date missing")

    # перетворення у табличний вигляд
    rows = [{"date": run_date, "base": base, "symbol": k, "rate": v} for k, v in rates.items()]
    df = pd.DataFrame(rows).sort_values(["symbol"]).reset_index(drop=True)

    # запис parquet у партиції by date
    part_dir = silver_dir / f"date={run_date}"
    part_dir.mkdir(parents=True, exist_ok=True)
    out_path = part_dir / "rates.parquet"
    write_parquet(df, out_path)
    return out_path

@flow(name="api-to-parquet-daily")
def etl_api_to_parquet(run_today: bool = True, specific_date: str | None = None):
    """
    Якщо run_today=True — качає 'latest' і ставить сьогоднішню дату.
    Якщо specific_date задано (YYYY-MM-DD) — теж працює, але для простоти лишимо latest.
    """
    logger = get_run_logger()
    dl_root = Path("02_api_to_parquet_daily") / "data_lake"
    raw_dir = dl_root / "raw"
    silver_dir = dl_root / "silver"

    run_date = date.today() if run_today else date.fromisoformat(specific_date)  # простий варіант

    # EXTRACT
    payload = fetch_rates.submit(run_date).result()

    # RAW
    raw_path = save_raw_json.submit(payload, raw_dir).result()
    logger.info(f"Raw saved to {raw_path}")

    # TRANSFORM -> SILVER
    parquet_path = transform_to_parquet.submit(raw_path, silver_dir).result()
    logger.info(f"Parquet saved to {parquet_path}")

    logger.info("ETL finished successfully.")

if __name__ == "__main__":
    etl_api_to_parquet()
