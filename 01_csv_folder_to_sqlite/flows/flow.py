from pathlib import Path
import pandas as pd
from prefect import flow, task, get_run_logger
from dotenv import dotenv_values
from pydantic import BaseModel
from datetime import datetime

from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[2]  # repo root
sys.path.append(str(ROOT / "00_common"))
from utils.io import read_csv_folder, write_parquet, to_sqlite  # noqa
from utils.validate import expect_non_null, expect_unique  # noqa

class Config(BaseModel):
    raw_folder: Path
    processed_folder: Path
    db_url: str
    table: str = "sales"

@task(retries=2, retry_delay_seconds=5)
def extract(cfg: Config) -> pd.DataFrame:
    df = read_csv_folder(cfg.raw_folder)
    return df

@task
def transform(df: pd.DataFrame) -> pd.DataFrame:
    # example transforms — tweak for your dataset
    # standardize columns
    cols = {c: c.strip().lower() for c in df.columns}
    df = df.rename(columns=cols)

    # parse dates
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    # enforce dtypes
    for col in ("quantity", "price"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # derived metrics
    if {"quantity", "price"}.issubset(df.columns):
        df["revenue"] = df["quantity"] * df["price"]

    # data quality
    key_cols = [c for c in ("order_id", "product_id") if c in df.columns]
    if key_cols:
        expect_non_null(df, key_cols)
        expect_unique(df, key_cols + (["order_date"] if "order_date" in df.columns else []))

    return df

@task
def load(df: pd.DataFrame, cfg: Config):
    # write silver parquet snapshot for lineage
    snapshot = cfg.processed_folder / f"silver_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
    write_parquet(df, snapshot)
    # upsert-ish (simple demo): replace table and reload (idempotent for small demo)
    to_sqlite(df, cfg.db_url, cfg.table, if_exists="replace")

@flow(name="csv-folder-to-sqlite")
def etl_csv_to_sqlite(
    raw_folder="01_csv_folder_to_sqlite/data_raw",
    processed_folder="01_csv_folder_to_sqlite/data_processed",
    table="sales"
):
    env = dotenv_values(ROOT / "00_common" / ".env")
    db_url = env.get("DATABASE_URL", "sqlite:///01_csv_folder_to_sqlite/db/etl_demo.sqlite")

    cfg = Config(
        raw_folder=Path(raw_folder),
        processed_folder=Path(processed_folder),
        db_url=db_url,
        table=table,
    )
    logger = get_run_logger()
    logger.info(f"Starting ETL with db={cfg.db_url}")
    df_raw = extract.submit(cfg).result()
    if df_raw.empty:
        logger.warning("No CSV files found. Nothing to process.")
        return
    df_t = transform.submit(df_raw).result()
    load.submit(df_t, cfg)
    logger.info(f"Done. Rows loaded: {len(df_t)}")

if __name__ == "__main__":
    etl_csv_to_sqlite()
