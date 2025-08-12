from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

def read_csv_folder(folder: Path) -> pd.DataFrame:
    files = sorted(folder.glob("*.csv"))
    dfs = [pd.read_csv(f) for f in files]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def write_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)

def to_sqlite(df: pd.DataFrame, db_url: str, table: str, if_exists: str = "append"):
    eng = create_engine(db_url)
    df.to_sql(table, eng, if_exists=if_exists, index=False)
