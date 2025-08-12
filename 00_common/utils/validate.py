import pandas as pd

def expect_non_null(df: pd.DataFrame, cols: list[str]):
    for c in cols:
        if df[c].isna().any():
            raise ValueError(f"Nulls found in required column: {c}")

def expect_unique(df: pd.DataFrame, cols: list[str]):
    if df.duplicated(subset=cols).any():
        raise ValueError(f"Duplicates found on key: {cols}")
