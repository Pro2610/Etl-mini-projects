from pathlib import Path
import json
import pandas as pd

from ..flows.flow import transform_to_parquet  # relative import for pytest

def test_transform_creates_parquet(tmp_path: Path):
    # штучний raw json
    payload = {
        "run_date": "2025-08-13",
        "base": "EUR",
        "rates": {"USD": 1.1, "GBP": 0.84}
    }
    raw = tmp_path / "raw.json"
    raw.write_text(json.dumps(payload), encoding="utf-8")
    silver_dir = tmp_path / "silver"

    out = transform_to_parquet.fn(raw, silver_dir)  # .fn щоб викликати тіло task
    assert out.exists()

    df = pd.read_parquet(out)
    assert set(df.columns) == {"date", "base", "symbol", "rate"}
    assert len(df) == 2
