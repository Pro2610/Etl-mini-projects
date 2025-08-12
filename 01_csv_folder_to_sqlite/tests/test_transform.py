import pandas as pd
from flows.flow import transform

def test_transform_adds_revenue():
    df = pd.DataFrame({"quantity":[2,3], "price":[10.0, 5.0]})
    out = transform.fn(df)  # call task function directly
    assert "revenue" in out.columns
    assert list(out["revenue"]) == [20.0, 15.0]
