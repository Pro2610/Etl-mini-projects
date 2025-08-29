from pathlib import Path
import pandas as pd
import duckdb
import datetime as dt

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint

# Джерело warehouse (візьми 07, якщо є, інакше 06)
DB_07 = Path(r"07_orchestration_prefect\warehouse.duckdb")
DB_06 = Path(r"06_merge_csv_api_duckdb\warehouse.duckdb")
DB_PATH = str(DB_07 if DB_07.exists() else DB_06)

GX_ROOT = Path("09_data_quality_gx/gx")

def load_tables():
    con = duckdb.connect(DB_PATH)
    fact = con.execute("SELECT user_id, amount_eur, date FROM fact_transactions").fetchdf()
    dim  = con.execute("SELECT symbol, rate_to_eur FROM dim_currency").fetchdf()
    con.close()
    return fact, dim

def build_expectations_for_fact(df: pd.DataFrame) -> ge.dataset.PandasDataset:
    gdf = ge.from_pandas(df.copy())
    # not nulls
    gdf.expect_column_values_to_not_be_null("user_id")
    gdf.expect_column_values_to_not_be_null("amount_eur")
    gdf.expect_column_values_to_not_be_null("date")
    # types / ranges
    gdf.expect_column_values_to_be_in_type_list("user_id", ["int64", "int32", "int"])
    gdf.expect_column_values_to_be_between("amount_eur", min_value=0, strictly=False)
    # date not in future
    today = dt.date.today()
    gdf.expect_column_values_to_be_between("date", max_value=str(today), parse_strings_as_datetimes=True)
    return gdf

def build_expectations_for_dim(df: pd.DataFrame) -> ge.dataset.PandasDataset:
    gdf = ge.from_pandas(df.copy())
    gdf.expect_column_values_to_not_be_null("symbol")
    gdf.expect_column_values_to_not_be_null("rate_to_eur")
    gdf.expect_column_values_to_be_between("rate_to_eur", min_value=0, strictly=True)
    # whitelist (за потреби розширюй)
    allowed = ["EUR", "USD", "GBP", "UAH", "JPY", "PLN", "CHF", "BRL"]
    gdf.expect_column_values_to_be_in_set("symbol", allowed)
    # унікальність символів
    gdf.expect_column_values_to_be_unique("symbol")
    return gdf

def run_validation(name: str, gdf: ge.dataset.PandasDataset) -> dict:
    context = ge.get_context(context_root_dir=str(GX_ROOT))
    # Runtime suite (без запису yaml, просто одноразово)
    suite_name = f"{name}_suite"
    try:
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)
    except Exception:
        suite = context.get_expectation_suite(suite_name)

    # Записати expectations з об’єкта gdf до suite
    suite.expectations = gdf.get_expectation_suite(discard_failed_expectations=False).expectations
    context.add_or_update_expectation_suite(expectation_suite=suite)

    # Runtime batch
    batch_request = RuntimeBatchRequest(
        datasource_name="runtime_pandas",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=f"{name}_asset",
        runtime_parameters={"batch_data": gdf},
        batch_identifiers={"default_identifier_name": "default_id"}
    )

    # Створимо простий checkpoint
    checkpoint = SimpleCheckpoint(
        name=f"{name}_checkpoint",
        data_context=context,
        validations=[{"batch_request": batch_request, "expectation_suite_name": suite_name}],
    )
    result = checkpoint.run()
    return result.to_json_dict()

def main():
    fact_df, dim_df = load_tables()
    fact_ge = build_expectations_for_fact(fact_df)
    dim_ge  = build_expectations_for_dim(dim_df)

    fact_res = run_validation("fact_transactions", fact_ge)
    dim_res  = run_validation("dim_currency", dim_ge)

    # Коротке зведення
    def ok(res): 
        return res["success"]

    print("✅ fact_transactions:", "PASS" if ok(fact_res) else "FAIL")
    print("✅ dim_currency:", "PASS" if ok(dim_res) else "FAIL")

if __name__ == "__main__":
    main()
