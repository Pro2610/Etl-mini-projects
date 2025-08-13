# Project 01 — CSV Folder to SQLite ETL

## 📌 Goal
This project demonstrates a simple **ETL pipeline** that:

1. Reads multiple monthly CSV files from a folder.
2. Cleans and validates the data.
3. Stores a processed snapshot as **Parquet**.
4. Loads the cleaned data into a **SQLite database**.

The goal is to show how to handle **batch ingestion**, enforce schema and data quality, and keep historical snapshots of processed data.

---

## 📂 Dataset
The input CSV files are located in `data_raw/` and contain:

- `order_id` — Unique order identifier
- `product_id` — Product code
- `order_date` — Date of the order
- `quantity` — Number of items sold
- `price` — Price per item

Example:
```csv
order_id,product_id,order_date,quantity,price

1,101,2024-01-05,2,10.5
2,102,2024-01-06,1,20.0
3,103,2024-01-07,3,5.0

⚙️ Steps in the Pipeline

Extract

Read all CSV files from the data_raw/ folder.

Merge them into a single DataFrame.

Transform

Standardize column names.

Convert order_date to datetime format.

Convert quantity and price to numeric.

Calculate a revenue column (quantity * price).

Run data quality checks:

No nulls in primary keys.

No duplicate rows.

Load

Save the processed dataset as a Parquet file in data_processed/ (timestamped snapshot).

Replace the table sales in the SQLite database located in db/etl_demo.sqlite.

▶️ How to Run

1. Install dependencies

pip install -r ../../00_common/requirements.txt

2. Run the ETL

python flows/flow.py

3. Expected Output

New Parquet file in data_processed/
Example: silver_20250813_084916.parquet

Updated SQLite database in db/etl_demo.sqlite containing the sales table.

🧪 Testing

To run the unit tests for the transformation step:

pytest tests/

💡 Skills Demonstrated

Batch CSV ingestion

Pandas transformations

Data quality validation

Historical data snapshots (Parquet)

SQLite database loading

Prefect 2 flow orchestration

Basic unit testing with Pytest

📈 Example Output in SQLite

order_id	  product_id	  order_date	  quantity  	price	     revenue
1             	101	        2024-01-05	     2	       10.5	      21.0
2	            102	      2024-01-06	     1	       20.0	      20.0
3	            103	      2024-01-07	     3	       5.0	      15.0

