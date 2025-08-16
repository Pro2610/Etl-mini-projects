import pandas as pd
from pathlib import Path
import sqlite3

def main():
    # Path to Excel file
    excel_file = Path("03_excel_to_postgres/data/sample.xlsx")
    db_file = Path("03_excel_to_postgres/data/orders.sqlite")

    if not excel_file.exists():
        print("❌ No Excel file found in data/. Please add one.")
        return

    # Read Excel
    df = pd.read_excel(excel_file)

    print("✅ Excel file loaded successfully!")
    print(df.head())

    # Save to SQLite
    conn = sqlite3.connect(db_file)
    df.to_sql("orders", conn, if_exists="replace", index=False)
    conn.close()

    print(f"✅ Data saved to SQLite database: {db_file}")

if __name__ == "__main__":
    main()
