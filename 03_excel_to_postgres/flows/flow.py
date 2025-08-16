import pandas as pd
from pathlib import Path

def main():
    # Path to Excel file
    excel_file = Path("03_excel_to_postgres/data/sample.xlsx")

    if not excel_file.exists():
        print("❌ No Excel file found in data/. Please add one.")
        return

    # Read Excel
    df = pd.read_excel(excel_file)

    print("✅ Excel file loaded successfully!")
    print(df.head())

if __name__ == "__main__":
    main()
