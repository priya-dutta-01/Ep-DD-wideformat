# extract_excel_sheet_columns.py
#
# Extracts:
#   dashboard_name = Excel file name (without extension)
#   sheet_name     = each worksheet/tab name
#   column_name    = each column header in that sheet
#
# Output:
#   CSV with columns: dashboard_name, sheet_name, column_index, column_name
#
# Usage:
#   python extract_excel_sheet_columns.py --excel "path/to/Episode_deep_dive.xlsx" --out "excel_sheet_columns.csv"
#
import argparse
from pathlib import Path

import pandas as pd


def extract_excel_schema(excel_path: str) -> pd.DataFrame:
    excel_file = Path(excel_path)
    if not excel_file.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_file}")

    dashboard_name = excel_file.stem  # file name without extension

    xls = pd.ExcelFile(excel_file)  # loads sheet list once
    rows = []

    for sheet in xls.sheet_names:
        # Read only headers (0 rows of data) for speed
        try:
            header_df = pd.read_excel(excel_file, sheet_name=sheet, nrows=0)
        except Exception as e:
            # If a sheet is unreadable, still record it with a note
            rows.append(
                {
                    "dashboard_name": dashboard_name,
                    "sheet_name": sheet,
                    "column_index": None,
                    "column_name": f"[ERROR READING SHEET: {e}]",
                }
            )
            continue

        cols = list(header_df.columns)

        # Handle completely blank/no-header sheets
        if len(cols) == 0:
            rows.append(
                {
                    "dashboard_name": dashboard_name,
                    "sheet_name": sheet,
                    "column_index": None,
                    "column_name": "[NO COLUMNS FOUND]",
                }
            )
            continue

        for idx, col in enumerate(cols, start=1):
            # Use repr-like safe string conversion (helps with weird headers)
            col_str = "" if col is None else str(col)
            rows.append(
                {
                    "dashboard_name": dashboard_name,
                    "sheet_name": sheet,
                    "column_index": idx,
                    "column_name": col_str,
                }
            )

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", required=True, help="Path to the Excel file (.xlsx)")
    ap.add_argument("--out", default="excel_sheet_columns.csv", help="Output CSV path")
    args = ap.parse_args()

    df = extract_excel_schema(args.excel)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"Excel: {Path(args.excel).resolve()}")
    print(f"Rows written: {len(df)}")
    print(f"Saved -> {out_path.resolve()}")


if __name__ == "__main__":
    main()
