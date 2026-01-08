import pandas as pd

# Path to Episode Deep Dive Excel
excel_path = r"C:/Users/57948/OneDrive - Bain/Documents/Ep-DD-wideformat/exports/Episode_deep_dive.xlsx"

# Output CSV
output_csv = "episode_dd_all_sheet_columns.csv"

# Columns to exclude (case-insensitive)
EXCLUDE_COLUMNS = {

}

def extract_all_sheet_columns(excel_path):
    """
    Extracts column names from all sheets in an Excel file,
    excluding specified column names.
    Returns a DataFrame with: sheet_name, column_name
    """
    xl = pd.ExcelFile(excel_path)
    records = []

    for sheet in xl.sheet_names:
        df = xl.parse(sheet, nrows=0)  # read headers only

        for col in df.columns:
            # Exclude unwanted column names (case-insensitive)
            if col.strip().lower() in EXCLUDE_COLUMNS:
                continue

            records.append({
                "sheet_name": sheet,
                "column_name": col
            })

    return pd.DataFrame(records)


if __name__ == "__main__":
    columns_df = extract_all_sheet_columns(excel_path)

    # Save filtered output
    columns_df.to_csv(output_csv, index=False)

    print(f"✅ Column list extracted (filtered) for {columns_df['sheet_name'].nunique()} sheets")
    print(f"📄 Output file: {output_csv}")
