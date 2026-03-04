# distress_reportaopration.py
import requests
import pandas as pd

# --- Step 1: Download the Excel file from SharePoint ---
shared_link = "https://planeteyefarma.sharepoint.com/:x:/g/EUx-2HCGZ2FOrjfu0f5Y8p8Bae4QQJ9Rz7Revq1rXMU10w?e=h7OrTk"
download_url = shared_link + "&download=1"
output_excel = "Distress_report.xlsx"

print("📥 Downloading Excel file...")
resp = requests.get(download_url)
resp.raise_for_status()

content_type = resp.headers.get("Content-Type", "")
if "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" not in content_type:
    raise ValueError(f"Expected an XLSX file, got Content-Type={content_type!r}")

with open(output_excel, "wb") as f:
    f.write(resp.content)

print(f"✅ Downloaded Excel file as '{output_excel}'")

# --- Step 2: Common cleaning function ---
def clean_sheet(df: pd.DataFrame) -> pd.DataFrame:
    # Clean column names
    df.columns = df.columns.str.strip()
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    # Fix typo
    if 'Chianage End' in df.columns and 'Chainage End' not in df.columns:
        df.rename(columns={'Chianage End': 'Chainage End'}, inplace=True)

    if 'Chianage End' in df.columns and 'Chainage End' in df.columns:
        df.drop(columns=['Chianage End'], inplace=True)

    # Date conversion
    def convert_date(date_str):
        date_obj = pd.to_datetime(date_str, dayfirst=True, errors='coerce')
        if pd.notna(date_obj) and date_obj.year == 1900:
            date_obj = date_obj.replace(year=2025)
        return date_obj.strftime('%d-%m-%Y') if pd.notna(date_obj) else pd.NaT

    df['Date'] = df['Date'].apply(convert_date)

    # Drop rows missing essentials
    essential_cols = [
        'Latitude', 'Longitude', 'Project Name',
        'Chainage Start', 'Chainage End',
        'Date', 'Lane', 'Carriage Type'
    ]
    df = df.dropna(subset=[c for c in essential_cols if c in df.columns])

    # Numeric columns (includes Scaling if present)
    numeric_columns = [
        "Chainage Start", "Chainage End", "Pothole", "Alligator crack",
        "Oblique crack", "Edge Break", "Patching", "Bleeding",
        "Hotspots", "Rutting", "Raveling", "Roughness", "Repair",
        "Block crack", "Longitudinal crack", "Total Distress",
        "Length", "Area", "Depth", "Width",
        "Single discrete crack", "Multiple cracks",
        "Joint crack", "Joint seal defects", "Punchout",
        "Slippage", "Heaves", "Simple crack",
        "Transverse crack", "Hairline crack",
        "Hungry Surface", "Settlement", "Shoving", "Stripping",
        "Scaling"  # Only exists in Sheet2
    ]

    numeric_columns = [c for c in numeric_columns if c in df.columns]
    df[numeric_columns] = (
        df[numeric_columns]
        .apply(pd.to_numeric, errors='coerce')
        .fillna(0)
        .astype('float32')
    )

    return df


# --- Step 3: Read Excel and process both sheets ---
print("🧹 Cleaning and preprocessing datasets...")

xls = pd.ExcelFile(output_excel)
required_sheets = ["Sheet1", "Sheet2"]

for sheet in required_sheets:
    if sheet not in xls.sheet_names:
        raise ValueError(f"Expected sheet '{sheet}' not found. Available sheets: {xls.sheet_names}")

# Sheet1
df1 = pd.read_excel(xls, sheet_name="Sheet1")
df1 = clean_sheet(df1)
df1.to_csv("Cleaned_Distress_report_road_dataset.csv", index=False)
print("✅ Sheet1 cleaned and saved")

# Sheet2
df2 = pd.read_excel(xls, sheet_name="Sheet2")
df2 = clean_sheet(df2)
df2.to_csv("Cleaned_Distress_report_road_dataset2.csv", index=False)
print("✅ Sheet2 cleaned and saved")

# --- Step 4: Preview ---
preview_cols = ['Project Name', 'Chainage Start', 'Chainage End', 'Distress Type', 'Total Distress', 'Date']

print("\n📊 Preview Sheet1:")
print(df1[[c for c in preview_cols if c in df1.columns]].head())

print("\n📊 Preview Sheet2:")
print(df2[[c for c in preview_cols if c in df2.columns]].head())
