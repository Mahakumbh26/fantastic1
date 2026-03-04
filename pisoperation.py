# pis_report_operation.py
import requests
import pandas as pd
import io

# --- Step 1: Download the Excel file from SharePoint ---
shared_link = "https://planeteyefarma.sharepoint.com/:x:/g/EX1l7IJptWNMn8IEBMs8Eg4B42lbV44eCps0_wxrCOVurQ?rtime=fHwSiNYK3kg"
download_url = shared_link + "&download=1"
output_excel = "PIS.xlsx"

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
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', case=False)]

    # Fix typo
    if "Lagtitude" in df.columns:
        df.rename(columns={"Lagtitude": "Latitude"}, inplace=True)

    # Date conversion
    def convert_date(date_str):
        date_obj = pd.to_datetime(date_str, dayfirst=True, errors="coerce")
        if pd.notna(date_obj) and date_obj.year == 1900:
            date_obj = date_obj.replace(year=2025)
        return date_obj.strftime("%d-%m-%Y") if pd.notna(date_obj) else pd.NaT

    for col in ["Previous Date", "Next Date", "Month"]:
        if col in df.columns:
            df[col] = df[col].apply(convert_date)

    # Numeric columns
    numeric_columns = [
        "Actual Start Chainage", "Actual End Chainage", "Kilometer",
        "AADT", "MJB", "MNB", "Flyover", "PUP", "VUP", "ROB",
        "Length", "Overall Progress"
    ]
    numeric_columns = [c for c in numeric_columns if c in df.columns]

    df[numeric_columns] = (
        df[numeric_columns]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
        .astype("float32")
    )

    # Drop rows missing essentials
    essential_cols = [
        "Latitude", "Longitude", "Project Name",
        "Actual Start Chainage", "Actual End Chainage", "Month"
    ]
    df = df.dropna(subset=[c for c in essential_cols if c in df.columns])

    return df


# --- Step 3: Read Excel and process both sheets ---
print("🧹 Cleaning and preprocessing datasets...")

xls = pd.ExcelFile(output_excel)
required_sheets = ["Sheet1", "Sheet2"]

for sheet in required_sheets:
    if sheet not in xls.sheet_names:
        raise ValueError(
            f"Expected sheet '{sheet}' not found. Available sheets: {xls.sheet_names}"
        )

# Sheet1
df1 = pd.read_excel(xls, sheet_name="Sheet1")
df1 = clean_sheet(df1)
df1.to_csv("Cleaned_PIS_dataset.csv", index=False, encoding="utf-8-sig")
print("✅ Sheet1 cleaned and saved")

# Sheet2
df2 = pd.read_excel(xls, sheet_name="Sheet2")
df2 = clean_sheet(df2)
df2.to_csv("Cleaned_PIS_dataset2.csv", index=False, encoding="utf-8-sig")
print("✅ Sheet2 cleaned and saved")

# --- Step 4: Preview ---
preview_cols = [
    "Project Name", "State", "District",
    "Actual Start Chainage", "Actual End Chainage",
    "Overall Progress", "Carriage Type", "Lane", "Direction"
]

print("\n📊 Preview Sheet1:")
print(df1[[c for c in preview_cols if c in df1.columns]].head())

print("\n📊 Preview Sheet2:")
print(df2[[c for c in preview_cols if c in df2.columns]].head())
