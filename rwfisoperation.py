import requests
import pandas as pd
import io
from datetime import datetime

# --- Step 1: Download Excel from SharePoint ---
shared_link = "https://planeteyefarma.sharepoint.com/:x:/g/EUC78TD-CSNDpzVIliGFpe4Bi4a5xAWSCKp00WCIQbbsjg?rtime=8OVdZ9sV3kg"
download_url = shared_link + "&download=1"

output_excel = "RWFIS.xlsx"
output_csv1 = "Cleaned_RWFIS_dataset.csv"
output_csv2 = "Cleaned_RWFIS_dataset2.csv"

print("📥 Downloading RWFIS Excel file...")
resp = requests.get(download_url)
resp.raise_for_status()

content_type = resp.headers.get("Content-Type", "")
if "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" not in content_type:
    raise ValueError(f"Expected an XLSX file, got Content-Type={content_type!r}")

with open(output_excel, "wb") as f:
    f.write(resp.content)
print(f"✅ Downloaded as '{output_excel}'")

# --- Step 2: Load Excel ---
excel_data = io.BytesIO(resp.content)
xls = pd.ExcelFile(excel_data)

def clean_rwfis_sheet(sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(xls, sheet_name=sheet_name)

    # Clean column names
    df.columns = df.columns.str.strip()
    df = df.loc[:, ~df.columns.str.contains("^Unnamed", case=False)]

    # Date handling
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
        df = df[df["Date"].notna()]
        df["Date"] = df["Date"].dt.strftime('%d-%m-%Y')

    # Numeric conversions
    numeric_columns = [
        "Length", "Chainage Start", "Chainage End", "Offset from Center Line",
        "Latitude", "Longitude", "Altitude"
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    # Drop incomplete rows
    essential_cols = [
        "Project  Name", "Chainage Start", "Chainage End",
        "Latitude", "Longitude", "Date"
    ]
    df = df.dropna(subset=[c for c in essential_cols if c in df.columns])

    return df

# =====================================================
# -------------------- SHEET 1 ------------------------
# =====================================================
print("🧹 Cleaning RWFIS Sheet1...")
df1 = clean_rwfis_sheet("Sheet1")
df1.to_csv(output_csv1, index=False, encoding="utf-8-sig")
print(f"✅ Sheet1 saved as '{output_csv1}'")

# =====================================================
# -------------------- SHEET 2 ------------------------
# =====================================================
print("🧹 Cleaning RWFIS Sheet2...")
df2 = clean_rwfis_sheet("Sheet2")
df2.to_csv(output_csv2, index=False, encoding="utf-8-sig")
print(f"✅ Sheet2 saved as '{output_csv2}'")

# --- Preview ---
preview_cols = [
    "District", "Block/Taluka", "Project  Name", "Direction",
    "Chainage Start", "Chainage End", "Feature", "Material Type",
    "Feature Condition ", "Safety Hazard (Y/N)", "Land Use",
    "Terrain", "Latitude", "Longitude", "Date"
]
preview_cols = [c for c in preview_cols if c in df1.columns]

print("\nPreview of cleaned RWFIS Sheet1 data:")
print(df1[preview_cols].head())

print(f"\nCompleted successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
