import requests
import pandas as pd
import io
from datetime import datetime

# --- Step 1: Download Excel file from SharePoint ---
shared_link = "https://planeteyefarma.sharepoint.com/:x:/g/EWYwKEzUsthAtzSXHdPYNrcB4EQTRA463SDDaXKZzi7-NA?rtime=PeqicxkV3kg"
download_url = shared_link + "&download=1"

output_excel = "TIS.xlsx"
output_csv_1 = "Cleaned_TIS_dataset.csv"
output_csv_2 = "Cleaned_TIS_dataset2.csv"

print("📥 Downloading TIS Excel file...")
resp = requests.get(download_url)
resp.raise_for_status()

content_type = resp.headers.get("Content-Type", "")
if "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" not in content_type:
    raise ValueError(f"Expected an XLSX file, got Content-Type={content_type!r}")

with open(output_excel, "wb") as f:
    f.write(resp.content)

print(f"✅ Downloaded Excel file as '{output_excel}'")

# --- Common cleaning function ---
def clean_tis_sheet(df: pd.DataFrame) -> pd.DataFrame:
    # Remove junk columns and whitespace
    df.columns = df.columns.str.strip()
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', case=False)]

    # Convert date format
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
        df = df[df["Date"].notna()]
        df["Date"] = df["Date"].dt.strftime('%d-%m-%Y')

    # Convert numeric columns
    numeric_columns = [
        "Chainage From", "Chainage To", "Length",
        "3-Wheeler/ Auto", "Car/ Jeep/ Van/ Taxi", "Standard Bus",
        "LCV", "2-Axle Trucks", "3-Axle Trucks", "MAV", "OSV",
        "Tractor", "Others", "AADT in Vehicles", "AADT in PCU",
        "CVD in Vehicles", "Longitude", "Latitude"
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    # Drop blank or incomplete rows
    essential_cols = [
        "Chainage From", "Chainage To", "Project Name",
        "Longitude", "Latitude", "Date"
    ]
    df = df.dropna(subset=[c for c in essential_cols if c in df.columns])

    return df


# --- Step 2: Load Excel in memory ---
excel_data = io.BytesIO(resp.content)

# --- Step 3: Process Sheet1 ---
print("🧹 Processing Sheet1...")
df1 = pd.read_excel(excel_data, sheet_name="Sheet1")
df1 = clean_tis_sheet(df1)
df1.to_csv(output_csv_1, index=False, encoding="utf-8-sig")
print(f"✅ Saved '{output_csv_1}'")

# --- Step 4: Process Sheet2 ---
print("🧹 Processing Sheet2...")
df2 = pd.read_excel(excel_data, sheet_name="Sheet2")
df2 = clean_tis_sheet(df2)
df2.to_csv(output_csv_2, index=False, encoding="utf-8-sig")
print(f"✅ Saved '{output_csv_2}'")

# --- Step 5: Preview (Sheet1) ---
preview_cols = [
    "Road Code", "Project Name", "District", "Block",
    "Chainage From", "Chainage To", "AADT in Vehicles",
    "AADT in PCU", "CVD in Vehicles", "Longitude", "Latitude", "Date"
]
preview_cols = [c for c in preview_cols if c in df1.columns]

print("\nPreview of cleaned TIS data (Sheet1):")
print(df1[preview_cols].head())

print(f"\nCompleted successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
