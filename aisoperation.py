import requests
import pandas as pd
import io
from datetime import datetime
import os

# --- Step 1: Download Excel from SharePoint ---
shared_link = "https://planeteyefarma.sharepoint.com/:x:/g/EZV5gggMkt1Hivyevsqt9RYB-48OJGt4CWuvVFq6UA0dKA?e=41XPfA&wdOrigin=TEAMS-MAGLEV.p2p_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1761627723776&web=1"
download_url = shared_link + "&download=1"

output_excel = "AIS.xlsx"
output_csv_1 = "Cleaned_AIS_dataset.csv"
output_csv_2 = "Cleaned_AIS_dataset2.csv"

print("📥 Downloading AIS Excel file...")
resp = requests.get(download_url)
resp.raise_for_status()

content_type = resp.headers.get("Content-Type", "")
if "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" not in content_type:
    raise ValueError(f"Expected an XLSX file, got Content-Type={content_type!r}")

with open(output_excel, "wb") as f:
    f.write(resp.content)
print(f"Downloaded as '{output_excel}'")

# --- Common cleaning function ---
def clean_ais_sheet(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', case=False)]

    # Handle date
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
        df = df[df["Date"].notna()]
        df["Date"] = df["Date"].dt.strftime('%d-%m-%Y')

    # Numeric columns
    numeric_columns = [
        "Chainage Start", "Chainage End",
        "Non-Injured Accident", "Minor Accident", "Major Accident", "Fatal Accident",
        "Total Accident", "Fatalities", "Minor Injury", "Major Injury",
        "Fatal Injury", "Total Injury",
        "Head-tail", "Head-on", "Overturning", "Skidding", "Sideswipe",
        "Pedestrian/Hit & Run", "Animals on road", "Mechanical Fault",
        "Vehicle lost control", "Overspeed", "Drunk & Drive",
        "Fault of driver", "Pedestrian Related",
        "Longitude", "Latitude", "Grievous Accident", "Grievous Injury"
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    # Drop incomplete rows
    essential_cols = [
        "Project Name", "Chainage Start", "Chainage End",
        "Latitude", "Longitude", "Date"
    ]
    df = df.dropna(subset=[c for c in essential_cols if c in df.columns])

    return df


# --- Step 2: Load Excel in memory ---
excel_data = io.BytesIO(resp.content)

# --- Step 3: Process Sheet1 ---
print("🧹 Processing Sheet1...")
df1 = pd.read_excel(excel_data, sheet_name="Sheet1")
df1 = clean_ais_sheet(df1)
df1.to_csv(output_csv_1, index=False, encoding="utf-8-sig")
print(f"✅ Saved '{output_csv_1}'")

# --- Step 4: Process Sheet2 ---
print("🧹 Processing Sheet2...")
df2 = pd.read_excel(excel_data, sheet_name="Sheet2")
df2 = clean_ais_sheet(df2)
df2.to_csv(output_csv_2, index=False, encoding="utf-8-sig")
print(f"✅ Saved '{output_csv_2}'")

# --- Step 5: Preview (Sheet1) ---
preview_cols = [
    "Project Name", "Direction", "Chainage Start", "Chainage End",
    "Total Accident", "Fatalities", "Total Injury",
    "Longitude", "Latitude", "Date",
    "Nature of Accident", "Cause of Accident"
]
preview_cols = [c for c in preview_cols if c in df1.columns]

print("\n📊 Preview of cleaned AIS data (Sheet1):")
print(df1[preview_cols].head())

print(f"\nCompleted successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
