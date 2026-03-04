import requests
import pandas as pd
import io
from datetime import datetime

# --- Step 1: Download Excel from SharePoint ---
shared_link = "https://planeteyefarma.sharepoint.com/:x:/g/EcbrAqyvqlVFp55ieytEbZoBMJgSi3euWfApJdwnDS9yIQ?e=pjIaGX&wdOrigin=TEAMS-MAGLEV.p2p_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1761636326367&web=1"   
download_url = shared_link + "&download=1"

output_excel = "BMS.xlsx"
output_csv = "Cleaned_BMS_dataset.csv"

print("📥 Downloading BMS Excel file...")
resp = requests.get(download_url)
resp.raise_for_status()

content_type = resp.headers.get("Content-Type", "")
if "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" not in content_type:
    raise ValueError(f"Expected an XLSX file, got Content-Type={content_type!r}")

with open(output_excel, "wb") as f:
    f.write(resp.content)
print(f"Downloaded Excel file as '{output_excel}'")

# --- Step 2: Load and clean Excel ---
excel_data = io.BytesIO(resp.content)
df = pd.read_excel(excel_data, sheet_name="Sheet2")
df.columns = df.columns.str.strip()
df = df.loc[:, ~df.columns.str.contains('^Unnamed', case=False)]

# --- Step 3: Clean date ---
if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df = df[df["Date"].notna()]
    df["Date"] = df["Date"].dt.strftime('%d-%m-%Y')

# --- Step 4: Convert numeric columns --- 
numeric_columns = [
    "Sr. No.", "No. of package", "Chainage Start", "Chainage End", "latitude", "longitude",
    "Departmental Chainge","Bridge No.","Pipe Culvert", "Box Culvert", "Chocked Culvert", 
    "Flyover", "FOB","MJB", "MNB", "PUP", "ROB", "Service Road Bridge", "Slab Culvert",
    "Utlility Crossing", "VUP"
]
numeric_columns = [c for c in numeric_columns if c in df.columns]

for col in numeric_columns:
    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

# --- Step 5: Drop incomplete or invalid rows ---
essential_cols = ["Project Name", "Chainage Start", "Chainage End", "latitude", "longitude", "Date"]
df = df.dropna(subset=[col for col in essential_cols if col in df.columns])

# --- Step 6: Save cleaned CSV ---
df.to_csv(output_csv, index=False, encoding="utf-8-sig")
print(f"Cleaned BMS data saved as '{output_csv}'")

# --- Step 7: Preview ---
preview_cols = [
    "Date", "Project Name", "Direction", "Type of structure ", "Chainage Start", "Chainage End",
    "latitude", "longitude"
]
preview_cols = [c for c in preview_cols if c in df.columns]

print("\n📊 Preview of cleaned BMS data:")
print(df[preview_cols].head())

print(f"\nCompleted successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
