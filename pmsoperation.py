import requests
import pandas as pd
import io
from datetime import datetime

# --- Step 1: Download Excel file from SharePoint ---
shared_link = "https://planeteyefarma.sharepoint.com/:x:/g/IQD3Mf2r8Gy4SrHhTPyoI4JMAdRlz2dwIKn7mzgEnBAIzUU?e=YCcROm"
download_url = shared_link + "&download=1"

output_excel = "PMS.xlsx"
output_csv1 = "Cleaned_PMS_dataset.csv"
output_csv2 = "Cleaned_PMS_dataset2.csv"

print("📥 Downloading PMS Excel file...")
resp = requests.get(download_url)
resp.raise_for_status()

content_type = resp.headers.get("Content-Type", "")
if "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" not in content_type:
    raise ValueError(f"Expected an XLSX file, got Content-Type={content_type!r}")

with open(output_excel, "wb") as f:
    f.write(resp.content)
print(f"✅ Downloaded Excel file as '{output_excel}'")

# --- Step 2: Load Excel ---
excel_data = io.BytesIO(resp.content)
xls = pd.ExcelFile(excel_data)

# =========================================================
# -------------------- SHEET 1 -----------------------------
# =========================================================
print("\n🧹 Cleaning Sheet1...")
df1 = pd.read_excel(xls, sheet_name="Sheet1")

df1.columns = df1.columns.str.strip()
df1 = df1.loc[:, ~df1.columns.str.contains("^Unnamed", case=False)]

# Date normalization
for col in ["Date of Survey", "Last Maintenance Date", "Date"]:
    if col in df1.columns:
        df1[col] = pd.to_datetime(df1[col], dayfirst=True, errors="coerce") \
                     .dt.strftime('%d-%m-%Y')

# Numeric conversions
numeric_cols_1 = [
    "Chainage Start", "Chainage end", "AADT", "Rainfall (mm):",
    "Latitude", "Longitude", "sum chainage"
]
for col in numeric_cols_1:
    if col in df1.columns:
        df1[col] = pd.to_numeric(df1[col], errors="coerce").astype("float32")

# Drop incomplete rows
essential_cols = ["Project Name", "Chainage Start", "Chainage end", "Date", "Latitude", "Longitude"]
df1 = df1.dropna(subset=[c for c in essential_cols if c in df1.columns])

# Save Sheet1 CSV
df1.to_csv(output_csv1, index=False, encoding="utf-8-sig")
print(f"✅ Sheet1 saved as '{output_csv1}'")

# =========================================================
# -------------------- SHEET 2 -----------------------------
# =========================================================
print("\n🧹 Cleaning Sheet2...")
df2 = pd.read_excel(xls, sheet_name="Sheet2")

df2.columns = df2.columns.str.strip()
df2 = df2.loc[:, ~df2.columns.str.contains("^Unnamed", case=False)]

# Normalize blank cells
df2 = df2.replace(r'^\s*$', pd.NA, regex=True)

# Date normalization
for col in ["Date of Survey", "Last Maintenance Date", "Date"]:
    if col in df2.columns:
        df2[col] = pd.to_datetime(df2[col], dayfirst=True, errors="coerce") \
                     .dt.strftime('%d-%m-%Y')

# Numeric conversions (IRI Range is intentionally excluded)
numeric_cols_2 = [
    "Chainage Start", "Chainage end", "AADT",
    "Latitude", "Longitude", "sum chainage",
    "International Roughness Index (IRI):","Width"
]
for col in numeric_cols_2:
    if col in df2.columns:
        df2[col] = pd.to_numeric(df2[col], errors="coerce").astype("float32")


# Drop incomplete rows
df2 = df2.dropna(subset=[c for c in essential_cols if c in df2.columns])

# Save Sheet2 CSV
df2.to_csv(output_csv2, index=False, encoding="utf-8-sig")
print(f"✅ Sheet2 saved as '{output_csv2}'")

# =========================================================
# -------------------- DONE -------------------------------
# =========================================================
print(f"\nCompleted successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")



