# inventoryaoperation.py
import requests
import pandas as pd
import io

# SharePoint/OneDrive shared link for the Excel file
shared_link = "https://planeteyefarma.sharepoint.com/:x:/g/ESSE41hijOFKsTlFEQZPmFIBXr1JpVrCWKctq5lnvrH0Cg?rtime=usI8mavq3Ug"
download_url = shared_link + "&download=1"

# Output filenames
output_excel = "Inventory.xlsx"
output_csv_1 = "Cleaned_Inventory_dataset.csv"
output_csv_2 = "Cleaned_Inventory_dataset2.csv"

print("📥 Downloading Excel file...")

# Download Excel
resp = requests.get(download_url)
resp.raise_for_status()

content_type = resp.headers.get("Content-Type", "")
if "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" not in content_type:
    raise ValueError(f"Expected XLSX file, got {content_type!r}")

# Save Excel locally
with open(output_excel, "wb") as f:
    f.write(resp.content)

print(f"✅ Downloaded Excel file as {output_excel}")

# Load workbook
excel_data = io.BytesIO(resp.content)
xls = pd.ExcelFile(excel_data)

# -----------------------
# Common helpers
# -----------------------
def convert_date(date_str):
    dt = pd.to_datetime(date_str, dayfirst=True, errors="coerce")
    if pd.notna(dt) and dt.year == 1900:
        dt = dt.replace(year=2025)
    return dt

def clean_df(df, numeric_columns):
    df.columns = df.columns.str.strip()
    df = df.loc[:, ~df.columns.str.contains(r"^Unnamed")]

    if "Date" in df.columns:
        df["Date"] = df["Date"].apply(convert_date)

    numeric_columns = [c for c in numeric_columns if c in df.columns]
    df[numeric_columns] = df[numeric_columns].apply(
        pd.to_numeric, errors="coerce"
    ).fillna(0)

    essential_cols = ["Latitude", "Longitude", "Project Name", "Chainage start", "Chainage end", "Date"]
    essential_cols = [c for c in essential_cols if c in df.columns]
    df = df.dropna(subset=essential_cols)

    for col in ["Chainage start", "Chainage end", "Trees"]:
        if col in df.columns:
            df[col] = df[col].astype("float32")

    return df

# =======================
# Process Sheet1
# =======================
if "Sheet1" not in xls.sheet_names:
    raise ValueError(f"'Sheet1' not found. Available sheets: {xls.sheet_names}")

df1 = pd.read_excel(xls, sheet_name="Sheet1")

numeric_cols_sheet1 = [
    "Chainage start", "Chainage end", "Trees", "Culvert", "Street Lights",
    "Bridges", "Traffic Signals", "KM Stones", "Bus Stop", "Truck LayBy",
    "Toll Plaza", "Adjacent Road", "Toilet Blocks", "Rest Area", "RCC Drain",
    "Fuel Station", "Emergency Call Box", "Tunnels", "Footpath", "Junction",
    "Sign Boards", "Solar Blinker", "Divider Break", "Median Plants",
    "Row fencing", "Service Road", "Crash Barrier"
]

df1 = clean_df(df1, numeric_cols_sheet1)
df1.to_csv(output_csv_1, index=False)
print(f"✅ Sheet1 cleaned → {output_csv_1}")

# =======================
# Process Sheet2
# =======================
if "Sheet2" not in xls.sheet_names:
    raise ValueError(f"'Sheet2' not found. Available sheets: {xls.sheet_names}")

df2 = pd.read_excel(xls, sheet_name="Sheet2")

numeric_cols_sheet2 = [
    "Chainage start", "Chainage end", "Trees", "Culvert", "Street Lights",
    "Bridges", "Traffic Signals", "Bus Stop", "Truck LayBy", "Toll Plaza",
    "Adjacent Road", "Toilet Blocks", "Rest Area", "RCC Drain", "Fuel Station",
    "Emergency Call Box", "Tunnels", "Footpath Length", "Footpath Width",
    "Junction", "Sign Boards", "Solar Blinker", "Median Plants",
    "Service Road", "KM Stones", "Crash Barrier", "Median Opening",
    "Circular chamber", "Rectangular Chamber", "Drinking water",
    "Storm water", "STP sinkhole", "Fire Hydrant"
]

df2 = clean_df(df2, numeric_cols_sheet2)
df2.to_csv(output_csv_2, index=False)
print(f"✅ Sheet2 cleaned → {output_csv_2}")

# -----------------------
# Preview
# -----------------------
print("\n📊 Sheet1 preview:")
print(df1.head())

print("\n📊 Sheet2 preview:")
print(df2.head())
