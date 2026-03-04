import requests
import pandas as pd

# --------------------------------------------------
# Step 1: Download Excel from SharePoint
# --------------------------------------------------

shared_link = "https://planeteyefarma.sharepoint.com/:x:/g/ESPyKU795WVFr7dLUbTC6E4BdQU6psCftFLpJCKBFzBRpA?rtime=J3F5eNDq3Ug"
download_url = shared_link + "&download=1"
output_excel = "Distress_prediction.xlsx"

print("📥 Downloading Excel file...")
resp = requests.get(download_url, allow_redirects=True)
resp.raise_for_status()

# Safer validation (faster & more reliable)
if resp.content[:2] != b"PK":
    raise ValueError("Downloaded file is not a valid XLSX file.")

with open(output_excel, "wb") as f:
    f.write(resp.content)

print(f"✅ Downloaded Excel file as '{output_excel}'")

# --------------------------------------------------
# Step 2: Optimized cleaning function
# --------------------------------------------------

def clean_distress_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Clean column names
    df.columns = df.columns.str.strip()
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    # -------- Date (VECTORISED - FAST) --------
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(
            df["Date"],
            dayfirst=True,
            errors="coerce"
        )

        # Fix 1900 year issue
        mask_1900 = df["Date"].dt.year == 1900
        df.loc[mask_1900, "Date"] = df.loc[mask_1900, "Date"] + pd.DateOffset(years=125)

        df["Date"] = df["Date"].dt.strftime("%d-%m-%Y")

    # -------- Numeric columns --------
    numeric_columns = [
        "Chainage Start", "Chianage End", "Pothole", "Alligator crack",
        "Oblique crack", "Edge Break", "Patching",
        "Bleeding", "Hotspots", "Rutting", "Raveling"
    ]

    numeric_columns = [c for c in numeric_columns if c in df.columns]

    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("float32")

    # -------- Drop missing essentials --------
    essential_fields = ["Latitude", "Longitude", "Project Name"]
    existing_essentials = [c for c in essential_fields if c in df.columns]
    df = df.dropna(subset=existing_essentials)

    return df


# --------------------------------------------------
# Step 3: Read Excel and process sheets
# --------------------------------------------------

print("🧹 Cleaning and preprocessing datasets...")

xls = pd.ExcelFile(output_excel, engine="openpyxl")

required_sheets = ["Sheet1", "Sheet2", "Sheet3"]
for sheet in required_sheets:
    if sheet not in xls.sheet_names:
        raise ValueError(
            f"Expected sheet '{sheet}' not found. Available sheets: {xls.sheet_names}"
        )

# --------------------------------------------------
# Sheet1 + Sheet2 → Dataset1 (COMBINED)
# --------------------------------------------------

df1_sheet1 = pd.read_excel(xls, sheet_name="Sheet1")
df1_sheet2 = pd.read_excel(xls, sheet_name="Sheet2")

df1_sheet1 = clean_distress_dataframe(df1_sheet1)
df1_sheet2 = clean_distress_dataframe(df1_sheet2)

df1 = pd.concat([df1_sheet1, df1_sheet2], ignore_index=True)
df1.to_csv("Cleaned_Distress_prediction_dataset.csv", index=False)

print("✅ Sheet1 + Sheet2 cleaned and combined as dataset1")

# --------------------------------------------------
# Sheet3 → Dataset2
# --------------------------------------------------

df2 = pd.read_excel(xls, sheet_name="Sheet3")
df2 = clean_distress_dataframe(df2)
df2.to_csv("Cleaned_Distress_prediction_dataset2.csv", index=False)

print("✅ Sheet3 cleaned and saved as dataset2")

# --------------------------------------------------
# Step 4: Preview
# --------------------------------------------------

preview_cols = [
    "Project Name", "Chainage Start", "Chianage End",
    "Total Distress", "Distress Type",
    "Pothole", "Alligator crack", "Oblique crack",
    "Edge Break", "Patching", "Bleeding",
    "Hotspots", "Rutting", "Raveling"
]

print("\n📊 Preview Dataset1 (Sheet1 + Sheet2):")
print(df1[[c for c in preview_cols if c in df1.columns]].head())
