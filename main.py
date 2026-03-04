from fastapi import FastAPI, HTTPException, Depends, Query,Path, UploadFile, File
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import pandas as pd
import os,io
from typing import List, Tuple
import math
import time,requests, re, os
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Callable
import subprocess
import numpy as np
import collections,cv2
import html
from fastapi.concurrency import run_in_threadpool
from prometheus_client import Counter, Histogram, Gauge, Info, generate_latest, CONTENT_TYPE_LATEST, CollectorRegistry, REGISTRY
 

class Settings(BaseSettings):
    data_dir: str = os.getcwd()
    inventory_file: str = "Cleaned_Inventory_dataset.csv"
    inventory2_file: str = "Cleaned_Inventory_dataset2.csv"
    prediction_file: str = "Cleaned_Distress_prediction_dataset.csv"
    prediction2_file: str = "Cleaned_Distress_prediction_dataset2.csv"
    report_file: str = "Cleaned_Distress_report_road_dataset.csv"
    report2_file: str = "Cleaned_Distress_report_road_dataset2.csv"
    pis_file: str = "Cleaned_PIS_dataset.csv"
    pis2_file: str = "Cleaned_PIS_dataset2.csv"
    tis_file: str = "Cleaned_TIS_dataset.csv"
    tis2_file: str = "Cleaned_TIS_dataset2.csv"
    pms_file: str = "Cleaned_PMS_dataset.csv"
    pms2_file: str = "Cleaned_PMS_dataset2.csv"
    rwfis_file: str = "Cleaned_RWFIS_dataset.csv"
    rwfis2_file: str = "Cleaned_RWFIS_dataset2.csv"
    ais_file: str = "Cleaned_AIS_dataset.csv"
    ais2_file: str = "Cleaned_AIS_dataset2.csv"
    bms_file: str = "Cleaned_BMS_dataset.csv"
    cron_interval_minutes: int = 10
    operation_interval_seconds: int = 600
    model_config = {"env_file": ".env", "extra": "ignore"}

settings = Settings()

app = FastAPI()

# ============================================
# PROMETHEUS METRICS FOR RAILWAY
# ============================================

# Application health status (1 = up, 0 = down)
# This is used by alert: NHITRAMSAPIDown
up = Gauge('up', 'Application is running (1 = up, 0 = down)', ['job'])

# Application info
app_info = Info('app_info', 'Application information')
app_info.info({
    'version': '1.0.0',
    'name': 'NHIT_RAMS API'
})

# HTTP request metrics
http_requests_total = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status']
)

# HTTP request duration - MUST be Histogram with buckets for alert: NHITRAMSHighLatency
http_request_duration_seconds = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint'],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0)
)

# Image processing metrics
images_processed_total = Counter(
    'images_processed_total',
    'Total number of images processed',
    ['operation_type']
)

# Excel operations metrics
excel_operations_total = Counter(
    'excel_operations_total',
    'Total Excel operations',
    ['operation']
)

# Scheduler metrics
scheduled_jobs_active = Gauge(
    'scheduled_jobs_active',
    'Number of active scheduled jobs'
)

# Cache metrics
cache_entries = Gauge(
    'cache_entries',
    'Number of entries in cache',
    ['cache_type']
)

# Error metrics
errors_total = Counter(
    'errors_total',
    'Total number of errors',
    ['error_type']
)

# Initialize metrics immediately (so they appear in /metrics even before first request)
up.labels(job='NHIT_RAMS_api_health').set(1)
scheduled_jobs_active.set(0)  # Will be updated on startup

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ============================================
# MIDDLEWARE TO TRACK ALL HTTP REQUESTS
# ============================================
@app.middleware("http")
async def prometheus_middleware(request, call_next):
    """Track all HTTP requests for Prometheus metrics"""
    # Skip metrics endpoint itself
    if request.url.path == "/metrics":
        return await call_next(request)
    
    start_time = time.time()
    
    try:
        response = await call_next(request)
        
        # Record metrics
        duration = time.time() - start_time
        http_request_duration_seconds.labels(
            method=request.method,
            endpoint=request.url.path
        ).observe(duration)
        
        http_requests_total.labels(
            method=request.method,
            endpoint=request.url.path,
            status=str(response.status_code)
        ).inc()
        
        return response
        
    except Exception as e:
        # Record error
        duration = time.time() - start_time
        http_request_duration_seconds.labels(
            method=request.method,
            endpoint=request.url.path
        ).observe(duration)
        
        http_requests_total.labels(
            method=request.method,
            endpoint=request.url.path,
            status="500"
        ).inc()
        
        errors_total.labels(error_type='middleware_exception').inc()
        raise

# === CONFIG ===
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")

# === Distress Excel for Form===
DRIVE_ID_DISTRESS = "b!1w4jNcnx8UGvUM01ZuhkgAUn7FisCZxLsw7TU-X9gPySPXfTGJVpSatARJkcxzaB"
ITEM_ID_DISTRESS = "01QAOLQ22MP3MHBBTHMFHK4N7O2H7FR4U7"
SHEET_NAME_DISTRESS = "Sheet1"

# === Inventory Excel for Form ===
DRIVE_ID_INVENTORY = "b!1w4jNcnx8UGvUM01ZuhkgAUn7FisCZxLsw7TU-X9gPySPXfTGJVpSatARJkcxzaB"
ITEM_ID_INVENTORY = "01QAOLQ2ZEQTRVQYUM4FFLCOKFCEDE7GCS"
SHEET_NAME_INVENTORY = "Sheet1"

# =====================================================

DISTANCE_SCALING_FACTOR = 4
PIPELINE = {}
# ----------------------------------------------------
# SESSION MEMORY STORAGE FOR MAIN EXCEL
# ----------------------------------------------------
SESSION_MAIN_EXCEL = None
SESSION_MAIN_NAME = None
 
def generate_combined_matrix1(image_bytes, fname):
 
    nparr = np.frombuffer(image_bytes, np.uint8)
    img_gray = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)
    img_gray = cv2.normalize(img_gray, None, 0, 255, cv2.NORM_MINMAX)
 
    height, width = img_gray.shape
    is_LHS = "lhs" in fname.lower()
 
    scale_m_per_pixel = 100.0 / width
    spacing_m = 0.12
    gap_pix = spacing_m / scale_m_per_pixel
 
    hori_positions = sorted(list({min(height - 1, int(i * gap_pix))
                                  for i in range(int(height / gap_pix) + 1)}))
    vert_positions = sorted(list({min(width - 1, int(i * gap_pix))
                                  for i in range(int(width / gap_pix) + 1)}))
 
    def extract_horizontal(y):
        p = img_gray[y, :].astype(float)
        p[p > 200] = 0
        return p
 
    def extract_vertical(x):
        p = img_gray[:, x].astype(float)
        p[p > 200] = 0
        return p
 
    # ---------------- HORIZONTAL ----------------
    hori_df = pd.DataFrame()
    hori_df["Distance_meter"] = np.arange(width) * scale_m_per_pixel
 
    for i, y in enumerate(hori_positions):
        hori_df[f"H{i+1}"] = extract_horizontal(y)
 
    hori_cols = [c for c in hori_df.columns if c != "Distance_meter" and (hori_df[c] > 0).any()]
    hori_df = hori_df[["Distance_meter"] + hori_cols]
    hori_df = hori_df[hori_df[hori_cols].gt(0).any(axis=1)].copy()
 
    if is_LHS:
        rev_cols = hori_cols[::-1]
        rename_map = {old: f"H{i+1}" for i, old in enumerate(rev_cols)}
        hori_df = hori_df[["Distance_meter"] + rev_cols].rename(columns=rename_map)
    else:
        rename_map = {old: f"H{i+1}" for i, old in enumerate(hori_cols)}
        hori_df = hori_df[["Distance_meter"] + hori_cols].rename(columns=rename_map)
 
    hori_df["Distance_meter"] -= hori_df["Distance_meter"].min()
 
    # ---------------- VERTICAL ----------------
    vert_raw = {f"V{i+1}": extract_vertical(x) for i, x in enumerate(vert_positions)}
    vert_df = pd.DataFrame(vert_raw)
    vert_df.insert(0, "Distance_meter_V", np.arange(height) * scale_m_per_pixel)
 
    valid_vcols = [c for c in vert_df.columns if c.startswith("V") and (vert_df[c] > 0).sum() >= 3]
    vert_df = vert_df[["Distance_meter_V"] + valid_vcols]
    vert_df = vert_df[vert_df[valid_vcols].gt(0).any(axis=1)].copy()
 
    if len(valid_vcols) == 0:
        new_vert_df = pd.DataFrame()
    else:
        vertical_matrix = vert_df[valid_vcols].to_numpy().T
        new_vert_df = pd.DataFrame(
            vertical_matrix,
            columns=[f"V{i+1}" for i in range(vertical_matrix.shape[1])]
        )
 
    # ---------------- MERGE ----------------
    df_final = pd.concat(
        [hori_df.reset_index(drop=True),
         new_vert_df.reset_index(drop=True)],
        axis=1
    )
 
    # ---------------- MAIN VALUE FORMULA ----------------
    grey_cols = [c for c in df_final.columns if c.startswith("H") or c.startswith("V")]
 
    def compute_main_value(row):
        values = row[grey_cols].astype(float)
        zero_mask = values == 0
 
        valid = values.where((values >= 1) & (values <= 100))
        if valid.dropna().empty:
            return row
 
        x = valid.max()
        P99 = 0.99 * x
        P66 = 0.66 * x
 
        norm = ((P99 - valid) / (P99 - P66)) * 100
        main = (1 - (norm / 100)) * 50
 
        result = np.where((values >= 1) & (values <= 100), main, values)
        result[zero_mask] = 0
 
        row[grey_cols] = np.clip(result, 0, None)
        return row
 
    df_main = df_final.apply(compute_main_value, axis=1)
 
    # FIX — JSON safe (no NaN allowed)
    df_main = df_main.fillna(0)
 
    return df_main


def validate_filename(fname):
    return re.match(r".+_(RHS|LHS)_L(1|2)\.[A-Za-z0-9]+$", fname, re.IGNORECASE)
 
def detect_side(fname):
    return "LHS" if "LHS" in fname.upper() else "RHS"
 
def df_to_json_safe(df: pd.DataFrame):
    return df.replace({np.nan: None}).to_dict(orient="records")

# =====================================================
# RAW + MAIN
def process_image_hv(image_bytes, fname):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)
    img = cv2.normalize(img, None, 0, 255, cv2.NORM_MINMAX)
    h, w = img.shape
    side = detect_side(fname)
    scale = 100.0 / w
    spacing = 0.12
    gap = spacing / scale
 
    h_pos = sorted({min(h-1,int(i*gap)) for i in range(int(h/gap)+1)})
    v_pos = sorted({min(w-1,int(i*gap)) for i in range(int(w/gap)+1)})
 
    hori = pd.DataFrame({"Distance_meter": np.arange(w)*scale})
    for i,y in enumerate(h_pos):
        p = img[y,:].astype(float); p[p>200]=0
        hori[f"H_{i+1}"] = p
 
    mask = hori.filter(like="H_").gt(0).any(axis=1)
    hori = hori[mask].reset_index(drop=True)
    hori["Distance_meter"] -= hori["Distance_meter"].min()
    hcols = [c for c in hori.columns if c.startswith("H_") and (hori[c]>0).any()]
    if side=="LHS": hcols = hcols[::-1]
    hori = hori[["Distance_meter"]+hcols]
    hori.columns = ["Distance_meter"]+[f"H_{i+1}" for i in range(len(hcols))]
 
    vert = pd.DataFrame({"Distance_meter": np.arange(h)*scale})
    for i,x in enumerate(v_pos):
        p = img[:,x].astype(float); p[p>200]=0
        vert[f"V_{i+1}"] = p
 
    mask = vert.filter(like="V_").gt(0).any(axis=1)
    vert = vert[mask].reset_index(drop=True)
    vert["Distance_meter"] -= vert["Distance_meter"].min()
    vcols = [c for c in vert.columns if c.startswith("V_") and (vert[c]>0).any()]
    vert = vert[["Distance_meter"]+vcols]
    vert.columns = ["Distance_meter"]+[f"V_{i+1}" for i in range(len(vcols))]
 
    return hori, vert, apply_main(hori.copy()), apply_main(vert.copy())

# =====================================================
# PATTERN
def is_mirror(vals):
    return len(vals)==3 and vals[0]==vals[2]
 
def analyze_grids(df):
    data = df.iloc[:,1:].values
    rows, cols = data.shape
    complete, partial = [], []
    names = df.columns[1:].tolist()
 
    for c in range(cols-2):
        for r in range(rows-2):
            grid = data[r:r+3, c:c+3]
            match = [i for i in range(3) if is_mirror(grid[i,:])]
            if len(match)==3:
                complete.append({"values":grid.tolist(),"row_start":r+2,"col_names":names[c:c+3]})
            elif len(match)>0:
                for i in match:
                    partial.append({"values":grid[i,:].tolist(),"row":r+i+2,"col_names":names[c:c+3]})
    return complete, partial
 
def build_pattern(df, complete, partial):
    out = df.copy()
    cols = [c for c in out.columns if c!="Distance_meter"]
    out[cols] = np.nan
 
    for g in complete:
        r0 = g["row_start"]-2
        for i in range(3):
            for j in range(3):
                out.at[r0+i, g["col_names"][j]] = g["values"][i][j]
 
    for r in partial:
        r0 = r["row"]-2
        for j in range(3):
            out.at[r0, r["col_names"][j]] = r["values"][j]
 
    return out
 
# =====================================================
# VISUALIZATION
def build_visualization(df_pattern, df_main):
    viz = df_pattern.copy()
    for col in viz.columns:
        if col=="Distance_meter": continue
        has_pattern = df_pattern[col].notna() & (df_pattern[col]!=0)
        blank = df_pattern[col].isna() | (df_pattern[col]==0)
        viz.loc[has_pattern,col] = np.nan
        viz.loc[blank,col] = df_main.loc[blank,col]
        viz.loc[viz[col]==0,col] = np.nan
    return viz

# ----------------------------------------------------
# Profile extractor
# ----------------------------------------------------
def analyze_profile(y_pos, img, scale, name):
    row = img[y_pos, :].astype(float)
    row[row > 200] = 0
    dist = np.arange(len(row)) * scale
    return pd.Series(row, name=name), dist
 
# =====================================================
# MAIN VALUE
def apply_main(df):
    cols = [c for c in df.columns if c.startswith(("H_", "V_"))]
 
    def f(row):
        vals = row[cols].clip(lower=0)
        zero = vals == 0
        valid = vals.where((vals >= 1) & (vals <= 100))
        if valid.dropna().empty:
            row[cols] = vals
            return row
        x = valid.max()
        P99, P66 = x*0.99, x*0.66
        norm = ((P99-valid)/(P99-P66))*100
        main = (1-(norm/100))*50
        res = np.where((vals>=1)&(vals<=100), main, vals)
        res[zero] = 0
        row[cols] = np.clip(res,0,None)
        return row
 
    return df.apply(f, axis=1)

# ----------------------------------------------------
# Main value conversion
# ----------------------------------------------------
def compute_main_value(df):
    grey_cols = [c for c in df.columns if "grey" in c]
 
    def calc(row):
        vals = row[grey_cols].clip(lower=0)
        valid = vals.where((vals >= 1) & (vals <= 100))
        zero_mask = vals == 0
 
        if valid.dropna().empty:
            return row
 
        x = valid.max()
        P99 = 0.99 * x
        P66 = 0.66 * x
 
        norm = ((P99 - valid) / (P99 - P66)) * 100
        final = (1 - norm / 100) * 50
        result = np.where((vals >= 1) & (vals <= 100), final, vals)
        result[zero_mask] = 0
        row[grey_cols] = result
        return row
 
    return df.apply(calc, axis=1)
 
 
# ----------------------------------------------------
# PROCESS IMAGE → RAW + MAIN (IN MEMORY)
# ----------------------------------------------------
def process_image_generate_excels_memory(image_bytes, fname):
 
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)
    img = cv2.normalize(img, None, 0, 255, cv2.NORM_MINMAX).astype(np.uint8)
 
    side = detect_side(fname)
    scale = 100 / img.shape[1]
    spacing_m = 0.12
    pixel_gap = spacing_m / scale
    height = img.shape[0]
 
    positions = sorted(
        set([
            min(height - 1, int(i * pixel_gap))
            for i in range(int(height / pixel_gap) + 1)
        ])
    )
 
    profiles = []
    dist_reference = None
    colnames = []
 
    for i, y in enumerate(positions):
        name = f"L{i+1} grey value"
        s, dist = analyze_profile(y, img, scale, name)
        profiles.append(s)
        colnames.append(name)
        if dist_reference is None:
            dist_reference = dist
 
    df = pd.concat(profiles, axis=1)
    df.insert(0, "Distance_meter", dist_reference)
 
    valid_cols = [c for c in colnames if (df[c] > 0).any()]
    df = df[["Distance_meter"] + valid_cols]
 
    if side == "LHS":
        valid_cols = valid_cols[::-1]
 
    rename_map = {old: f"L{i+1} grey value" for i, old in enumerate(valid_cols)}
    df = df.rename(columns=rename_map)
    df = df[["Distance_meter"] + list(rename_map.values())]
 
    df[df.select_dtypes(include="number").columns] = df.select_dtypes(include="number").clip(0)
 
    df_main = compute_main_value(df.copy())
    df_main[df_main.select_dtypes(include="number").columns] = df_main.select_dtypes(include="number").clip(0)
 
    return df_main
 
 
# =====================================================
# CLASSIFICATION
CLASSIFICATION_RULES = {
    "Pothole":{"value":(50,100),"shape":[(3,3),(4,3),(5,3),(6,3),(7,2),(8,2)]},
    "Alligator":{"value":(6,20),"shape":[(1,8),(2,8),(3,8)]},
    "Transverse Crack":{"value":(6,24),"shape":[(1,5),(1,6),(1,7)]},
    "Longitudinal Crack":{"value":(10,30),"shape":[(3,1),(1,3),(4,1)]},
    "Hairline":{"value":(2,6),"shape":[(2,3),(3,2),(3,4)]},
}
CARD=[(0,1),(0,-1),(1,0),(-1,0)]
DIAG=[(1,1),(1,-1),(-1,1),(-1,-1)]
 
def run_classification(matrix, dist, is_L2):
    rows, cols = matrix.shape
    visited = np.zeros((rows,cols),bool)
    res=[]
    for r in range(rows):
        for c in range(cols):
            if visited[r,c]: continue
            val=matrix[r,c]
            q=collections.deque([(r,c)])
            visited[r,c]=True
            rmin=rmax=r; cmin=cmax=c
            while q:
                rr,cc=q.popleft()
                for dr,dc in CARD+DIAG:
                    nr,nc=rr+dr,cc+dc
                    if 0<=nr<rows and 0<=nc<cols and not visited[nr,nc] and matrix[nr,nc]==val:
                        visited[nr,nc]=True
                        q.append((nr,nc))
                        rmin,rmax=min(rmin,nr),max(rmax,nr)
                        cmin,cmax=min(cmin,nc),max(cmax,nc)
            R,C=rmax-rmin+1,cmax-cmin+1
            for name,rule in CLASSIFICATION_RULES.items():
                if (R,C) in rule["shape"] and rule["value"][0]<val<rule["value"][1]:
                    length=abs(dist[rmin]-dist[rmax])
                    width=(C-1)*DISTANCE_SCALING_FACTOR*10
                    d=(cmax+1)*DISTANCE_SCALING_FACTOR*0.01
                    if is_L2: d+=3.4
                    res.append({
                        "Classification":name,
                        "Length(M)":float(length),
                        "Width(MM)":float(width),
                        "Depth(MM)":float(val),
                        "Distance_from_Median(M)":float(d),
                        "startloc":float(dist[rmin]),
                        "endloc":float(dist[rmax])
                    })
                    break
    return pd.DataFrame(res)


# =====================================================
def fetch(name: str):
    key = name.rsplit(".", 1)[0]
    return PIPELINE.get(key)

def run_classification1(matrix, dist, is_L2):
 
    matrix = np.nan_to_num(matrix, nan=0)
 
    rows, cols = matrix.shape
    visited = np.full((rows, cols), False)
    results = []
 
    for r in range(rows):
        for c in range(cols):
 
            if visited[r, c]:
                continue
 
            val = matrix[r, c]
            queue = collections.deque([(r, c)])
            visited[r, c] = True
 
            rmin = rmax = r
            cmin = cmax = c
 
            # BFS flood fill
            while queue:
                rr, cc = queue.popleft()
 
                for dr, dc in CARD + DIAG:
                    nr, nc = rr + dr, cc + dc
 
                    if 0 <= nr < rows and 0 <= nc < cols:
                        if not visited[nr, nc] and matrix[nr, nc] == val:
                            visited[nr, nc] = True
                            queue.append((nr, nc))
                            rmin = min(rmin, nr)
                            rmax = max(rmax, nr)
                            cmin = min(cmin, nc)
                            cmax = max(cmax, nc)
 
            Rdim = rmax - rmin + 1
            Cdim = cmax - cmin + 1
 
            for cname, rule in CLASSIFICATION_RULES.items():
 
                if (Rdim, Cdim) in rule["shape"] and rule["value"][0] < val < rule["value"][1]:
 
                    length = abs(dist[rmin] - dist[rmax])
                    width = (Cdim - 1) * DISTANCE_SCALING_FACTOR * 10
                    d = (cmax + 1) * DISTANCE_SCALING_FACTOR * 0.01
 
                    if is_L2:
                        d += 3.4
 
                    results.append({
                        "Classification": cname,
                        "Length(M)": float(length),
                        "Width(MM)": float(width),
                        "Depth(MM)": float(val),
                        "Distance_from_Median(M)": float(d),
                        "startloc": float(dist[rmin]),
                        "endloc": float(dist[rmax])
                    })
                    break
 
    return pd.DataFrame(results)
# -----For Distress Analyzer End------

executor = ThreadPoolExecutor(max_workers=3)
scheduler = AsyncIOScheduler()
last_run_timestamp: str = ""

def deep_sanitize(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: deep_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [deep_sanitize(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj

def load_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found at {path}")
    
    # Try UTF-8 first (for PIS), fallback to ISO-8859-1
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="ISO-8859-1")
    
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_', regex=False)
    df.rename(columns={'chianage_end': 'chainage_end'}, inplace=True)
    df.dropna(axis=1, how='all', inplace=True)
    
    # Normalize text columns
    for col in ['asset_type', 'distress_type', 'project_name', 'direction']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    
    # Handle PIS-specific columns (detect automatically)
    if 'actual_start_chainage' in df.columns and 'actual_end_chainage' in df.columns:
        df.rename(columns={
            'actual_start_chainage': 'chainage_start',
            'actual_end_chainage': 'chainage_end',
            'month': 'date'
        }, inplace=True)
    
    elif 'chainage_from' in df.columns and 'chainage_to' in df.columns:
        df.rename(columns={
            'chainage_from': 'chainage_start',
            'chainage_to': 'chainage_end'
        }, inplace=True)

    # Handle date format consistently
    # Handle date format consistently
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce', dayfirst=True)
        df = df[df['date'].notna()]
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    # ---- FORCE NUMERIC CHAINAGE (FIX) ----
    for col in ["chainage_start", "chainage_end"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    if "chainage_start" in df.columns and "chainage_end" in df.columns:
        df = df.dropna(subset=["chainage_start", "chainage_end"])
    
    return df

def apply_filters(
    df: pd.DataFrame,
    filter_col: str,
    values: List[str],
    start: float,
    end: float,
    date: str,
    directions: List[str],
    projects: List[str]
) -> pd.DataFrame:
    q = df.copy()
    
    # Asset/Distress type filter - ONLY apply if values is not empty
    if values and not (len(values) == 1 and values[0].lower() == "all"):
        q = q[q[filter_col].isin(values)]
    
    # Direction filter
    if not (len(directions) == 1 and directions[0].lower() == "all"):
        q = q[q["direction"].isin(directions)]
    
    # Project filter
    if not (len(projects) == 1 and projects[0].lower() == "all"):
        q = q[q["project_name"].isin(projects)]
    
    # Chainage + Date
    q = (
        q.loc[lambda d: (d['chainage_start'] >= start) & (d['chainage_end'] <= end)]
        .loc[lambda d: d['date'] == date]
    )
    
    return q


def group_by_latlon(df: pd.DataFrame) -> List[List[Dict[str, Any]]]:
    if {'latitude', 'longitude'}.issubset(df.columns):
        return [g.to_dict('records') for _, g in df.groupby(['latitude', 'longitude'], sort=False)]
    return [df.to_dict('records')]

def cron_job():
    global last_run_timestamp
    last_run_timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    print("[Cron]", last_run_timestamp)

def run_script(name: str):
    path = os.path.join(settings.data_dir, name)
    if os.path.exists(path):
        def task():
            try:
                subprocess.run(["python", path], check=True)
            except Exception as e:
                print(f"Error running {name}:", e)
        executor.submit(task)
    else:
        print("Script not found:", name)

def schedule_operations():
    for script in [
        "distress_predictionaoperation.py",
        "distress_roportaopration.py",
        "inventoryaoperation.py",
        "pisoperation.py",
        "tisoperation.py",
        "pmsoperation.py",
        "rwfisoperation.py",
        "aisoperation.py",
        "bmsoperation.py"
    ]:
        run_script(script)

@app.on_event("startup")
async def on_startup():
    scheduler.add_job(
        cron_job, trigger=IntervalTrigger(minutes=settings.cron_interval_minutes),
        id="cron", replace_existing=True
    )
    scheduler.add_job(
        schedule_operations,
        trigger=IntervalTrigger(seconds=settings.operation_interval_seconds),
        id="ops", replace_existing=True
    )
    scheduler.start()
    schedule_operations()
    
    # Update metrics - Set up=1 for health check
    up.labels(job='NHIT_RAMS_api_health').set(1)
    scheduled_jobs_active.set(len(scheduler.get_jobs()))

@app.on_event("shutdown")
def on_shutdown():
    # Mark app as down when shutting down
    up.labels(job='NHIT_RAMS_api_health').set(0)
    scheduler.shutdown()

# ============================================
# METRICS ENDPOINT FOR RAILWAY PROMETHEUS
# ============================================
@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint in text format"""
    return Response(
        content=generate_latest(REGISTRY),
        media_type=CONTENT_TYPE_LATEST
    )

# ============================================
# HEALTH CHECK ENDPOINT
# ============================================
@app.get("/health")
async def health_check():
    """Health check endpoint for Railway"""
    return {
        "status": "healthy",
        "app_up": 1,
        "scheduler_running": scheduler.running,
        "active_jobs": len(scheduler.get_jobs())
    }

# Pydantic models
class BaseFilter(BaseModel):
    chainage_start: float
    chainage_end: float
    date: str
    direction: List[str]   # <-- now list
    project_name: List[str]  # <-- now list


class InventoryFilter(BaseFilter):
    asset_type: List[str]

class DistressFilter(BaseFilter):
    distress_type: List[str]

class DistressRecord(BaseModel):   # for form API
    Latitude: float
    Longitude: float
    Chainage_Start: float
    Chainage_End: float
    Project_Name: str
    Distress_Type: str
    Direction: str
    Date: str
    Length: float = 0
    Carriage_Type: str = ""
    Width: float = 0
    Depth: float = 0
    Lane: str = ""
    No_of_Distress: int = 1


class InventoryRecord(BaseModel):   # for form API
    Project_Name: str
    Chainage_start: float
    Chainage_end: float
    Direction: str
    Asset_type: str
    Latitude: float
    Longitude: float
    Date: str
    Sub_Asset_Type: str = ""
    Carriage_Type: str = ""
    Lane: str = ""
    No_of_inventories: int = 0


def get_access_token() -> str:
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    r = requests.post(url, data=data, timeout=30)
    if r.status_code != 200:
        raise HTTPException(status_code=401, detail=f"Token error: {r.text}")
    return r.json()["access_token"]


def index_to_excel_col(n: int) -> str:
    s = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        s = chr(65 + rem) + s
    return s


def extract_next_row_from_used_range(addr: str) -> int:
    part = addr.split("!")[-1]
    if ":" in part:
        end = part.split(":")[-1]
    else:
        end = part
    m = re.search(r"(\d+)$", end)
    return int(m.group()) + 1 if m else 2

# CSV cache + loader
cache: Dict[str, pd.DataFrame] = {}
def get_df(key: str, fname: str) -> pd.DataFrame:
    if key not in cache:
        try:
            cache[key] = load_csv(os.path.join(settings.data_dir, fname))
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
    return cache[key]

# Dynamic filtering/count endpoints
def make_filter_ep(kind: str) -> Callable:
    def ep(
        req: (
            InventoryFilter if kind == 'inv' or kind == 'inv2' else
            DistressFilter if kind in ['pred','pred2', 'rep','rep2'] else
            BaseFilter
        ),
        df: pd.DataFrame = Depends(lambda: get_df(
            kind,
            settings.inventory_file if kind == 'inv' else (
                settings.inventory2_file if kind == 'inv2' else (
                    settings.prediction_file if kind == 'pred' else (
                        settings.prediction2_file if kind == 'pred2' else (
                            settings.report_file if kind == 'rep' else (
                                settings.report2_file if kind == 'rep2' else (
                                    settings.pis_file if kind == 'pis' else (
                                        settings.pis2_file if kind == 'pis2' else (
                                            settings.tis_file if kind == 'tis' else (
                                                settings.tis2_file if kind == 'tis2' else (
                                                    settings.pms_file if kind == 'pms' else (
                                                        settings.pms2_file if kind == 'pms2' else (
                                                            settings.rwfis_file if kind == 'rwfis' else (
                                                                settings.rwfis2_file if kind == 'rwfis2' else (
                                                                    settings.bms_file if kind == 'bms' else (
                                                                        settings.ais2_file if kind == 'ais2' else settings.ais_file
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        ))
    ):
        # Column selector
        col = 'asset_type' if kind in ['inv', 'inv2'] else (
            'distress_type' if kind in ['pred','pred2', 'rep','rep2'] else None
        )
        vals = req.asset_type if kind in ['inv', 'inv2'] else (
            req.distress_type if kind in ['pred', 'pred2', 'rep','rep2'] else []
        )
        
        df_f = apply_filters(
            df,
            col if col else df.columns[0],
            vals if col else [],
            req.chainage_start,
            req.chainage_end,
            req.date,
            req.direction,
            req.project_name
        )
        
        if df_f.empty:
            return JSONResponse({"message": "No match"})
        
        return JSONResponse(deep_sanitize(group_by_latlon(df_f)))
    
    return ep

app.post("/inventory_filter")(make_filter_ep('inv'))
app.post("/inventory2_filter")(make_filter_ep('inv2'))
app.post("/distress_predic_filter")(make_filter_ep('pred'))
app.post("/distress_predic_filter2")(make_filter_ep('pred2'))
app.post("/distress_report_filter")(make_filter_ep('rep'))
app.post("/distress_report_filter2")(make_filter_ep('rep2'))
app.post("/pis_filter")(make_filter_ep('pis')) 
app.post("/pis2_filter")(make_filter_ep('pis2'))
app.post("/tis_filter")(make_filter_ep('tis'))
app.post("/tis2_filter")(make_filter_ep('tis2'))
app.post("/pms_filter")(make_filter_ep('pms'))
app.post("/pms2_filter")(make_filter_ep('pms2'))
app.post("/rwfis_filter")(make_filter_ep('rwfis')) 
app.post("/rwfis2_filter")(make_filter_ep('rwfis2'))
app.post("/ais_filter")(make_filter_ep('ais'))
app.post("/ais2_filter")(make_filter_ep('ais2'))
app.post("/bms_filter")(make_filter_ep('bms'))


@app.post("/distress_report_filter_kml")
def distress_report_filter_kml(
    req: DistressFilter,
    df: pd.DataFrame = Depends(lambda: get_df("rep", settings.report_file)),
):
    """
    Apply the same filters as /distress_report_filter and return a KML file
    with one Point Placemark per distress record (using latitude/longitude).
    """
    # Apply filters exactly like /distress_report_filter
    df_f = apply_filters(
        df,
        "distress_type",
        req.distress_type,
        req.chainage_start,
        req.chainage_end,
        req.date,
        req.direction,
        req.project_name,
    )

    if df_f.empty:
        return JSONResponse({"message": "No match"})

    # Ensure required columns exist; otherwise this will gracefully
    # fall back to blanks/zeros in the description.
    def get_val(row, col, default=""):
        return row[col] if col in row and row[col] is not None else default

    # Build KML string
    parts: List[str] = []
    parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    parts.append('<kml xmlns="http://www.opengis.net/kml/2.2">')
    parts.append("  <Document>")
    parts.append("    <name>Distress Report</name>")
    # Single reusable style (icon similar to sample)
    parts.append('    <Style id="distressStyle">')
    parts.append("      <IconStyle>")
    parts.append("        <color>ffffffff</color>")
    parts.append("        <colorMode>normal</colorMode>")
    parts.append("        <scale>1.3</scale>")
    parts.append("        <Icon>")
    parts.append("          <href>http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png</href>")
    parts.append("        </Icon>")
    parts.append("      </IconStyle>")
    parts.append("    </Style>")

    # Group Placemarks by distress_type to mimic Folder layout
    for distress_type, grp in df_f.groupby("distress_type", sort=False):
        folder_name = str(distress_type)
        parts.append("    <Folder>")
        parts.append(f"      <name>{html.escape(folder_name)}</name>")

        for _, r in grp.iterrows():
            row = r.to_dict()

            ch_start = get_val(row, "chainage_start", "")
            ch_end = get_val(row, "chainage_end", "")
            direction = get_val(row, "direction", "")
            lane = get_val(row, "lane", "")
            length = get_val(row, "length", 0)
            area = get_val(row, "area", 0)
            width = get_val(row, "width", 0)
            depth = get_val(row, "depth", 0)
            pavement_type = get_val(row, "pavement_type", "")
            project_name = get_val(row, "project_name", "")

            lat = get_val(row, "latitude", None)
            lon = get_val(row, "longitude", None)
            if lat is None or lon is None:
                continue  # skip rows without coordinates

            # Build Placemark name and description similar to sample file
            name = f"Ch: {ch_start}-{ch_end} | {distress_type}"
            desc_lines = [
                f"Project: {project_name}",
                f"Distress: {distress_type}",
                f"Chainage: {ch_start} - {ch_end}",
                f"Direction: {direction}, Lane: {lane}",
                f"Length: {length} m, Area: {area} m²",
                f"Width: {width} m, Depth: {depth} m",
                f"Pavement Type: {pavement_type}",
            ]
            description = html.escape("\n".join(str(x) for x in desc_lines))

            parts.append("      <Placemark>")
            parts.append(f"        <name>{html.escape(str(name))}</name>")
            parts.append(f"        <description>{description}</description>")
            parts.append("        <styleUrl>#distressStyle</styleUrl>")
            parts.append("        <Point>")
            parts.append(f"          <coordinates>{lon},{lat},0.0</coordinates>")
            parts.append("        </Point>")
            parts.append("      </Placemark>")

        parts.append("    </Folder>")

    parts.append("  </Document>")
    parts.append("</kml>")

    kml_str = "\n".join(parts)
    return StreamingResponse(
        io.BytesIO(kml_str.encode("utf-8")),
        media_type="application/vnd.google-earth.kml+xml",
        headers={"Content-Disposition": 'attachment; filename="distress_report.kml"'},
    )


@app.post("/distress_predic_filter_kml")
def distress_predic_filter_kml(
    req: DistressFilter,
    df: pd.DataFrame = Depends(lambda: get_df("pred", settings.prediction_file)),
):
    """
    Apply the same filters as /distress_predic_filter and return a KML file
    with one Point Placemark per distress prediction (using latitude/longitude).
    """
    df_f = apply_filters(
        df,
        "distress_type",
        req.distress_type,
        req.chainage_start,
        req.chainage_end,
        req.date,
        req.direction,
        req.project_name,
    )

    if df_f.empty:
        return JSONResponse({"message": "No match"})

    def get_val(row, col, default=""):
        return row[col] if col in row and row[col] is not None else default

    parts: List[str] = []
    parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    parts.append('<kml xmlns="http://www.opengis.net/kml/2.2">')
    parts.append("  <Document>")
    parts.append("    <name>Distress Prediction</name>")
    parts.append('    <Style id="distressStyle">')
    parts.append("      <IconStyle>")
    parts.append("        <color>ffffffff</color>")
    parts.append("        <colorMode>normal</colorMode>")
    parts.append("        <scale>1.3</scale>")
    parts.append("        <Icon>")
    parts.append("          <href>http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png</href>")
    parts.append("        </Icon>")
    parts.append("      </IconStyle>")
    parts.append("    </Style>")

    for distress_type, grp in df_f.groupby("distress_type", sort=False):
        folder_name = str(distress_type)
        parts.append("    <Folder>")
        parts.append(f"      <name>{html.escape(folder_name)}</name>")

        for _, r in grp.iterrows():
            row = r.to_dict()

            ch_start = get_val(row, "chainage_start", "")
            ch_end = get_val(row, "chainage_end", "")
            direction = get_val(row, "direction", "")
            lane = get_val(row, "lane", "")
            length = get_val(row, "length", 0)
            area = get_val(row, "area", 0)
            width = get_val(row, "width", 0)
            depth = get_val(row, "depth", 0)
            pavement_type = get_val(row, "pavement_type", "")
            project_name = get_val(row, "project_name", "")

            lat = get_val(row, "latitude", None)
            lon = get_val(row, "longitude", None)
            if lat is None or lon is None:
                continue

            name = f"Ch: {ch_start}-{ch_end} | {distress_type}"
            desc_lines = [
                f"Project: {project_name}",
                f"Distress: {distress_type}",
                f"Chainage: {ch_start} - {ch_end}",
                f"Direction: {direction}, Lane: {lane}",
                f"Length: {length} m, Area: {area} m²",
                f"Width: {width} m, Depth: {depth} m",
                f"Pavement Type: {pavement_type}",
            ]
            description = html.escape("\n".join(str(x) for x in desc_lines))

            parts.append("      <Placemark>")
            parts.append(f"        <name>{html.escape(str(name))}</name>")
            parts.append(f"        <description>{description}</description>")
            parts.append("        <styleUrl>#distressStyle</styleUrl>")
            parts.append("        <Point>")
            parts.append(f"          <coordinates>{lon},{lat},0.0</coordinates>")
            parts.append("        </Point>")
            parts.append("      </Placemark>")

        parts.append("    </Folder>")

    parts.append("  </Document>")
    parts.append("</kml>")

    kml_str = "\n".join(parts)
    return StreamingResponse(
        io.BytesIO(kml_str.encode("utf-8")),
        media_type="application/vnd.google-earth.kml+xml",
        headers={"Content-Disposition": 'attachment; filename="distress_prediction.kml"'},
    )


def make_count_ep(kind: str) -> Callable:
    def ep(
        req: InventoryFilter if kind == 'inv' else DistressFilter,
        df: pd.DataFrame = Depends(lambda: get_df(
            kind,
            settings.inventory_file if kind == 'inv' else (
                settings.prediction_file if kind == 'pred' else
                settings.report_file
            )
        ))
    ):
        col = 'asset_type' if kind == 'inv' else 'distress_type'
        vals = req.asset_type if kind == 'inv' else req.distress_type
        
        df_f = apply_filters(
            df, col, vals,
            req.chainage_start,
            req.chainage_end,
            req.date,
            req.direction,
            req.project_name
        )
        
        if df_f.empty:
            return JSONResponse({"message": "No match"})
        
        grp = df_f.groupby(['latitude', 'longitude'], sort=False).first().reset_index()
        cnts = grp[col].value_counts().reset_index().values.tolist()
        
        return JSONResponse({"counts": cnts})
    
    return ep

app.post("/inventory_count")(make_count_ep('inv'))
app.post("/distress_predic_count")(make_count_ep('pred'))
app.post("/distress_report_count")(make_count_ep('rep'))

# Chainage-summary with only project & type filters
DATASETS = {
    "Inventory": settings.inventory_file,
    "Inventory2": settings.inventory2_file,
    "Distress Reported": settings.report_file,
    "Distress Reported2": settings.report2_file,
    "Distress Predicted": settings.prediction_file,
    "Distress Predicted2": settings.prediction2_file,
    "PIS": settings.pis_file,
    "PIS2": settings.pis2_file,
    "TIS": settings.tis_file,
    "TIS2": settings.tis2_file,
    "PMS": settings.pms_file,
    "PMS2": settings.pms2_file,
    "RWFIS": settings.rwfis_file,
    "RWFIS2": settings.rwfis2_file,
    "AIS":settings.ais_file,
    "AIS2":settings.ais2_file,
    "BMS":settings.bms_file
}

@app.get("/chainage-summary", response_model=Dict[str, Any])
def chainage_summary(
    project: str | None = Query(None, description="Exact project name to filter"),
    type: str | None = Query(
        None,
        pattern="^(inventory|inventory2|distress_reported|distress_reported2|distress_predicted|distress_predicted2|pis|pis2|tis|tis2|pms|pms2|rwfis|rwfis2|ais|ais2|bms)$",  #   Added pis
        description="Dataset: inventory, inventory2, distress_reported, distress_reported2, distress_predicted, distress_predicted2, pis, pis2, tis, tis2, pms, pms2, rwfis, rwfis2, ais, ais2 or bms"
    ),
):
    type_map = {
        "inventory": "Inventory",
        "inventory2": "Inventory2",
        "distress_reported": "Distress Reported",
        "distress_reported2": "Distress Reported2",
        "distress_predicted": "Distress Predicted",
        "distress_predicted2": "Distress Predicted2",
        "pis": "PIS",   
        "pis2": "PIS2", 
        "tis": "TIS",
        "tis2": "TIS2",
        "pms":"PMS",
        "pms2":"PMS2",
        "rwfis": "RWFIS",
        "rwfis2": "RWFIS2",
        "ais": "AIS",
        "ais2": "AIS2",
        "bms":"BMS"
    }
    
    targets = [type_map[type]] if type else list(DATASETS.keys())
    result: Dict[str, Any] = {}
    
    for name in targets:
        fname = DATASETS[name]
        path = os.path.join(settings.data_dir, fname)
        if not os.path.exists(path):
            continue  # Skip missing files instead of raising error
        
        # Use load_csv for proper column mapping
        try:
            df = load_csv(path)
        except Exception as e:
            print(f"Error loading {fname}: {e}")
            continue
        
        if project:
            df = df[df["project_name"] == project]
        if df.empty:
            continue
        
        proj_groups: Dict[str, Dict[str, float]] = {}
        for proj, grp in df.groupby("project_name", sort=False):
            grp_sorted = grp.sort_values("chainage_start")
            first = grp_sorted.iloc[0]
            last = grp_sorted.iloc[-1]
            
            proj_groups[proj] = {
                "Start First": float(first["chainage_start"]),
                "End First": float(first["chainage_end"]),
                "Start Last": float(last["chainage_start"]),
                "End Last": float(last["chainage_end"]),
            }
        
        if proj_groups:
            result[name] = proj_groups
    
    if not result:
        raise HTTPException(status_code=404, detail="No data matching the filters")
    
    return result



@app.get("/projects-dates/{dataset}", response_model=dict)
def get_projects_dates(
    dataset: str = Path(
        ...,
        description="Dataset: inventory, inventory2, distress_reported,distress_reported2, distress_predicted, distress_predicted2, pis, pis2, tis, tis2, pms, pms2, rwfis, rwfis2, ais, ais2 or bms"
    )
):
    """
    Return all project names in the given dataset along with unique dates for each project.
    """
    dataset_map = {
    "inventory": settings.inventory_file,
    "inventory2": settings.inventory2_file,
    "distress_reported": settings.report_file,
    "distress_reported2": settings.report2_file,
    "distress_predicted": settings.prediction_file,
    "distress_predicted2": settings.prediction2_file,
    "pis": settings.pis_file,
    "pis2": settings.pis2_file,
    "tis": settings.tis_file,
    "tis2": settings.tis2_file,
    "pms": settings.pms_file,
    "pms2": settings.pms2_file,
    "rwfis": settings.rwfis_file,
    "rwfis2": settings.rwfis2_file,
    "ais":settings.ais_file,
    "ais2":settings.ais2_file,
    "bms":settings.bms_file
    }

    if dataset not in dataset_map:
        raise HTTPException(
            status_code=400,
            detail="Invalid dataset. Choose from: inventory,inventory2, distress_reported, distress_reported2,distress_predicted, distress_predicted2, pis,pis2, tis, tis2, pms, pms2, rwfis, rwfis2, ais, ais2 or bms"
        )
    
    path = os.path.join(settings.data_dir, dataset_map[dataset])
    if not os.path.exists(path):
        raise HTTPException(
            status_code=404,
            detail=f"Dataset file not found: {dataset_map[dataset]}"
        )
    
    # Use load_csv for proper column mapping
    df = load_csv(path)
    
    if 'project_name' not in df.columns or 'date' not in df.columns:
        raise HTTPException(
            status_code=400,
            detail="Dataset must contain 'project_name' and 'date' columns"
        )
    
    # Date is already in YYYY-MM-DD format from load_csv
    df = df.dropna(subset=['project_name', 'date'])
    
    result = {}
    for project, group in df.groupby('project_name'):
        dates = sorted(group['date'].unique().tolist())
        result[project] = dates
    
    if not result:
        raise HTTPException(status_code=404, detail="No projects found in dataset")
    
    return result

@app.post("/refresh-cache-dash")
def refresh_cache():
    cache.clear()
    cache_entries.labels(cache_type='main').set(0)
    return {"message": "Cache cleared and will reload on next request"}


@app.get("/last-cron-job")
async def last_run():
    return {"last_run": last_run_timestamp}

@app.post("/append_distressReported_excel/")
def append_excel(record: DistressRecord):
    excel_operations_total.labels(operation='distress_append').inc()
    
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # --- Build columns as in your sheet (keeps same order) ---
    columns: List[str] = [
        "Latitude", "Longitude", "Chainage Start", "Chainage End", "Project Name",
        "Distress Type", "Direction", "Total Distress", "Oblique crack", "Pothole",
        "Edge Break", "Patching", "Bleeding", "Hotspots", "Alligator crack",
        "Roughness", "Repair", "Block crack", "Rutting", "Longitudinal crack",
        "Raveling", "Date", "Length", "Area", "Carriage Type", "Single discrete crack",
        "Multiple cracks", "Joint crack", "Joint seal defects", "Punchout", "Slippage",
        "Heaves", "Simple crack", "Transverse crack", "Width", "Depth", "Lane",
        "Hairline crack", "Hungry Surface", "Settlement", "Shoving", "Stripping"
    ]

    # --- Prepare row values with defaults (do NOT change sheet structure) ---
    row = [0] * len(columns)
    area = (record.Length * record.Width) if (record.Length and record.Width) else 0
    value_map = {
        "Latitude": record.Latitude,
        "Longitude": record.Longitude,
        "Chainage Start": record.Chainage_Start,
        "Chainage End": record.Chainage_End,
        "Project Name": record.Project_Name,
        "Distress Type": record.Distress_Type,
        "Direction": record.Direction,
        "Date": record.Date,
        "Length": record.Length,
        "Area": area,
        "Carriage Type": record.Carriage_Type,
        "Width": record.Width,
        "Depth": record.Depth,
        "Lane": record.Lane,
        "Total Distress": 1
    }

    # fill map values
    for i, col in enumerate(columns):
        if col in value_map:
            row[i] = value_map[col]
        elif col.strip().lower() == record.Distress_Type.strip().lower():
            # place No_of_Distress in the matching distress column (case-insensitive match)
            row[i] = record.No_of_Distress

    # --- Step 1: get usedRange and column count ---
    # --- Step 1: get usedRange and column count ---
    used_range_url = (
        f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID_DISTRESS }/items/{ITEM_ID_DISTRESS }"
        f"/workbook/worksheets('{SHEET_NAME_DISTRESS }')/usedRange?$select=address"
    )

    used_res = requests.get(used_range_url, headers=headers, timeout=30)
    if used_res.status_code != 200:
        raise HTTPException(status_code=used_res.status_code, detail=f"Failed reading usedRange: {used_res.text}")

    used_json = used_res.json()

    # We only got address, so infer column count from address range (e.g. "Sheet1!A1:AN260")
    addr = used_json.get("address", "")
    if ":" in addr:
        end = addr.split(":")[-1]
        col_letters = re.sub(r"\d", "", end)
        col_count = 0
        for ch in col_letters:
            col_count = col_count * 26 + (ord(ch.upper()) - 64)
    else:
        col_count = len(columns)

    next_row = extract_next_row_from_used_range(addr)


    # determine number of columns present in sheet:
    col_count = None
    if used_json.get("values") and isinstance(used_json["values"], list) and len(used_json["values"]) > 0:
        # number of columns is length of first row in values
        col_count = len(used_json["values"][0])
    else:
        # fallback: parse address like "A1:AN260"
        addr = used_json.get("address", "")
        if ":" in addr:
            end = addr.split(":")[-1]
            # extract column letters
            col_letters = re.sub(r"\d", "", end)
            col_count = 0
            # convert letters to index
            for ch in col_letters:
                col_count = col_count * 26 + (ord(ch.upper()) - 64)
        else:
            # fallback to the columns the code expects
            col_count = len(columns)

    # clamp row length to sheet column count (do not change sheet structure)
    if col_count < len(row):
        # if sheet has fewer columns than our expected list, shrink row to sheet size
        row = row[:col_count]
    elif col_count > len(row):
        # if sheet had extra columns, pad our row with zeros so we don't shift cells
        row = row + [0] * (col_count - len(row))

    # --- Step 2: compute next_row ---
    address_field = used_json.get("address", "")  # e.g. "Sheet1!A1:AN260"
    next_row = extract_next_row_from_used_range(address_field)

    # Build the range with the detected last column:
    last_col_letter = index_to_excel_col(col_count if col_count > 0 else len(row))
    range_address = f"A{next_row}:{last_col_letter}{next_row}"

    # --- Step 3: attempt write with retries (handle workbook locks / transient errors) ---
    write_url = (
        f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID_DISTRESS }/items/{ITEM_ID_DISTRESS }"
        f"/workbook/worksheets('{SHEET_NAME_DISTRESS }')/range(address='{range_address}')"
    )

    payload = {"values": [row]}

    max_retries = 5
    backoff = 2  # seconds (exponential growth)
    for attempt in range(1, max_retries + 1):
        try:
            res = requests.patch(write_url, headers=headers, json=payload, timeout=30)
        except requests.RequestException as e:
            # network issue — retry
            if attempt == max_retries:
                raise HTTPException(status_code=500, detail=f"Network error writing to Excel: {str(e)}")
            time.sleep(backoff ** attempt)
            continue

        # success
        if res.status_code in (200, 201, 204):
            return {"status": "success", "row_inserted_at": next_row, "columns_used": col_count}

        # workbook locked or transient server error -> retry
        body = res.text or ""
        if res.status_code in (423, 429) or "locked" in body.lower() or "generalException" in body:
            if attempt == max_retries:
                raise HTTPException(status_code=503, detail=f"Workbook write failed after retries: {res.status_code} {res.text}")
            # wait and retry
            time.sleep(backoff ** attempt)
            continue

        # other non-retriable error — return it
        raise HTTPException(status_code=res.status_code, detail=res.text)

    # if loop somehow ends without return
    raise HTTPException(status_code=500, detail="Unexpected failure writing to Excel")
# ===========================
# New Inventory Endpoint
# ===========================
@app.post("/append_inventory_excel/")
def append_inventory_excel(record: InventoryRecord):
    excel_operations_total.labels(operation='inventory_append').inc()
    
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Column structure of your Inventory Excel
    columns = [
        "Project Name", "Chainage start", "Chainage end", "Direction", "Asset type",
        "Trees", "Culvert", "Street Lights", "Bridges", "Traffic Signals", "Bus Stop",
        "Truck LayBy", "Toll Plaza", "Adjacent Road", "Toilet Blocks", "Rest Area",
        "RCC Drain", "Fuel Station", "Emergency Call Box", "Tunnels", "Footpath",
        "Junction", "Sign Boards", "Solar Blinker", "Median Plants", "Service Road",
        "KM Stones", "Crash Barrier", "Median Opening", "Latitude", "Longitude",
        "Date", "Sub Asset Type", "Carriage Type", "Lane"
    ]

    # Prepare a new row with all zeros
    row = [0] * len(columns)

    # Map user input values
    value_map = {
        "Project Name": record.Project_Name,
        "Chainage start": record.Chainage_start,
        "Chainage end": record.Chainage_end,
        "Direction": record.Direction,
        "Asset type": record.Asset_type,
        "Latitude": record.Latitude,
        "Longitude": record.Longitude,
        "Date": record.Date,
        "Sub Asset Type": record.Sub_Asset_Type,
        "Carriage Type": record.Carriage_Type,
        "Lane": record.Lane,
    }

    # Fill input fields
    for i, col in enumerate(columns):
        if col in value_map:
            row[i] = value_map[col]
        elif col.strip().lower() == record.Asset_type.strip().lower():
            row[i] = record.No_of_inventories  # match asset column like "trees"

    # --- Step 1: Get usedRange ---
    used_url = (
        f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID_INVENTORY}/items/{ITEM_ID_INVENTORY}"
        f"/workbook/worksheets('{SHEET_NAME_INVENTORY}')/usedRange?$select=address"
    )
    used_res = requests.get(used_url, headers=headers, timeout=30)
    if used_res.status_code != 200:
        raise HTTPException(status_code=used_res.status_code, detail=f"usedRange error: {used_res.text}")
    used_json = used_res.json()
    addr = used_json.get("address", "")
    next_row = extract_next_row_from_used_range(addr)

    # Detect number of columns in Excel
    end = addr.split(":")[-1] if ":" in addr else "AJ1"
    col_letters = re.sub(r"\d", "", end)
    col_count = sum((ord(ch.upper()) - 64) * (26 ** i) for i, ch in enumerate(reversed(col_letters)))
    last_col_letter = index_to_excel_col(col_count)

    # Trim/pad row
    if len(row) < col_count:
        row += [0] * (col_count - len(row))
    elif len(row) > col_count:
        row = row[:col_count]

    # --- Step 2: Write the new row ---
    range_address = f"A{next_row}:{last_col_letter}{next_row}"
    write_url = (
        f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID_INVENTORY}/items/{ITEM_ID_INVENTORY}"
        f"/workbook/worksheets('{SHEET_NAME_INVENTORY}')/range(address='{range_address}')"
    )
    payload = {"values": [row]}

    max_retries = 5
    backoff = 2
    for attempt in range(1, max_retries + 1):
        res = requests.patch(write_url, headers=headers, json=payload, timeout=30)
        if res.status_code in (200, 201, 204):
            return {"status": "success", "inserted_row": next_row, "columns_used": col_count}
        if res.status_code in (423, 429) or "locked" in res.text.lower():
            time.sleep(backoff ** attempt)
            continue
        raise HTTPException(status_code=res.status_code, detail=res.text)

    raise HTTPException(status_code=500, detail="Excel write failed after retries")

# ----------------------------------------------------
# MAIN ENDPOINT — PROCESS IMAGE For DistressAnalyzer
# ----------------------------------------------------
@app.post("/process_image")
async def process_image(file: UploadFile = File(...)):
 
    global SESSION_MAIN_EXCEL, SESSION_MAIN_NAME
 
    try:
        fname = file.filename
 
        if not validate_filename(fname):
            errors_total.labels(error_type='invalid_filename').inc()
            return {"status": "error",
                    "message": "Invalid filename. Must end in _RHS_L1, _RHS_L2, _LHS_L1, _LHS_L2"}
 
        image_bytes = await file.read()
        
        # Track image processing
        images_processed_total.labels(operation_type='process_image').inc()
 
        df_main = process_image_generate_excels_memory(image_bytes, fname)
 
        SESSION_MAIN_EXCEL = df_main
        SESSION_MAIN_NAME = fname
 
        dist_col = df_main["Distance_meter"].to_numpy()
        matrix = df_main.iloc[:, 1:].to_numpy()
        is_L2 = "L2" in fname.upper()
 
        results_df = run_classification(matrix, dist_col, is_L2)
 
        grouped_output = {"Crack": {}, "Pothole": {}}
 
        for cls in results_df["Classification"].unique():
 
            cls_df = results_df[results_df["Classification"] == cls]
 
            grouped_output["Pothole" if "pothole" in cls.lower() else "Crack"][cls] = {
                "count": len(cls_df),
                "total_length_m": float(cls_df["Length(M)"].sum()),
                "total_width_mm": float(cls_df["Width(MM)"].sum())
            }
 
        return {
            "status": "success",
            "file_has_L2": is_L2,
            "summary": grouped_output,
            "detailed_results": results_df.to_dict(orient="records")
        }
 
    except Exception as e:
        errors_total.labels(error_type='process_image_error').inc()
        return {"status": "error", "message": str(e)}
 
 
# ----------------------------------------------------
# SEPARATE ENDPOINT — RETURN MAIN EXCEL DATA
# ----------------------------------------------------
@app.get("/read_main")
def read_main(image_name: str):
    global SESSION_MAIN_EXCEL, SESSION_MAIN_NAME
 
    if SESSION_MAIN_EXCEL is None:
        return {"status": "error", "message": "No image processed in this session"}
 
    req = image_name.rsplit(".", 1)[0].lower()
    last = SESSION_MAIN_NAME.rsplit(".", 1)[0].lower()
 
    if req != last:
        return {
            "status": "error",
            "message": f"Requested image '{image_name}' does not match last processed: '{SESSION_MAIN_NAME}'"
        }
 
    return {
        "status": "success",
        "rows": len(SESSION_MAIN_EXCEL),
        "columns": list(SESSION_MAIN_EXCEL.columns),
        "data": SESSION_MAIN_EXCEL.to_dict(orient="records")
    }

@app.post("/process_imagevh")
async def process_image(file: UploadFile = File(...)):
 
    global SESSION_MAIN_EXCEL, SESSION_MAIN_NAME
 
    try:
        fname = file.filename
 
        if not validate_filename(fname):
            return {
                "status": "error",
                "message": "Invalid filename. Must follow: *_RHS_L1.jpg, *_LHS_L2.jpg"
            }
 
        image_bytes = await file.read()
 
        df_main = generate_combined_matrix1(image_bytes, fname)
 
        SESSION_MAIN_EXCEL = df_main
        SESSION_MAIN_NAME = fname
 
        dist_col = df_main["Distance_meter"].to_numpy()
        matrix = df_main.iloc[:, 1:].to_numpy()
        matrix = np.nan_to_num(matrix, nan=0)
 
        is_L2 = "L2" in fname.upper()
 
        results_df = run_classification1(matrix, dist_col, is_L2)
 
        grouped_output = {"Crack": {}, "Pothole": {}}
 
        for cls in results_df["Classification"].unique():
            cls_df = results_df[results_df["Classification"] == cls]
 
            grouped_output[
                "Pothole" if "pothole" in cls.lower() else "Crack"
            ][cls] = {
                "count": len(cls_df),
                "total_length_m": float(cls_df["Length(M)"].sum()),
                "total_width_mm": float(cls_df["Width(MM)"].sum())
            }
 
        return {
            "status": "success",
            "file_has_L2": is_L2,
            "summary": grouped_output,
            "detailed_results": results_df.to_dict(orient="records")
        }
 
    except Exception as e:
        return {"status": "error", "message": str(e)}
 
 
# =====================================================
# READ MAIN MATRIX
# =====================================================
@app.get("/read_mainvh")
def read_main(image_name: str):
 
    global SESSION_MAIN_EXCEL, SESSION_MAIN_NAME
 
    if SESSION_MAIN_EXCEL is None:
        return {"status": "error", "message": "No image processed yet"}
 
    req = image_name.rsplit(".", 1)[0].lower()
    last = SESSION_MAIN_NAME.rsplit(".", 1)[0].lower()
 
    if req != last:
        return {
            "status": "error",
            "message": f"Requested image '{image_name}' does not match last processed '{SESSION_MAIN_NAME}'"
        }
 
    return {
        "status": "success",
        "rows": len(SESSION_MAIN_EXCEL),
        "columns": list(SESSION_MAIN_EXCEL.columns),
        "data": SESSION_MAIN_EXCEL.to_dict(orient="records")
    }
 
# =====================================================
@app.post("/enter_image")
async def process_image(file: UploadFile = File(...)):
    fname = file.filename
    if not validate_filename(fname):
        return {"status":"error","message":"Invalid filename"}
 
    base = fname.rsplit(".",1)[0]
    img_bytes = await file.read()
 
    hori, vert, hori_main, vert_main = process_image_hv(img_bytes, fname)
    complete, partial = analyze_grids(vert)
    pattern = build_pattern(vert, complete, partial)
    viz = build_visualization(pattern, vert_main)
 
    dist = hori_main["Distance_meter"].to_numpy()
    matrix = hori_main.iloc[:,1:].to_numpy()
    is_L2 = "L2" in fname.upper()
    class_df = run_classification(matrix, dist, is_L2)
 
    grouped = {"Crack":{}, "Pothole":{}}
    if not class_df.empty:
        for cls in class_df["Classification"].unique():
            dfc = class_df[class_df["Classification"]==cls]
            grouped["Pothole" if "pothole" in cls.lower() else "Crack"][cls] = {
                "count": int(len(dfc)),
                "total_length_m": float(dfc["Length(M)"].sum()),
                "total_width_mm": float(dfc["Width(MM)"].sum())
            }
 
    PIPELINE[base] = {
        "raw":{"horizontal":hori, "vertical":vert},
        "main":{"horizontal":hori_main, "vertical":vert_main},
        "pattern":pattern,
        "visualization":viz,
        "classification":class_df
    }
 
    return {
        "status":"success",
        "file":fname,
        "file_has_L2":is_L2,
        "summary":grouped,
        "detailed_results": df_to_json_safe(class_df)
    }
 #==========================PMS Roughness=========================================#

# =====================================================
# IN-MEMORY CACHE (NO FILESYSTEM)
# =====================================================
CACHE = {
    "images": [],     # (filename, bytes)
    "profiles": [],   # (filename, DataFrame)
    "ri": []          # (filename, DataFrame)
}

# =====================================================
# CACHE RESET (CRITICAL)
# =====================================================
def reset_cache():
    CACHE["images"].clear()
    CACHE["profiles"].clear()
    CACHE["ri"].clear()
 
# =====================================================
# PARAMETERS (SAME AS YOUR LOGIC)
# =====================================================
KNOWN_DISTANCE = 100.0
GRAY_THRESHOLD = 200
LINE_SPACING_M = 0.10
 
# =====================================================
# CHAINAGE FROM FILENAME
# =====================================================
def extract_chainage_from_filename(filename: str) -> Tuple[float, float]:
    match = re.search(r"(\d+(?:\.\d+)?)\s*(?:_to_|-)\s*(\d+(?:\.\d+)?)", filename)
    if not match:
        raise ValueError(f"Invalid filename (chainage not found): {filename}")
 
    start = float(match.group(1))
    end = float(match.group(2))
 
    if end <= start:
        raise ValueError("Chainage end must be greater than start")
 
    return start, end
 
# =====================================================
# IMAGE → PROFILE DATAFRAME
# =====================================================
def image_to_profile_df(image_bytes: bytes) -> pd.DataFrame:
    img = cv2.imdecode(np.frombuffer(image_bytes, np.uint8), cv2.IMREAD_GRAYSCALE)
    if img is None:
        raise ValueError("Invalid image")
 
    img = cv2.normalize(img, None, 0, 255, cv2.NORM_MINMAX)
 
    scale = KNOWN_DISTANCE / img.shape[1]
    pixel_gap = max(int(round(LINE_SPACING_M / scale)), 1)
 
    lines = list(range(0, img.shape[0], pixel_gap))
    if lines[-1] != img.shape[0] - 1:
        lines.append(img.shape[0] - 1)
 
    profiles = {}
    distance_ref = None
 
    for i, y in enumerate(lines):
        profile = img[y, :].astype(float)
        profile[profile > GRAY_THRESHOLD] = 0
        distance = np.arange(len(profile)) * scale
 
        profiles[f"L{i+1}_gray"] = profile
        if distance_ref is None:
            distance_ref = distance
 
    df = pd.DataFrame(profiles)
    df.insert(0, "Distance_meter", distance_ref)
    return df
 
# =====================================================
# PROFILE → RI / IRI DATAFRAME
# =====================================================
def profile_to_ri_df(
    df: pd.DataFrame,
    chainage_start: float,
    chainage_end: float
) -> pd.DataFrame:
 
    gray_cols = [c for c in df.columns if "gray" in c.lower()]
 
    df["Average_gray"] = df[gray_cols].apply(
        lambda r: np.mean([v for v in r if v > 0]) if any(r > 0) else 0,
        axis=1
    )
 
    offsets = np.arange(0, 100.25, 0.25)
    interp = np.interp(offsets, df["Distance_meter"], df["Average_gray"])
 
    rows = []
    segment_length = (chainage_end - chainage_start) / 10
    current = chainage_start
 
    for start in range(0, 100, 10):
        block = interp[(offsets >= start) & (offsets < start + 10)]
        ri = block.sum() / 2 if len(block) else 0.0
        iri = (ri / 630) ** (1 / 1.12) if ri > 0 else 0.0
 
        rows.append({
            "Chainage_Start": round(current, 3),
            "Chainage_End": round(current + segment_length, 3),
            "RI": round(float(ri), 3),
            "IRI": round(float(iri), 3)
        })
 
        current += segment_length
 
    return pd.DataFrame(rows)
# =====================================================
# 1️⃣ UPLOAD IMAGES
# =====================================================
# =====================================================
# 1️⃣ UPLOAD IMAGES (FULL RESET GUARANTEED)
# =====================================================
@app.post("/upload-images")
async def upload_images(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(400, "No files provided")
 
    # 🔥 CLEAR ALL PREVIOUS DATA FIRST
    reset_cache()
    
    images_processed_total.labels(operation_type='upload_images').inc()
 
    for f in files:
        content = await f.read()
        if not content:
            raise HTTPException(
                status_code=400,
                detail=f"Empty file detected: {f.filename}"
            )
        CACHE["images"].append((f.filename, content))
    
    cache_entries.labels(cache_type='images').set(len(CACHE["images"]))
 
    return {
        "status": "Previous data cleared. New images uploaded successfully.",
        "image_count": len(CACHE["images"])
    }
 
# =====================================================
# 2️⃣ PROFILE GENERATION (METADATA ONLY)
# =====================================================
@app.get("/profiles-data")
async def profiles_data():
    if not CACHE["images"]:
        raise HTTPException(400, "No images uploaded")
 
    CACHE["profiles"].clear()
    summary = {}
 
    for name, img_bytes in CACHE["images"]:
        df = await run_in_threadpool(image_to_profile_df, img_bytes)
        CACHE["profiles"].append((name, df))
        summary[name] = {
            "rows": len(df),
            "columns": list(df.columns)
        }
 
    return {"type": "profile_data", "summary": summary}
 
# =====================================================
# 3️⃣ RI / IRI DATA
# =====================================================
@app.get("/ri-iri-data")
def ri_iri_data():
    if not CACHE["profiles"]:
        raise HTTPException(400, "Profile data not generated")
 
    CACHE["ri"].clear()
    result = {}
 
    for name, df in CACHE["profiles"]:
        start_ch, end_ch = extract_chainage_from_filename(name)
        ri_df = profile_to_ri_df(df, start_ch, end_ch)
        CACHE["ri"].append((name, ri_df))
        result[name] = ri_df.to_dict(orient="records")
 
    return {"type": "ri_iri_data", "data": result}
 
# =====================================================
# 4️⃣ MASTER DATA
# =====================================================
@app.get("/master-data")
def master_data():
    if not CACHE["ri"]:
        raise HTTPException(400, "RI data not generated")
 
    rows = []
    for name, df in CACHE["ri"]:
        temp = df.copy()
        temp["Source_Image"] = name
        rows.extend(temp.to_dict(orient="records"))
 
    return {
        "type": "master_data",
        "total_records": len(rows),
        "rows": rows
    }
 
# =====================================================
# 5️⃣ PROFILE EXCEL
# =====================================================
@app.get("/profiles-excel")
def profiles_excel():
    if not CACHE["profiles"]:
        raise HTTPException(400, "Profile data not generated")
 
    name, df = CACHE["profiles"][0]
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
 
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={name}_profiles.xlsx"}
    )
 
# =====================================================
# 6️⃣ RI / IRI EXCEL
# =====================================================
@app.get("/ri-iri-excel")
def ri_iri_excel():
    if not CACHE["ri"]:
        raise HTTPException(400, "RI data not generated")
 
    name, df = CACHE["ri"][0]
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
 
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={name}_RI_IRI.xlsx"}
    )
 
# =====================================================
# 7️⃣ MASTER EXCEL
# =====================================================
@app.get("/master-excel")
def master_excel():
    if not CACHE["ri"]:
        raise HTTPException(400, "RI data not generated")
 
    dfs = []
    for name, df in CACHE["ri"]:
        temp = df.copy()
        temp["Source_Image"] = name
        dfs.append(temp)
 
    master = pd.concat(dfs, ignore_index=True)
 
    buf = io.BytesIO()
    master.to_excel(buf, index=False)
    buf.seek(0)
 
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=MASTER_RI_IRI.xlsx"}
    )
 
# =====================================================
@app.get("/get/raw/{name}")
def get_raw(name:str):
    d=fetch(name)
    if not d: return {"status":"error"}
    return {
        "horizontal":df_to_json_safe(d["raw"]["horizontal"]),
        "vertical":df_to_json_safe(d["raw"]["vertical"])
    }
 
@app.get("/get/main/{name}")
def get_main(name:str):
    d=fetch(name)
    if not d: return {"status":"error"}
    return {
        "horizontal":df_to_json_safe(d["main"]["horizontal"]),
        "vertical":df_to_json_safe(d["main"]["vertical"])
    }

@app.get("/get/pattern/{name}")
def get_pattern(name: str):
    d = fetch(name)
    if not d: return {"status":"error"}
    return {"status":"success","data": df_to_json_safe(d["pattern"])}
 
@app.get("/get/visualization/{name}")
def get_visualization(name: str):
    d = fetch(name)
    if not d: return {"status":"error"}
    return {"status":"success","data": df_to_json_safe(d["visualization"])}
