"""
──────────────────────────────────────────────────────────────────────────────
Health-ingest API  –  what it does

• Listens on  ➜  POST /health  (FastAPI + Uvicorn, port 5000).  
  ⟶ The iPhone’s Health Auto Export sends a JSON payload here every minute.

• For every POST it performs **two writes**:

  1.  A full archive copy of the raw blob
      ->  ~/biologger/data/raw/bio_YYYY-MM-DD.csv
      (one CSV row per POST, columns: data , time)

  2.  A *flattened* one-row-per-heartbeat version
      ->  ~/biologger/data/flat/hr_YYYY-MM-DD.csv  
      Columns: timestamp , hr_bpm , source , context  
      Only written when heart-rate metrics are present.

•  The flat folder is what `fusionv2.py` reads.  
   The raw folder lets you go back and replay / re-parse if needed.

Directory tree after a day of running
└── ~/biologger/data/
    ├── raw/    ← big blobs for safekeeping
    │   └── bio_2025-07-28.csv
    └── flat/   ← tidy rows for analytics
        └── hr_2025-07-28.csv

Return value to the phone:  {"status":"ok","rows_written":N}

────────────────── Quick reference ──────────────────
HEAD /health          →  405 (intentionally; only POST is allowed)
POST /health (JSON)   →  200 OK
        body: {
          "metrics":[
            {"name":"heart_rate", "data":[ … ]},
            …
          ]
        }
──────────────────────────────────────────────────────
"""


#!/usr/bin/env python3
"""
Fuse latest CO₂ log with latest biomarker file.

*  Reads the newest JSON in ~/data/co2/
*  Down‑samples CO₂ to 1‑minute averages
*  Reads the newest bio_*.csv in ~/biologger/data/
*  Merges on timestamp (nearest, ≤ 90 s)
*  Writes fused_YYYY‑MM‑DD.csv next to the biomarker folder

Run:  ./fusionv2.py   or   python3 fusionv2.py
"""

import json
import pandas as pd
import pathlib
import datetime as dt

# — paths —
CO2_DIR  = pathlib.Path("~/data/co2").expanduser()
BIO_DIR  = pathlib.Path("~/biologger/data").expanduser()
OUT_DIR  = BIO_DIR.parent  # ~/biologger/

# — locate newest CO2 file —
co2_path = max(CO2_DIR.glob("co2_*.json"), key=lambda p: p.stat().st_mtime)

# — tolerant JSON loader (array with trailing commas) —
records: list[dict] = []
with co2_path.open() as f:
    for line in f:
        line = line.strip()
        if line in ("[", "]", ""):
            continue
        if line.endswith(","):
            line = line[:-1]
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            # skip any incomplete final line
            pass

co2 = pd.DataFrame.from_records(records)
co2["timestamp"] = pd.to_datetime(co2["timestamp"], utc=True).dt.tz_convert(None)

# — 1‑minute averaging —
co2_1m = (co2.set_index("timestamp")
             .resample("60S").mean()
             .dropna()
             .reset_index())

# — locate newest biomarker CSV —
bio_path = max(BIO_DIR.glob("bio_*.csv"), key=lambda p: p.stat().st_mtime)
bio = pd.read_csv(bio_path)
bio["time"] = pd.to_datetime(bio["time"], utc=True).dt.tz_convert(None)


# — select heart-rate rows —
key = "type" if "type" in bio.columns else "measurementType"
hr = (bio[bio[key] == "HeartRate"]
        .rename(columns={"value": "hr_bpm"}))

if hr.empty:
    print("⚠ No HeartRate rows found in", bio_path.name)
    exit()


# — merge with 90‑second tolerance —
merged = pd.merge_asof(
    co2_1m.sort_values("timestamp"),
    hr[["time", "hr_bpm"]].rename(columns={"time": "timestamp"})
      .sort_values("timestamp"),
    on="timestamp",
    direction="nearest",
    tolerance=pd.Timedelta("90s"),
)

# — write output —
out_path = OUT_DIR / f"fused_{dt.date.today()}.csv"
merged.to_csv(out_path, index=False)

print(
    f"✅  Wrote {out_path}  |  rows: {len(merged):,}  |  HR matches: {merged['hr_bpm'].notna().sum():,}"
)
