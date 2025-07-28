#!/usr/bin/env python3
"""
───────────────────────────────────────────────────────────────────────────────
fusionv2.py  –  “CO₂ × Heart-Rate” daily join
───────────────────────────────────────────────────────────────────────────────

GOAL
────
Turn two independent sensor streams …

    1. co2_*.json          ← SCD-30 logger (one reading / 2 s)
    2. hr_*.csv            ← Health-ingest “flat” file (one row / beat)

… into one tidy, same-clock dataframe so that Streamlit (or a notebook)
can plot CO₂ ppm and HR (BPM) on the same timeline and run correlations.

PIPELINE
────────
1. **Locate latest files**
   • newest   ~/data/co2/co2_YYYY-MM-DDThh-mm-ss.json
   • newest   ~/biologger/data/flat/hr_YYYY-MM-DD.csv

2. **Load CO₂ with a tolerant JSON reader**
   The logger saves a *JSON array* that is being appended to while the
   file is open. We therefore stream line-by-line, strip “[ … , ]”
   brackets and ignore the occasionally half-written last line.

3. **Time-normalise the CO₂ series**
   – Convert the sensor’s RFC 3339 timestamp → UTC-naïve pandas datetime  
   – Resample to *exact* 60-second means (`resample("60s").mean()`)

4. **Load the flat HR file**
   Each row is already tidy:  
   `timestamp, hr_bpm  (and possibly skin_temp, spo2 … later)`

5. **Merge**  
   `pd.merge_asof` (nearest neighbour) with ±90 s tolerance so the
   inevitable clock skew between Pi and iPhone doesn’t drop matches.

6. **Write fused_<date>.csv** to ~/biologger/  
   Every run overwrites the same-day file; Streamlit just re-reads it.

RESULT
──────
A CSV like:

    timestamp,co2_ppm,temp_c,humidity_pct,hr_bpm
    2025-07-28 01:00:00,  615.4,24.11,48.7,    72
    2025-07-28 01:01:00,  614.9,24.10,48.8,    73
    …

If no HR rows fall within 90 s the `hr_bpm` cell is NaN; the final
printout reports how many CO₂ minutes found a matching beat.

USAGE
─────
    chmod +x fusionv2.py
    ./fusionv2.py            # or  python3 fusionv2.py

Make it a cron-job (`@hourly`) or call it from the Streamlit refresh
callback—whatever fits your demo flow.

DEPENDENCIES
────────────
Only **pandas** + the standard library; runs inside the same venv as the
logger/ingest.

───────────────────────────────────────────────────────────────────────────────
"""

#!/usr/bin/env python3
"""
Fuse latest CO₂ log with latest *flat* heart-rate file.

 • Reads newest  co2_*.json   in  ~/data/co2/
 • Down-samples CO₂ to 1-minute means
 • Reads newest  hr_*.csv     in  ~/biologger/data/flat/
 • Timestamp-merges (nearest ≤90 s)
 • Writes       fused_YYYY-MM-DD.csv  in  ~/biologger/
"""

import json, pathlib, datetime as dt
import pandas as pd

# ── paths ────────────────────────────────────────────────────────────────────
CO2_DIR  = pathlib.Path("~/data/co2").expanduser()
FLAT_DIR = pathlib.Path("~/biologger/data/flat").expanduser()
OUT_DIR  = pathlib.Path("~/biologger").expanduser()

# ── newest CO₂ file ──────────────────────────────────────────────────────────
co2_path = max(CO2_DIR.glob("co2_*.json"), key=lambda p: p.stat().st_mtime)

records = []
with co2_path.open() as f:
    for line in f:
        line = line.strip()
        if line in ("[", "]", ""):
            continue                      # skip JSON-array brackets & blanks
        if line.endswith(","):
            line = line[:-1]              # drop trailing comma
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            pass                         # ignore incomplete last line

co2 = pd.DataFrame.from_records(records)
co2["timestamp"] = pd.to_datetime(co2["timestamp"], utc=True).dt.tz_convert(None)

co2_1m = (co2.set_index("timestamp")
             .resample("60s").mean()
             .dropna()
             .reset_index())

# ── newest flat HR file ──────────────────────────────────────────────────────
hr_path = max(FLAT_DIR.glob("hr_*.csv"), key=lambda p: p.stat().st_mtime)
hr      = pd.read_csv(hr_path)
hr["timestamp"] = pd.to_datetime(hr["timestamp"])

# ── merge (≤ 90 s) ───────────────────────────────────────────────────────────
merged = pd.merge_asof(
    co2_1m.sort_values("timestamp"),
    hr.sort_values("timestamp"),
    on="timestamp",
    direction="nearest",
    tolerance=pd.Timedelta("90s"),
)

# ── write out ────────────────────────────────────────────────────────────────
out = OUT_DIR / f"fused_{dt.date.today()}.csv"
merged.to_csv(out, index=False)

print(f"✅  Wrote {out}  |  rows: {len(merged):,}  |  HR matches: "
      f"{merged['hr_bpm'].notna().sum():,}")
