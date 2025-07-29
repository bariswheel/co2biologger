#!/usr/bin/env python3
"""
fusionv2.py  –  “squeeze-everything” CO₂ × HR fusion

Why this version?
• Parses the raw Health-ingest blobs (~/biologger/data/raw) to grab EVERY
  heart-rate sample, not just the flattened beats you created later.
• Accepts smart quotes, missing zones, Avg/Min/Max or plain 'value'.
• ±10-minute merge tolerance → maximises HR matches (demo-friendly).

Outputs:  ~/biologger/fused_<YYYY-MM-DD>.csv   (overwrites per run)
Columns:   timestamp,co2_ppm,temp_c,humidity_pct,hr_bpm
"""

import json, re, ast, pathlib, datetime as dt
import pandas as pd

# ── paths ────────────────────────────────────────────────────────────────
CO2_DIR  = pathlib.Path("~/data/co2").expanduser()
RAW_DIR  = pathlib.Path("~/biologger/data/raw").expanduser()
OUT_DIR  = pathlib.Path("~/biologger").expanduser()
today    = dt.date.today().isoformat()

# ── helper to fix the single-quoted JSON blobs ───────────────────────────
def safe_load(raw: str) -> dict:
    # strip outer quotes if CSV quoted
    raw = raw.strip('"')
    # smart → straight apostrophes
    raw = raw.replace("’", "'").replace("‘", "'")
    # None/True/False → lowercase for JSON
    raw = raw.replace("None", "null").replace("True", "true").replace("False", "false")
    # single → double quotes
    raw = re.sub(r"'", '"', raw)
    return json.loads(raw)

# ── 1) load ALL CO₂ JSONs for today ──────────────────────────────────────
records = []
for p in sorted(CO2_DIR.glob(f"co2_{today}*.json")):
    with p.open() as f:
        for line in f:
            line = line.strip().rstrip(",")
            if line in ("[", "]", ""):
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass

if not records:
    print(f"⚠ No CO₂ records for {today}")
    exit()

co2 = pd.DataFrame.from_records(records)
co2["timestamp"] = (
    pd.to_datetime(co2["timestamp"], format="ISO8601", utc=True, errors="coerce")
      .dt.tz_convert(None)
)
co2_1m = (
    co2.set_index("timestamp")
       .resample("60s").mean()
       .dropna()
       .reset_index()
)

# ── 2) explode raw biomarker CSVs into per-beat rows ────────────────────
beat_rows = []
for p in RAW_DIR.glob(f"bio_{today}.csv"):
    for blob in pd.read_csv(p)["data"]:
        try:
            j = safe_load(blob)
        except Exception:
            continue
        for m in j.get("metrics", []):
            if m.get("name") != "heart_rate":
                continue
            for d in m.get("data", []):
                ts = pd.to_datetime(d.get("date"), utc=True, errors="coerce")
                if pd.isna(ts):
                    continue
                beat_rows.append(
                    {
                        "timestamp": ts.tz_convert(None),
                        "hr_bpm": d.get("Avg", d.get("value")
                                        or d.get("Min") or d.get("Max")),
                    }
                )

if not beat_rows:
    print(f"⚠ No heart-rate beats found for {today}")
    exit()

hr = pd.DataFrame(beat_rows).dropna().sort_values("timestamp")

# ── 3) merge with ±10 minutes tolerance (closest) ───────────────────────
merged = pd.merge_asof(
    co2_1m.sort_values("timestamp"),
    hr.sort_values("timestamp"),
    on="timestamp",
    direction="nearest",
    tolerance=pd.Timedelta("600s"),   # 10 minutes
)

# ── 4) write fused CSV ──────────────────────────────────────────────────
out = OUT_DIR / f"fused_{today}.csv"
merged.to_csv(out, index=False)
print(
    f"✅  Wrote {out}\n"
    f"   CO₂ minutes: {len(co2_1m):,}\n"
    f"   HR samples : {len(hr):,}\n"
    f"   Fused rows : {len(merged):,}\n"
    f"   HR matches : {merged['hr_bpm'].notna().sum():,}"
)
