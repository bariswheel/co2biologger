#!/usr/bin/env python3
"""
fusionv3.py — multi-day CO₂ × Heart-Rate fuser                     🌕 2025-07-29
───────────────────────────────────────────────────────────────────────────────
WHAT
====
Merge every minute of indoor CO₂ with every minute of heart-rate (HR) into a
single tidy CSV **per day**:

    timestamp , co2_ppm , temp_c , humidity_pct , hr_bpm , source , context

WHY
===
Makes it trivial to resample (e.g. 10-min means) and visualise in Streamlit or
Jupyter for air-quality / wellbeing insights.

HOW
===
1.  Lift csv’s 128 kB field limit (HR rows are huge).
2.  For each `bio_YYYY-MM-DD.csv` in  `~/biologger/data/raw/`
       • parse its “data” blob → rows (timestamp, hr_bpm, …)  
       • cache to `~/biologger/data/flat/hr_YYYY-MM-DD.csv`
3.  **Collect all** `co2_YYYY-MM-DDT*.json` in `~/data/co2/`, concat, resample
    to 1-minute means.
4.  `merge_asof` tables with ±3 min tolerance.
5.  Write `~/biologger/data/fused/fused_YYYY-MM-DD.csv`.
6.  Print a quick report + first 50 rows (all) + first 50 rows *with* HR.

--------------------------------------------------------------------------"""

import ast, csv, json, sys, pathlib
import pandas as pd

# ── directories ──────────────────────────────────────────────────────
RAW_DIR   = pathlib.Path("~/biologger/data/raw").expanduser()
FLAT_DIR  = pathlib.Path("~/biologger/data/flat").expanduser()
CO2_DIR   = pathlib.Path("~/data/co2").expanduser()
FUSED_DIR = pathlib.Path("~/biologger/data/fused").expanduser()
for d in (FLAT_DIR, FUSED_DIR):
    d.mkdir(parents=True, exist_ok=True)

csv.field_size_limit(sys.maxsize)          # allow multi-MB fields

# ── helper: flatten one raw HR CSV ───────────────────────────────────
def flatten_hr(raw_csv: pathlib.Path) -> pathlib.Path:
    day = raw_csv.stem.split("_")[-1]               # YYYY-MM-DD
    flat_csv = FLAT_DIR / f"hr_{day}.csv"

    rows = []
    with raw_csv.open(newline="") as fh:
        rdr = csv.DictReader(fh)
        for rec in rdr:
            try:
                payload = ast.literal_eval(rec["data"])
                for m in payload["data"]["metrics"]:
                    if m.get("name") != "heart_rate":
                        continue
                    for beat in m["data"]:
                        rows.append(
                            (beat["date"],
                             beat.get("Avg", beat.get("value")),
                             beat.get("source"),
                             beat.get("context"))
                        )
            except Exception:
                continue  # skip malformed row

    if rows:
        pd.DataFrame(rows,
                     columns=["timestamp", "hr_bpm", "source", "context"]
                     ).to_csv(flat_csv, index=False)
    return flat_csv

# ── helper: load all CO₂ JSONs for a day ─────────────────────────────
def load_day_co2(day: str) -> pd.DataFrame:
    records = []
    for jp in sorted(CO2_DIR.glob(f"co2_{day}T*.json")):
        with jp.open() as fh:
            for ln in fh:
                ln = ln.strip().rstrip(",")
                if ln in ("", "[", "]"):
                    continue
                try:
                    records.append(json.loads(ln))
                except json.JSONDecodeError:
                    continue  # ignore junk lines
    if not records:
        return pd.DataFrame()

    return (
        pd.DataFrame.from_records(records)
          .assign(
              # parse both offsetted & naïve ISO strings; coerce bad ones to NaT
              timestamp=lambda df:
                  pd.to_datetime(
                      df["timestamp"],
                      errors="coerce",          # don’t explode on weird lines
                      utc=True,                 # attach UTC if offset supplied
                      format="ISO8601"          # pandas ≥1.4: mixed ISO support
                  ).dt.tz_convert(None)         # strip timezone → naïve dtype
          )
          .dropna(subset=["timestamp"])
          .set_index("timestamp")
          .resample("60s").mean()
          .dropna()
          .reset_index()
    )
# ── main loop ────────────────────────────────────────────────────────
for raw_csv in sorted(RAW_DIR.glob("bio_*.csv"), key=lambda p: p.stat().st_mtime):
    day = raw_csv.stem.split("_")[-1]                # YYYY-MM-DD
    flat_csv = flatten_hr(raw_csv)

    if not flat_csv.exists():
        print(f"{raw_csv.name}: no HR rows → skip\n")
        continue

    hr = (pd.read_csv(flat_csv)
            .assign(timestamp=lambda df:
                    pd.to_datetime(df["timestamp"], utc=True)
                      .dt.tz_convert(None))
            .dropna(subset=["hr_bpm"]))

    if hr.empty:
        print(f"{raw_csv.name}: no HR rows → skip\n")
        continue

    co2 = load_day_co2(day)
    if co2.empty:
        print(f"▲  no CO₂ logs for {day} → skip\n")
        continue

    merged = pd.merge_asof(
        co2.sort_values("timestamp"),
        hr.sort_values("timestamp"),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta("3min")
    )

    out = FUSED_DIR / f"fused_{day}.csv"
    merged.to_csv(out, index=False)

    # ── report ───────────────────────────────────────────────────────
    print(f"\n✅  Wrote {out}")
    print(f"   CO₂ minutes: {len(co2):>6,}")
    print(f"   HR  samples : {len(hr):>6,}")
    print(f"   Fused rows : {len(merged):>6,}")
    print(f"   HR matches : {merged['hr_bpm'].notna().sum():>6,}\n")

    pd.set_option("display.width", 120)
    print("── First 50 rows ──")
    print(merged.head(50).to_string(index=False))

    with_hr = merged[merged["hr_bpm"].notna()].head(50)
    if not with_hr.empty:
        print("\n── First 50 rows *with HR* ──")
        print(with_hr.to_string(index=False))
    print("\n" + "═"*80 + "\n")
