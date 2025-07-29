#!/usr/bin/env python3
"""
fusionv3.py ── multi-day CO₂ × Heart-Rate fuser                         🌕 2025-07-29

WHAT & WHY
==========

Indoor CO₂ comes in many-line JSON logs (one file/hour) while Health-Auto-Export
drops *one* enormous CSV per day whose **data** column is itself a Python-style
dict-string with Avg / Min / Max heart-rate for that minute.

We want one tidy table per day:

    timestamp , co2_ppm , temp_c , humidity_pct , hr_bpm , source , context

so that Streamlit (or anything else) can down-sample further and plot.

HOW
====

1.  **Raise** the default 128 KB CSV field limit (your HR rows are huge).
2.  For *each* raw CSV in `~/biologger/data/raw/`
        ▸ parse the `data` blob → rows (timestamp, hr_bpm, source, context)  
        ▸ write/overwrite `~/biologger/data/flat/hr_YYYY-MM-DD.csv`
3.  Pick the newest CO₂ JSON whose *date* matches the raw file’s date,
   convert to 1-minute means.
4.  **merge_asof** with a ±3 min window.
5.  Write `~/biologger/data/fused/fused_YYYY-MM-DD.csv`.
6.  Echo a small report, the first 50 overall lines **and** the first 50 lines
    that actually contain HR values.

After this you can point Streamlit at *fused/* and (e.g.) resample to 10-minute
bins for charts & scores.

--------------------------------------------------------------------------"""

import ast, csv, json, pathlib, sys, datetime as dt
import pandas as pd

# ───────────────────────────────────────────────── constants ─────────────────
RAW_DIR   = pathlib.Path("~/biologger/data/raw").expanduser()
FLAT_DIR  = pathlib.Path("~/biologger/data/flat").expanduser()
CO2_DIR   = pathlib.Path("~/data/co2").expanduser()
FUSED_DIR = pathlib.Path("~/biologger/data/fused").expanduser()

for d in (FLAT_DIR, FUSED_DIR):
    d.mkdir(parents=True, exist_ok=True)

csv.field_size_limit(sys.maxsize)        # lift 128 KB ceiling  ⇦⇦⇦

# ────────────────────────── helper ────────────────────────────────────────────
def flatten_hr(raw_csv: pathlib.Path) -> pathlib.Path:
    """Return path of freshly written flat/hr_YYYY-MM-DD.csv"""
    day = raw_csv.stem.rsplit("_", 1)[-1]              # 2025-07-29
    flat_path = FLAT_DIR / f"hr_{day}.csv"

    rows_out = []
    with raw_csv.open(newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            try:
                payload = ast.literal_eval(r["data"])  # safe ‘eval’
                for m in payload["data"]["metrics"]:
                    if m.get("name") != "heart_rate":
                        continue
                    for beat in m["data"]:
                        rows_out.append(
                            (beat["date"], beat["Avg"], beat.get("source"),
                             beat.get("context"))
                        )
            except Exception:
                continue                                # skip malformed row

    if not rows_out:
        return flat_path  # nothing parsed, but avoid crash

    pd.DataFrame(rows_out,
                 columns=["timestamp", "hr_bpm", "source", "context"]
         ).to_csv(flat_path, index=False)
    return flat_path

# ────────────────────────── main ─────────────────────────────────────────────
for raw_csv in sorted(RAW_DIR.glob("bio_*.csv"), key=lambda p: p.stat().st_mtime):
    day = raw_csv.stem.rsplit("_", 1)[-1]              # yyyy-mm-dd
    flat_csv = flatten_hr(raw_csv)

    # ── find matching CO₂ JSON (same calendar day) ───────────────────────────
    co2_files = sorted(CO2_DIR.glob(f"co2_{day}T*.json"))
    if not co2_files:
        print(f"⚠  no CO₂ for {day} → skipping\n")
        continue

    co2_path = co2_files[-1]                           # newest that day
    records = []
    with co2_path.open() as f:
        for line in f:
            line = line.strip().rstrip(",")
            if line and line not in "[]":
                records.append(json.loads(line))

    co2 = (pd.DataFrame.from_records(records)
           .assign(timestamp=lambda df: pd.to_datetime(df["timestamp"]))
           .set_index("timestamp")
           .resample("60s").mean()
           .dropna()
           .reset_index())

    # ── heart-rate for that day ──────────────────────────────────────────────
    hr = (pd.read_csv(flat_csv)
            .assign(timestamp=lambda df:
                    pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(None))
            .dropna(subset=["hr_bpm"]))

    if hr.empty:
        print(f"{raw_csv.name}: 0 HR rows → skipping\n")
        continue

    # ── fuse (±3 min) ────────────────────────────────────────────────────────
    merged = pd.merge_asof(
        co2.sort_values("timestamp"),
        hr.sort_values("timestamp"),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta("3min"),
    )

    out = FUSED_DIR / f"fused_{day}.csv"
    merged.to_csv(out, index=False)

    # ── quick report ─────────────────────────────────────────────────────────
    print(f"\n✅  Wrote {out}")
    print(f"   CO₂ minutes: {len(co2):>6,}")
    print(f"   HR  samples : {len(hr):>6,}")
    print(f"   Fused rows : {len(merged):>6,}")
    print(f"   HR matches : {merged['hr_bpm'].notna().sum():>6,}\n")

    print("── First 50 rows ──")
    print(merged.head(50).to_string(index=False))

    hr_rows = merged[merged["hr_bpm"].notna()].head(50)
    if not hr_rows.empty:
        print("\n── First 50 rows *with HR* ──")
        print(hr_rows.to_string(index=False))
    print("\n" + "═"*80 + "\n")
