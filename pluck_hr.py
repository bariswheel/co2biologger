#!/usr/bin/env python3
"""
pluck_hr.py  ––  minimal demo

Pulls the *Avg* BPM and the associated timestamp found in each
`bio_YYYY-MM-DD.csv` row that looks like

  "{'data': {'metrics': [{'units': 'count/min',
     'name': 'heart_rate',
     'data': [{'source': '...', 'Min': 80, 'Avg': 80, 'date': '2025-07-28 00:41:00 -0700', 'Max': 80}, …]}}]}"

Outputs two columns:  timestamp , avg_bpm
"""

import csv, ast, pathlib, re, pandas as pd

csv.field_size_limit(10_000_000)

RAW = pathlib.Path("~/biologger/data/raw").expanduser()
raw_file = max(RAW.glob("bio_*.csv"), key=lambda p: p.stat().st_mtime)

rows = []
with raw_file.open(newline="") as f:
    rdr = csv.DictReader(f)
    for r in rdr:
        blob = r["data"].replace("\xa0", " ")        # normalise weird spaces
        obj  = ast.literal_eval(blob)                # safe Python-literal parse
        try:
            beats = obj["data"]["metrics"][0]["data"]
            for beat in beats:
                rows.append(
                    {
                        "timestamp": pd.to_datetime(beat["date"], utc=True),
                        "avg_bpm":   beat["Avg"],
                    }
                )
        except (KeyError, IndexError, TypeError):
            # skip malformed rows silently
            continue

hr = pd.DataFrame(rows)
print(hr.head(10).to_string(index=False))
print(f"\nParsed {len(hr):,} HR samples from → {raw_file.name}")
