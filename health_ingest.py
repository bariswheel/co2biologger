#!/usr/bin/env python3
# health_ingest.py
from fastapi import FastAPI, Request
import pandas as pd, pathlib, datetime as dt, json
from zoneinfo import ZoneInfo           # ← NEW: local-time helper

DATA = pathlib.Path("data"); DATA.mkdir(exist_ok=True)
app = FastAPI()

@app.post("/health")
async def sink(req: Request):
    payload = await req.json()

    # use local (SF) time so filenames roll at local midnight
    ts = pd.Timestamp.now(tz=ZoneInfo("America/Los_Angeles"))

    raw_dir  = DATA / "raw";  raw_dir.mkdir(exist_ok=True)
    flat_dir = DATA / "flat"; flat_dir.mkdir(exist_ok=True)

    # ---- 1. archive full blob ----
    raw_fn = raw_dir / f"bio_{ts.date()}.csv"
    pd.DataFrame([{"data": payload, "time": ts}]).to_csv(
        raw_fn, mode="a", header=not raw_fn.exists(), index=False
    )

    # ---- 2. flatten any heart-rate objects ----
    rows = []
    for metric in payload.get("metrics", []):
        if metric.get("name") != "heart_rate":
            continue
        for rec in metric.get("data", []):
            rows.append(
                {
                    "timestamp": pd.to_datetime(rec["date"]),
                    "hr_bpm":    rec.get("Avg", rec.get("value")),
                    "source":    rec.get("source"),
                    "context":   rec.get("context"),
                }
            )
    if rows:
        flat_fn = flat_dir / f"hr_{ts.date()}.csv"
        pd.DataFrame(rows).to_csv(
            flat_fn, mode="a", header=not flat_fn.exists(), index=False
        )

    return {"status": "ok", "rows_written": len(rows)}
