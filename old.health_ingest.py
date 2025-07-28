#!/usr/bin/env python3
# health_ingest.py
from fastapi import FastAPI, Request
import pandas as pd, pathlib, datetime as dt, json

DATA = pathlib.Path("data"); DATA.mkdir(exist_ok=True)
app = FastAPI()

@app.post("/health")
async def sink(req: Request):
    payload = await req.json()            # expect JSON from the phone
    ts = pd.to_datetime(payload.get("timestamp", dt.datetime.utcnow()))
    row = {**payload, "time": ts}
    fn = DATA / f"bio_{ts.date()}.csv"
    pd.DataFrame([row]).to_csv(fn, mode="a", header=not fn.exists(), index=False)
    return {"status": "ok"}
