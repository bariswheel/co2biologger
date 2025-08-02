#!/usr/bin/env python3
# ~/biologger/dashboard.py
"""
Streamlit dashboard for fused CO₂ + heart-rate data.

▪︎ Reads CSVs in ~/biologger/data/fused/fused_YYYY-MM-DD.csv  
▪︎ Lets the user choose a day (latest pre-selected).  
▪︎ Draws three Plotly charts:
      ‣ CO₂ vs time
      ‣ HR vs time
      ‣ Combined overlay (twin Y axes)
The file list is cached for 60 s so new days appear automatically.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import pathlib
from datetime import timedelta

FUSED_DIR = pathlib.Path("~/biologger/data/fused").expanduser()

st.set_page_config(page_title="Indoor Air & Biomarkers", layout="wide")
st.title("Indoor CO₂ & Heart-Rate dashboard")
st.caption("Charts refresh automatically every 60 s")

# ── helper caches ──────────────────────────────────────────────────
@st.cache_data(ttl=60)
def list_fused_files():
    return sorted(FUSED_DIR.glob("fused_*.csv"))

@st.cache_data(ttl=60)
def load_csv(path):
    return pd.read_csv(path, parse_dates=["timestamp"])

# ── sidebar day-picker ─────────────────────────────────────────────
files = list_fused_files()
if not files:
    st.error("No fused CSVs found in ~/biologger/data/fused/")
    st.stop()

labels = [p.stem.replace("fused_", "") for p in files]
default_ix = len(files) - 1
choice = st.sidebar.selectbox("Choose day", labels, index=default_ix)
csv_path = files[labels.index(choice)]

df = load_csv(csv_path)
st.subheader(f"Showing: **{csv_path.name}**  "
             f"({len(df):,} rows, {df['hr_bpm'].notna().sum():,} HR values)")

# ── build charts ───────────────────────────────────────────────────
fig_co2 = go.Figure().add_scatter(
    x=df["timestamp"], y=df["co2_ppm"],
    mode="lines", name="CO₂ ppm", line=dict(color="#1f77b4")
)
fig_co2.update_layout(
    title="CO₂ concentration", yaxis_title="ppm", xaxis_title="time",
    margin=dict(l=40, r=20, t=40, b=40), height=300
)

fig_hr = go.Figure().add_scatter(
    x=df["timestamp"], y=df["hr_bpm"],
    mode="lines", name="HR bpm", line=dict(color="#d62728")
)
fig_hr.update_layout(
    title="Heart-rate", yaxis_title="bpm", xaxis_title="time",
    margin=dict(l=40, r=20, t=40, b=40), height=300
)

fig_overlay = go.Figure()
fig_overlay.add_scatter(
    x=df["timestamp"], y=df["co2_ppm"],
    mode="lines", name="CO₂ ppm", line=dict(color="#1f77b4")
)
if df["hr_bpm"].notna().sum():
    fig_overlay.add_scatter(
        x=df["timestamp"], y=df["hr_bpm"],
        mode="lines", name="HR bpm", yaxis="y2", line=dict(color="#d62728")
    )

fig_overlay.update_layout(
    title="CO₂ and HR overlay",
    xaxis=dict(title="time"),
    yaxis=dict(title="CO₂ (ppm)"),
    yaxis2=dict(title="HR (bpm)", overlaying="y", side="right", showgrid=False),
    legend=dict(orientation="h", y=-0.25),
    margin=dict(l=60, r=60, t=40, b=60),
    height=400
)

# ── show charts ────────────────────────────────────────────────────
col1, col2 = st.columns(2)
with col1:
    st.plotly_chart(fig_co2, use_container_width=True)
with col2:
    st.plotly_chart(fig_hr, use_container_width=True)

st.plotly_chart(fig_overlay, use_container_width=True)
