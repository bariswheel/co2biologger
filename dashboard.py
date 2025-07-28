# ~/biologger/dashboard.py
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import pathlib
from datetime import timedelta

st.set_page_config(page_title="Indoor Air & Biomarkers", layout="wide")
st.title("Live CO₂ & Heart-Rate")
st.caption("Dashboard refreshes automatically every 60 s")

@st.cache_data(ttl=60)
def load_latest():
    latest = sorted(pathlib.Path("~/biologger").expanduser()
                    .glob("fused_*.csv"))[-1]
    df = pd.read_csv(latest, parse_dates=["timestamp"])
    return df

df = load_latest()

# ── Build the figure ────────────────────────────────────────────
fig = go.Figure()

# CO₂ trace (left axis)
fig.add_scatter(
    x=df["timestamp"], y=df["co2_ppm"],
    mode="lines", name="CO₂ ppm", line=dict(color="#1f77b4")
)

# Heart-rate trace (right axis) – only if data present
if df["hr_bpm"].notna().sum():
    fig.add_scatter(
        x=df["timestamp"], y=df["hr_bpm"],
        mode="lines", name="HR bpm", yaxis="y2", line=dict(color="#d62728")
    )

fig.update_layout(
    xaxis=dict(title="Time"),
    yaxis=dict(title="CO₂ (ppm)"),
    yaxis2=dict(
        title="Heart-Rate (bpm)",
        overlaying="y",
        side="right",
        showgrid=False,
    ),
    legend=dict(orientation="h", y=-0.25),
    margin=dict(l=60, r=60, t=40, b=60),
)

st.plotly_chart(fig, use_container_width=True)
