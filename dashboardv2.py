#!/usr/bin/env python3
"""
dashboard.py – Indoor CO₂ & Biomarkers Streamlit UI
────────────────────────────────────────────────────

WHAT / WHY
----------
Interactive dashboard for the fused CO₂ × heart-rate CSVs created by
*fusionv3.py*.  Adds three UX niceties requested on 2025-07-29:

1.  **Time-bucket selector** in the sidebar (raw 1-min, 5-min, 10-min,
    30-min or 60-min means) so you can zoom out without rerunning scripts.
2.  **Download button** that lets viewers grab exactly the fused file
    they’re inspecting (`st.download_button`).
3.  **Night-score stub** – a toy metric ( 100 – average CO₂ during periods
    when HR < 65 bpm ).  It appears next to the page title and updates
    with the chosen resampling.

HOW
----
* Detects the latest `fused_YYYY-MM-DD.csv` in `~/biologger/data/fused/`
  (user can override via sidebar).
* Resamples on-the-fly per the selected bucket.
* Renders three charts:
    • CO₂ vs. time
    • HR vs. time
    • Dual-axis overlay
* Auto-refresh every 60 s (configurable at top).

————————————————————————————————————————————————————————
"""

from pathlib import Path
import pandas as pd
import streamlit as st

# ── CONFIG ───────────────────────────────────────────────────────────
FUSED_DIR = Path("~/biologger/data/fused").expanduser()
AUTO_REFRESH_SEC = 60

# ── SIDEBAR – file + bucket selection ────────────────────────────────
st.sidebar.title("Data source")
all_fused = sorted(FUSED_DIR.glob("fused_*.csv"))
if not all_fused:
    st.error(f"No fused CSV files found in {FUSED_DIR}")
    st.stop()

latest = all_fused[-1]
file_choice = st.sidebar.selectbox(
    "Choose a fused file", options=all_fused, index=len(all_fused) - 1,
    format_func=lambda p: p.name)

bucket = st.sidebar.selectbox(
    "Resample to …", options=[
        ("Raw 1-min", "1min"),
        ("5 min", "5min"),
        ("10 min", "10min"),
        ("30 min", "30min"),
        ("Hourly", "60min"),
    ], format_func=lambda t: t[0])  # show label, keep freq as value

freq = bucket[1]

# ── LOAD & RESAMPLE ──────────────────────────────────────────────────
df = pd.read_csv(file_choice, parse_dates=["timestamp"])
df = df.set_index("timestamp").sort_index()

if freq != "1min":
    df = (df
          .resample(freq)
          .agg({
              "co2_ppm": "mean",
              "temp_c": "mean",
              "humidity_pct": "mean",
              "hr_bpm": "mean",
              "source": "first",
              "context": "first",
          })
          .dropna(subset=["co2_ppm"]))

# Helper counts
hr_values = df["hr_bpm"].notna().sum()
total_rows = len(df)

# ── SIMPLE NIGHT-SCORE STUB ──────────────────────────────────────────
try:
    asleep = df["hr_bpm"] < 65
    avg_co2_sleep = df.loc[asleep, "co2_ppm"].mean()
    night_score = max(0, 100 - int(round(avg_co2_sleep - 400) / 3))  # toy
except Exception:
    night_score = "—"

# ── LAYOUT ───────────────────────────────────────────────────────────
st.set_page_config(page_title="Indoor Air & Biomarkers", layout="wide", page_icon="🫁")
st.title("Indoor CO₂ & Heart-Rate dashboard")
st.caption(f"Charts refresh automatically every {AUTO_REFRESH_SEC} s")

st.subheader(
    f"Showing: {file_choice.name} "
    f"({total_rows:,} rows, {hr_values:,} HR values, bucket={bucket[0]})"
    f"   |   Night-score: **{night_score}**")

# Download
st.download_button(
    label="⬇️ Download this fused CSV",
    data=file_choice.read_bytes(),
    mime="text/csv",
    file_name=file_choice.name,
)

# ── CHARTS ───────────────────────────────────────────────────────────
co2_chart = st.line_chart(df["co2_ppm"], height=200, use_container_width=True)
hr_chart  = st.line_chart(df["hr_bpm"],  height=200, use_container_width=True)

st.subheader("CO₂ and HR overlay")
overlay = df[["co2_ppm", "hr_bpm"]].dropna()
st.line_chart(overlay, use_container_width=True, height=250)

# --- after we load the day’s fused DataFrame ------------------------------
import numpy as np

# crude example: penalise high CO₂, reward low resting HR
sleep_blocks = df.between_time("00:00", "08:00")
mean_co2  = sleep_blocks["co2_ppm"].mean()
mean_hr   = sleep_blocks["hr_bpm"].mean()

score = 100
score -= (mean_co2 - 600) * 0.05     # −1 pt per 20 ppm above 600
score -= (mean_hr  - 55)  * 0.5      # −1 pt per 2 bpm above 55
score  = np.clip(score, 0, 100)

st.metric("💤 Sleep score", f"{score:0.0f} / 100")
st.caption(f"(avg CO₂ ≈ {mean_co2:0.0f} ppm, avg HR ≈ {mean_hr:0.0f} bpm)")

# ──────────────────────────────────────────────────────────────────
# 4.  QUICK SCORE-CARDS  ───────────────────────────────────────────
# ------------------------------------------------------------------
night_block = df.between_time("00:00", "08:00")      # your sleep window
day_block   = df.between_time("08:00", "23:59")      # the rest

def air_quality_score(block: pd.DataFrame) -> float:
    """Linear penalty above 600 ppm and a hard floor at 0."""
    mean_co2 = block["co2_ppm"].mean()
    score = 100 - max(mean_co2 - 600, 0) * 0.05      # −1 pt per 20 ppm
    return max(0, min(100, score)), mean_co2

sleep_score, sleep_co2 = air_quality_score(night_block)
day_score,   day_co2   = air_quality_score(day_block)

with st.container():
    col1, col2 = st.columns(2)
    col1.metric("🌙 Air-quality sleep score", f"{sleep_score:.0f} / 100",
                f"avg CO₂ ≈ {sleep_co2:.0f} ppm")
    col2.metric("🌞 Daytime air-quality score", f"{day_score:.0f} / 100",
                f"avg CO₂ ≈ {day_co2:.0f} ppm")

st.divider()

# ──────────────────────────────────────────────────────────────────
# 5.  CORRELATION SECTION  ────────────────────────────────────────
# -----------------------------------------------------------------
st.subheader("CO₂ ↔ Heart-rate correlation")

corr_df = df.dropna(subset=["hr_bpm"])
if corr_df.empty:
    st.info("No overlapping CO₂ + HR data to correlate.")
else:
    r = corr_df["co2_ppm"].corr(corr_df["hr_bpm"])
    st.caption(f"Pearson *r* = **{r:+.2f}**   ({len(corr_df):,} paired points)")

    import altair as alt

    base = alt.Chart(corr_df).mark_circle(
        size=35, opacity=0.35, color="steelblue"
    ).encode(
        x=alt.X("co2_ppm:Q",
                title="CO₂ concentration (ppm)",
                scale=alt.Scale(domain=[500, 800])),        # ⬅ clamp X
        y=alt.Y("hr_bpm:Q",
                title="Heart-rate (bpm)",
                scale=alt.Scale(domain=[40, 110])),         # ⬅ clamp Y
        tooltip=["timestamp:T", "co2_ppm:Q", "hr_bpm:Q"]
    )

    trend = base.transform_regression(
        "co2_ppm", "hr_bpm"
    ).mark_line(color="firebrick", size=2)

    st.altair_chart(base + trend, use_container_width=True)

st.caption(
    "Scores: 100 = excellent (≤600 ppm), 60 ≈ 900 ppm, 0 ≥ 2600 ppm • "
    "Correlation uses only timestamps where both metrics are present."
)
# OPTIONAL automatic refresh ──────────────────────────────────────
if st.query_params.get("auto"):
    st.rerun()
