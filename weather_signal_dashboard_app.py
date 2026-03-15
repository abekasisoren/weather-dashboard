import json
import os
from datetime import datetime

import pandas as pd
import psycopg
import streamlit as st

DATABASE_URL = os.environ.get("DATABASE_URL")

st.set_page_config(page_title="Global Weather Signal Dashboard", layout="wide")

st.title("Global Weather Signal Dashboard")
st.caption("Early weather intelligence for markets based on real ECMWF forecast signals.")

if not DATABASE_URL:
    st.error("DATABASE_URL environment variable is not set.")
    st.stop()


def read_sql(query: str) -> pd.DataFrame:
    with psycopg.connect(DATABASE_URL) as conn:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=cols)


def score_bucket(score: int) -> str:
    if score >= 8:
        return "HIGH CONVICTION"
    if score >= 5:
        return "ACTIONABLE"
    return "EARLY SIGNAL"


def format_dt(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M UTC")
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(value)


def parse_details(value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return {}


def make_recommendation(row) -> str:
    bias = str(row.get("trade_bias", "watch")).lower()
    commodity = row.get("commodity", "Unknown")
    anomaly = row.get("anomaly_type", "weather signal")
    score = int(row.get("signal_level", 0))

    if bias == "bullish":
        if score >= 8:
            return f"Strong bullish watch on {commodity}"
        if score >= 5:
            return f"Moderate bullish watch on {commodity}"
        return f"Early bullish signal for {commodity}"

    if bias == "bearish":
        if score >= 8:
            return f"Strong bearish watch on {commodity}"
        if score >= 5:
            return f"Moderate bearish watch on {commodity}"
        return f"Early bearish signal for {commodity}"

    return f"Monitor {commodity} for {anomaly}"


def make_what_changed(row, details) -> str:
    anomaly = row.get("anomaly_type", "")
    value = row.get("anomaly_value")
    region = row.get("region", "")
    commodity = row.get("commodity", "")

    if anomaly in ("heatwave", "extreme_heat"):
        return f"{region} is showing {anomaly.replace('_', ' ')} conditions affecting {commodity}. Peak forecast temperature is {value:.1f}°C."
    if anomaly == "frost":
        return f"{region} is showing frost risk affecting {commodity}. Minimum forecast temperature is {value:.1f}°C."
    if anomaly == "heavy_rain":
        return f"{region} is showing heavy rain risk affecting {commodity}. Forecast precipitation is {value:.1f} mm over the scan window."
    if anomaly == "drought":
        mean_temp = details.get('temp_c_mean')
        if mean_temp is not None:
            return f"{region} is showing drought risk affecting {commodity}. Rainfall is only {value:.1f} mm with mean temperature around {mean_temp:.1f}°C."
        return f"{region} is showing drought risk affecting {commodity}. Rainfall is only {value:.1f} mm."
    if anomaly == "storm_wind":
        return f"{region} is showing storm-wind risk affecting {commodity}. Peak wind is {value:.1f} m/s."
    return f"{region} is showing a weather signal affecting {commodity}. Measured value: {value}."


def make_why_it_matters(row) -> str:
    commodity = row.get("commodity", "")
    anomaly = row.get("anomaly_type", "")
    bias = row.get("trade_bias", "watch")

    if bias == "bullish":
        return f"{anomaly.replace('_', ' ').title()} can tighten supply or raise demand sensitivity for {commodity}, which may support prices and related exposures."
    if bias == "bearish":
        return f"{anomaly.replace('_', ' ').title()} can improve supply conditions or weaken pricing support for {commodity}, which may pressure prices and related exposures."
    return f"This signal may affect pricing expectations and positioning in {commodity}, but the direction is not yet strong enough for conviction."


def make_best_vehicle(row) -> str:
    commodity = row.get("commodity", "")
    mapping = {
        "Corn": "Corn futures / CORN ETF",
        "Soybeans": "Soybean futures / SOYB ETF",
        "Wheat": "Wheat futures / WEAT ETF",
        "Coffee": "Coffee futures / JO ETF",
        "Sugar": "Sugar futures / CANE ETF",
        "Natural Gas": "Natural gas futures / UNG ETF",
        "Coal": "Coal producers / coal-linked equities",
        "Rice": "Rice futures / regional agriculture proxies",
        "Power Utilities": "European utilities / power-sensitive names",
    }
    return mapping.get(commodity, commodity)


def make_proxy_equities(row) -> str:
    commodity = row.get("commodity", "")

    mapping = {
        "Corn": "ADM, BG, CF, MOS, CTVA, DE, UNP",
        "Soybeans": "ADM, BG, CF, MOS, CTVA, DE",
        "Wheat": "ADM, BG, MOS, CF, DE",
        "Coffee": "SBUX, NSRGY, JDE-related exposure, coffee futures proxies",
        "Sugar": "CZZ, TRRJF, sugar futures proxies",
        "Natural Gas": "UNG, EQT, CTRA, RRC, LNG exporters, utilities",
        "Power Utilities": "European utilities, power generators, gas-sensitive industrials",
        "Coal": "BTU, ARCH, AMR, coal transport exposure",
        "Rice": "Rice-linked agri merchants and regional staples exposure",
    }
    return mapping.get(commodity, commodity)


def show_signal_block(row):
    details = parse_details(row.get("details"))

    bucket = score_bucket(int(row["signal_level"]))
    recommendation = make_recommendation(row)
    what_changed = make_what_changed(row, details)
    why_it_matters = make_why_it_matters(row)
    best_vehicle = make_best_vehicle(row)
    proxy_equities = make_proxy_equities(row)

    st.markdown(f"### {row['region']} — {row['anomaly_type'].replace('_', ' ').title()} — {row['commodity']}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Signal Level", int(row["signal_level"]))
    c2.metric("Bucket", bucket)
    c3.metric("Trade Bias", str(row["trade_bias"]).title())
    c4.metric("Recommendation", recommendation)

    c5, c6, c7 = st.columns(3)
    c5.metric("Severity", int(row["severity_score"]))
    c6.metric("Persistence", int(row["persistence_score"]))
    c7.metric("Market Score", int(row["market_score"]))

    st.markdown(f"**Forecast Window:** {format_dt(row['forecast_start'])} → {format_dt(row['forecast_end'])}")
    st.markdown(f"**Best Vehicle:** {best_vehicle}")
    st.markdown(f"**Proxy Equities:** {proxy_equities}")

    with st.expander("Full explanation"):
        st.markdown("**What changed**")
        st.write(what_changed)

        st.markdown("**Why it matters**")
        st.write(why_it_matters)

        st.markdown("**Weather details**")
        if details:
            pretty_details = {
                "temp_c_max": details.get("temp_c_max"),
                "temp_c_mean": details.get("temp_c_mean"),
                "temp_c_min": details.get("temp_c_min"),
                "precip_mm_7d": details.get("precip_mm_7d"),
                "wind_ms_max": details.get("wind_ms_max"),
            }
            st.json(pretty_details)
        else:
            st.write("No additional detail available.")

    st.divider()


try:
    global_shocks = read_sql(
        """
        SELECT
            timestamp,
            region,
            commodity,
            anomaly_type,
            anomaly_value,
            persistence_score,
            severity_score,
            market_score,
            signal_level,
            trade_bias,
            source_file,
            forecast_start,
            forecast_end,
            details,
            created_at
        FROM weather_global_shocks
        ORDER BY signal_level DESC, region ASC, commodity ASC, anomaly_type ASC
        """
    )
except Exception as e:
    st.error(f"Could not read weather_global_shocks from database: {e}")
    st.stop()

if global_shocks.empty:
    st.warning("No weather shocks found yet.")
    st.stop()

if "created_at" in global_shocks.columns and not global_shocks["created_at"].isna().all():
    st.write("Last update:", format_dt(global_shocks["created_at"].max()))

high_conviction = global_shocks[global_shocks["signal_level"] >= 8].copy()
actionable = global_shocks[(global_shocks["signal_level"] >= 5) & (global_shocks["signal_level"] < 8)].copy()
early = global_shocks[global_shocks["signal_level"] < 5].copy()

st.header("🔥 High Conviction")
if high_conviction.empty:
    st.write("No high-conviction weather trades right now.")
else:
    for _, row in high_conviction.iterrows():
        show_signal_block(row)

st.header("⚠️ Actionable")
if actionable.empty:
    st.write("No actionable setups right now.")
else:
    for _, row in actionable.iterrows():
        show_signal_block(row)

st.header("👀 Early Signals")
if early.empty:
    st.write("No early signals right now.")
else:
    for _, row in early.iterrows():
        with st.expander(f"{row['region']} — {row['anomaly_type'].replace('_', ' ').title()} — {row['commodity']}"):
            details = parse_details(row.get("details"))
            st.write(make_what_changed(row, details))
            st.write(make_why_it_matters(row))
            st.markdown(f"**Trade Bias:** {str(row['trade_bias']).title()}")
            st.markdown(f"**Best Vehicle:** {make_best_vehicle(row)}")
            st.markdown(f"**Proxy Equities:** {make_proxy_equities(row)}")

with st.expander("Raw weather_global_shocks table"):
    st.dataframe(global_shocks, use_container_width=True)
