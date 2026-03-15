import json
import os
from datetime import datetime

import pandas as pd
import psycopg
import streamlit as st

DATABASE_URL = os.environ.get("DATABASE_URL")

st.set_page_config(page_title="Global Weather Signal Dashboard", layout="wide")

st.title("Global Weather Signal Dashboard")
st.caption("Early weather intelligence for markets ranked like a trading radar.")

if not DATABASE_URL:
    st.error("DATABASE_URL environment variable is not set.")
    st.stop()

# REAL ETF / STOCK MAPS
COMMODITY_MAP = {
    "Corn": {
        "vehicle": "CORN",
        "stocks_long": ["ADM", "BG", "CF", "MOS", "CTVA", "DE", "UNP"],
    },
    "Soybeans": {
        "vehicle": "SOYB",
        "stocks_long": ["ADM", "BG", "CF", "MOS", "CTVA", "DE"],
    },
    "Wheat": {
        "vehicle": "WEAT",
        "stocks_long": ["ADM", "BG", "MOS", "CF", "DE"],
    },
    "Coffee": {
        "vehicle": "JO",
        "stocks_long": ["SBUX", "NSRGY"],
    },
    "Sugar": {
        "vehicle": "CANE",
        "stocks_long": ["CZZ", "TRRJF"],
    },
    "Natural Gas": {
        "vehicle": "UNG",
        "stocks_long": ["EQT", "LNG", "CTRA", "RRC"],
    },
    "Oil": {
        "vehicle": "USO",
        "stocks_long": ["XOM", "CVX", "COP"],
    },
    "Power Utilities": {
        "vehicle": "XLU",
        "stocks_long": ["NEE", "DUK", "SO", "AEP"],
    },
    "Coal": {
        "vehicle": "KOL",
        "stocks_long": ["BTU", "ARCH", "AMR"],
    },
    "Rice": {
        "vehicle": "DBA",
        "stocks_long": ["ADM", "BG"],
    },
}

# EVENT OVERLAYS = EXTRA NAMES, ONLY WHEN THEY MAKE SENSE
EVENT_OVERLAY = {
    "hurricane_risk": {
        "long": ["HD", "LOW", "GNRC", "CAT", "VMC", "XOM", "CVX"],
        "short": ["ALL", "TRV", "CB"],
    },
    "flood_risk": {
        "long": ["CAT", "VMC", "MLM", "XYL", "HD", "LOW"],
        "short": ["ALL", "TRV", "CB"],
    },
    "heavy_rain": {
        "long": ["CAT", "VMC", "MLM"],
        "short": ["ALL", "TRV", "CB"],
    },
    "storm_wind": {
        "long": ["GNRC", "CAT", "VMC"],
        "short": ["ALL", "TRV", "CB"],
    },
    "wildfire_risk": {
        "long": ["GNRC", "CAT", "VMC", "HD", "LOW"],
        "short": ["PCG"],
    },
    "cold_wave": {
        "long": ["LNG", "EQT", "CTRA", "RRC"],
        "short": ["DAL", "UAL", "AAL"],
    },
    "frost": {
        "long": ["LNG", "EQT", "CTRA", "RRC"],
        "short": [],
    },
    "heatwave": {
        "long": ["GNRC"],
        "short": [],
    },
    "extreme_heat": {
        "long": ["GNRC"],
        "short": [],
    },
    "drought": {
        "long": [],
        "short": [],
    },
}


def read_sql(query: str) -> pd.DataFrame:
    with psycopg.connect(DATABASE_URL) as conn:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=cols)


def is_missing(value) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    text = str(value).strip()
    return text == "" or text.lower() in {"none", "nan", "nat", "null"}


def normalize_text(value, fallback="-") -> str:
    return fallback if is_missing(value) else str(value).strip()


def format_dt(value) -> str:
    if is_missing(value):
        return "-"
    try:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return str(value)
        return parsed.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(value)


def parse_jsonish(value):
    if is_missing(value):
        return {}
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def safe_int(value, default=0) -> int:
    try:
        if pd.isna(value):
            return default
        return int(value)
    except Exception:
        return default


def safe_float(value, default=None):
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def score_bucket(score: int) -> str:
    if score >= 8:
        return "HIGH"
    if score >= 5:
        return "MEDIUM"
    return "EARLY"


def trade_label_from_bias(value: str) -> str:
    if is_missing(value):
        return "No Trade"
    v = str(value).strip().lower()
    if v in {"bullish", "long"}:
        return "Long"
    if v in {"bearish", "short"}:
        return "Short"
    return "No Trade"


def normalize_anomaly_key(value: str) -> str:
    raw = normalize_text(value, "").lower().strip()
    mapping = {
        "extreme_heat": "heatwave",
        "frost": "frost",
        "cold_wave": "cold_wave",
        "heavy_rain": "heavy_rain",
        "flood_risk": "flood_risk",
        "storm_wind": "storm_wind",
        "wildfire_risk": "wildfire_risk",
        "hurricane_risk": "hurricane_risk",
    }
    return mapping.get(raw, raw)


def commodity_context_type(row) -> str:
    commodity = normalize_text(row.get("commodity"), "")
    if commodity in {"Corn", "Soybeans", "Wheat", "Coffee", "Sugar", "Rice"}:
        return "ag"
    if commodity in {"Natural Gas", "Oil", "Coal"}:
        return "energy"
    if commodity in {"Power Utilities"}:
        return "utilities"
    return "other"


def infer_trade(row) -> str:
    direct = trade_label_from_bias(row.get("trade_bias"))
    if direct != "No Trade":
        return direct

    anomaly = normalize_anomaly_key(row.get("anomaly_type"))
    commodity = normalize_text(row.get("commodity"), "")
    ctype = commodity_context_type(row)

    if anomaly in {"heatwave", "extreme_heat", "drought"} and ctype in {"ag", "energy", "utilities"}:
        return "Long"

    if anomaly in {"cold_wave", "frost"} and commodity in {"Natural Gas", "Power Utilities", "Wheat"}:
        return "Long"

    if anomaly in {"heavy_rain", "flood_risk"} and ctype == "ag":
        return "Short"

    if anomaly in {"heavy_rain", "flood_risk"} and ctype in {"energy", "utilities"}:
        return "Long"

    if anomaly in {"storm_wind", "hurricane_risk", "wildfire_risk"} and ctype in {"energy", "utilities"}:
        return "Long"

    return "No Trade"


def conviction_badge(bucket: str) -> str:
    bucket = normalize_text(bucket, "EARLY").upper()
    if bucket == "HIGH":
        return "<span style='background:#7f1d1d;color:#fecaca;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>HIGH</span>"
    if bucket == "MEDIUM":
        return "<span style='background:#78350f;color:#fde68a;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>MEDIUM</span>"
    return "<span style='background:#334155;color:#cbd5e1;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>EARLY</span>"


def trade_badge(trade: str) -> str:
    if trade == "Long":
        return "<span style='background:#14532d;color:#bbf7d0;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>LONG</span>"
    if trade == "Short":
        return "<span style='background:#991b1b;color:#fecaca;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>SHORT</span>"
    return "<span style='background:#374151;color:#e5e7eb;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>NO TRADE</span>"


def build_title(row) -> str:
    region = normalize_text(row.get("region"))
    anomaly = normalize_text(row.get("anomaly_type")).replace("_", " ").title()
    commodity = normalize_text(row.get("commodity"))
    return f"{region} — {anomaly} — {commodity}"


def get_vehicle(row) -> str:
    commodity = normalize_text(row.get("commodity"), "")
    info = COMMODITY_MAP.get(commodity)
    if info:
        return info["vehicle"]
    return commodity if commodity else "-"


def get_commodity_stocks(row) -> list[str]:
    commodity = normalize_text(row.get("commodity"), "")
    info = COMMODITY_MAP.get(commodity)
    if info:
        return info.get("stocks_long", [])
    return []


def get_event_overlay_longs(row) -> list[str]:
    anomaly = normalize_anomaly_key(row.get("anomaly_type"))
    return EVENT_OVERLAY.get(anomaly, {}).get("long", [])


def get_event_overlay_shorts(row) -> list[str]:
    anomaly = normalize_anomaly_key(row.get("anomaly_type"))
    return EVENT_OVERLAY.get(anomaly, {}).get("short", [])


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            out.append(item)
            seen.add(item)
    return out


def get_stock_trade(row) -> str:
    trade = infer_trade(row)
    ctype = commodity_context_type(row)

    commodity_stocks = get_commodity_stocks(row)
    overlay_longs = get_event_overlay_longs(row)
    overlay_shorts = get_event_overlay_shorts(row)

    if trade == "Long":
        if ctype in {"ag", "energy", "utilities"}:
            names = dedupe_keep_order(commodity_stocks + overlay_longs)[:5]
            return f"Long {', '.join(names)}" if names else "-"
        names = overlay_longs[:5]
        return f"Long {', '.join(names)}" if names else "-"

    if trade == "Short":
        if ctype == "ag":
            names = commodity_stocks[:5]
            return f"Short {', '.join(names)}" if names else "-"
        names = overlay_shorts[:5]
        return f"Short {', '.join(names)}" if names else "-"

    return "-"


def get_commodity_trade(row) -> str:
    commodity = normalize_text(row.get("commodity"), "")
    trade = infer_trade(row)
    if commodity == "-":
        return "-"
    if trade == "Long":
        return f"Long {commodity}"
    if trade == "Short":
        return f"Short {commodity}"
    return f"Watch {commodity}"


def get_why_it_matters(row) -> str:
    anomaly = normalize_text(row.get("anomaly_type"), "").replace("_", " ")
    commodity = normalize_text(row.get("commodity"), "")
    trade = infer_trade(row)

    if trade == "Long":
        return f"{anomaly.title()} can tighten supply, disrupt logistics, or raise demand risk for {commodity}, which may support prices and related stocks."
    if trade == "Short":
        return f"{anomaly.title()} can damage conditions or pressure pricing for {commodity}, which may hurt prices and related stocks."
    return f"{anomaly.title()} matters for {commodity}, but the setup is not strong enough yet for a clear trade."


def get_assets_summary(row) -> str:
    parts = []

    vehicle = get_vehicle(row)
    if vehicle != "-":
        parts.append(vehicle)

    stock_trade = get_stock_trade(row)
    if stock_trade != "-":
        parts.append(stock_trade)

    return " | ".join(parts) if parts else "-"


def build_trigger_evidence(row) -> list[str]:
    details = parse_jsonish(row.get("details"))
    evidence = []

    anomaly = normalize_anomaly_key(row.get("anomaly_type"))
    temp_max = safe_float(details.get("temp_c_max"))
    temp_mean = safe_float(details.get("temp_c_mean"))
    temp_min = safe_float(details.get("temp_c_min"))
    precip = safe_float(details.get("precip_mm_7d"))
    wind = safe_float(details.get("wind_ms_max"))

    if anomaly == "heatwave":
        if temp_max is not None:
            evidence.append(f"Max temp: {temp_max:.1f}°C (heat trigger > 35°C)")
        if temp_mean is not None:
            evidence.append(f"Mean temp: {temp_mean:.1f}°C")

    if anomaly == "drought":
        if precip is not None:
            evidence.append(f"Rainfall: {precip:.1f} mm (drought trigger ≤ 10 mm)")
        if temp_mean is not None:
            evidence.append(f"Mean temp: {temp_mean:.1f}°C (heat stress condition ≥ 28°C)")

    if anomaly in {"frost", "cold_wave"}:
        if temp_min is not None:
            evidence.append(f"Min temp: {temp_min:.1f}°C")

    if anomaly in {"heavy_rain", "flood_risk"}:
        if precip is not None:
            evidence.append(f"Rainfall: {precip:.1f} mm")

    if anomaly in {"storm_wind", "hurricane_risk", "wildfire_risk"}:
        if wind is not None:
            evidence.append(f"Wind: {wind:.1f} m/s")
        if temp_max is not None:
            evidence.append(f"Max temp: {temp_max:.1f}°C")
        if precip is not None:
            evidence.append(f"Rainfall: {precip:.1f} mm")

    evidence.append(f"Signal level: {safe_int(row.get('signal_level'))}")
    evidence.append(f"Persistence: {safe_int(row.get('persistence_score'))}")
    evidence.append(f"Severity: {safe_int(row.get('severity_score'))}")
    evidence.append(f"Market score: {safe_int(row.get('market_score'))}")

    return evidence


def show_trade_card(row, rank_number=None):
    title = build_title(row)
    prefix = f"#{rank_number} " if rank_number is not None else ""
    trade = infer_trade(row)
    bucket = normalize_text(row.get("signal_bucket"), score_bucket(safe_int(row.get("signal_level"))))

    st.markdown(f"### {prefix}{title}")

    b1, b2 = st.columns([1, 5])
    with b1:
        st.markdown(trade_badge(trade), unsafe_allow_html=True)
    with b2:
        st.markdown(conviction_badge(bucket), unsafe_allow_html=True)

    st.markdown(f"**Commodity Trade:** {get_commodity_trade(row)}")
    st.markdown(f"**Stock Trade:** {get_stock_trade(row)}")
    st.markdown(f"**Vehicle:** {get_vehicle(row)}")
    st.markdown(f"**Why this matters:** {get_why_it_matters(row)}")
    st.markdown(f"**Assets:** {get_assets_summary(row)}")
    st.markdown(f"**Forecast Window:** {format_dt(row.get('forecast_start'))} → {format_dt(row.get('forecast_end'))}")

    with st.expander("Full breakdown"):
        st.markdown("**Why this signal triggered**")
        for item in build_trigger_evidence(row):
            st.write(f"- {item}")

        st.markdown("**Commodity recommendation**")
        st.write(get_commodity_trade(row))

        st.markdown("**Stock recommendation**")
        st.write(get_stock_trade(row))

        st.markdown("**Weather details**")
        details = parse_jsonish(row.get("details"))
        if isinstance(details, dict) and details:
            st.json(details)
        else:
            st.write("No details available.")

    st.divider()


df = read_sql(
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
        signal_bucket,
        trade_bias,
        recommendation,
        affected_market,
        best_vehicle,
        proxy_equities,
        secondary_exposures,
        affected_assets_json,
        what_changed,
        why_it_matters,
        what_to_watch_next,
        source_file,
        forecast_start,
        forecast_end,
        details,
        created_at
    FROM weather_global_shocks
    ORDER BY created_at DESC, signal_level DESC, region ASC, commodity ASC
    """
)

if df.empty:
    st.warning("No weather shocks found yet.")
    st.stop()

for col in ["signal_level", "persistence_score", "severity_score", "market_score"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

df["signal_bucket"] = df["signal_bucket"].fillna(df["signal_level"].apply(score_bucket))
df["trade_display"] = df.apply(infer_trade, axis=1)

df = (
    df.sort_values(
        by=["signal_level", "persistence_score", "market_score", "severity_score", "created_at"],
        ascending=[False, False, False, False, False],
    )
    .drop_duplicates(subset=["region", "commodity", "anomaly_type"], keep="first")
    .reset_index(drop=True)
)

st.sidebar.header("Filters")

bucket_options = ["HIGH", "MEDIUM", "EARLY"]
selected_buckets = st.sidebar.multiselect("Conviction", bucket_options, default=bucket_options)

trade_options = ["Long", "Short", "No Trade"]
selected_trades = st.sidebar.multiselect("Trade", trade_options, default=trade_options)

region_options = sorted(df["region"].dropna().astype(str).unique().tolist())
selected_regions = st.sidebar.multiselect("Region", region_options, default=region_options)

commodity_options = sorted(df["commodity"].dropna().astype(str).unique().tolist())
selected_commodities = st.sidebar.multiselect("Commodity", commodity_options, default=commodity_options)

min_signal = st.sidebar.slider("Minimum Signal", min_value=1, max_value=10, value=1)
top_n = st.sidebar.slider("Top Trades to Show", min_value=3, max_value=50, value=20)

filtered = df[
    df["signal_bucket"].isin(selected_buckets)
    & df["trade_display"].isin(selected_trades)
    & df["region"].astype(str).isin(selected_regions)
    & df["commodity"].astype(str).isin(selected_commodities)
    & (df["signal_level"] >= min_signal)
].copy()

filtered = filtered.sort_values(
    by=["signal_level", "persistence_score", "market_score", "severity_score", "created_at"],
    ascending=[False, False, False, False, False],
).reset_index(drop=True)

last_update = filtered["created_at"].max() if "created_at" in filtered.columns and not filtered.empty else None

long_df = filtered[filtered["trade_display"] == "Long"]
short_df = filtered[filtered["trade_display"] == "Short"]

top_long = build_title(long_df.iloc[0]) if not long_df.empty else "-"
top_short = build_title(short_df.iloc[0]) if not short_df.empty else "-"
high_count = int((filtered["signal_bucket"] == "HIGH").sum())

st.header("📡 Market Radar")
r1, r2, r3, r4 = st.columns(4)
r1.metric("Top Long", top_long)
r2.metric("Top Short", top_short)
r3.metric("High Conviction", high_count)
r4.metric("Last Update", format_dt(last_update))

st.header("🌍 Top Global Trades Right Now")

if filtered.empty:
    st.write("No trades match the current filters.")
else:
    top_df = filtered.head(top_n).copy()
    for idx, (_, row) in enumerate(top_df.iterrows(), start=1):
        show_trade_card(row, rank_number=idx)

st.header("📊 Ranking Table")

table_df = filtered[
    [
        "region",
        "commodity",
        "anomaly_type",
        "trade_display",
        "signal_bucket",
        "signal_level",
        "persistence_score",
        "market_score",
        "severity_score",
    ]
].copy()

table_df["Commodity Trade"] = filtered.apply(get_commodity_trade, axis=1)
table_df["Stock Trade"] = filtered.apply(get_stock_trade, axis=1)
table_df["Vehicle"] = filtered.apply(get_vehicle, axis=1)

table_df = table_df.rename(
    columns={
        "region": "Region",
        "commodity": "Commodity",
        "anomaly_type": "Anomaly",
        "trade_display": "Trade",
        "signal_bucket": "Conviction",
        "signal_level": "Signal",
        "persistence_score": "Persistence",
        "market_score": "Market",
        "severity_score": "Severity",
    }
)

table_df["Anomaly"] = table_df["Anomaly"].astype(str).str.replace("_", " ", regex=False).str.title()

st.dataframe(table_df, use_container_width=True)

with st.expander("Raw filtered table"):
    st.dataframe(filtered, use_container_width=True)
