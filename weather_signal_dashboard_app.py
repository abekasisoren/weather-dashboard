import json
import os
from datetime import datetime

import pandas as pd
import psycopg
import streamlit as st

DATABASE_URL = os.environ.get("DATABASE_URL")

st.set_page_config(page_title="Global Weather Signal Dashboard", layout="wide")

st.title("Global Weather Signal Dashboard")
st.caption("Early weather intelligence for markets ranked by global tradability, not just geography.")

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


def parse_jsonish(value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
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


def compute_global_rank(row) -> float:
    signal_level = safe_int(row.get("signal_level"), 0)
    persistence = safe_int(row.get("persistence_score"), 0)
    severity = safe_int(row.get("severity_score"), 0)
    market_score = safe_int(row.get("market_score"), 0)

    # More weight on final signal and persistence
    return round(
        (signal_level * 0.50)
        + (persistence * 0.25)
        + (severity * 0.15)
        + (market_score * 0.10),
        2,
    )


def score_bucket(score: int) -> str:
    if score >= 8:
        return "HIGH CONVICTION"
    if score >= 5:
        return "ACTIONABLE"
    return "EARLY SIGNAL"


def normalize_text(value, fallback="-") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def get_asset_list(row) -> list[dict]:
    payload = parse_jsonish(row.get("affected_assets_json"))
    if isinstance(payload, list):
        return payload
    return []


def summarize_assets(row) -> str:
    assets = get_asset_list(row)
    if not assets:
        return normalize_text(row.get("proxy_equities"), "-")

    priority_order = {"primary": 0, "direct": 1, "secondary": 2}
    assets = sorted(
        assets,
        key=lambda x: (
            priority_order.get(str(x.get("priority", "")).lower(), 9),
            str(x.get("symbol", "")),
        ),
    )

    labels = []
    for a in assets[:10]:
        symbol = str(a.get("symbol", "")).strip()
        if symbol:
            labels.append(symbol)

    return ", ".join(labels) if labels else "-"


def build_top_trade_label(row) -> str:
    return (
        f"{normalize_text(row.get('region'))} — "
        f"{normalize_text(row.get('anomaly_type')).replace('_', ' ').title()} — "
        f"{normalize_text(row.get('commodity'))}"
    )


def show_trade_card(row, rank_number=None):
    title = build_top_trade_label(row)
    rank_prefix = f"#{rank_number} " if rank_number is not None else ""
    st.markdown(f"### {rank_prefix}{title}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Global Rank Score", row["global_rank_score"])
    c2.metric("Signal Level", safe_int(row["signal_level"]))
    c3.metric("Bucket", normalize_text(row["signal_bucket"], score_bucket(safe_int(row["signal_level"]))))
    c4.metric("Trade Bias", normalize_text(row["trade_bias"]).title())

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Persistence", safe_int(row["persistence_score"]))
    c6.metric("Severity", safe_int(row["severity_score"]))
    c7.metric("Market Score", safe_int(row["market_score"]))
    c8.metric("Best Vehicle", normalize_text(row["best_vehicle"]))

    st.markdown(f"**Recommendation:** {normalize_text(row.get('recommendation'))}")
    st.markdown(f"**Affected Market:** {normalize_text(row.get('affected_market'))}")
    st.markdown(f"**Proxy Equities / Assets:** {summarize_assets(row)}")
    st.markdown(f"**Forecast Window:** {format_dt(row.get('forecast_start'))} → {format_dt(row.get('forecast_end'))}")

    with st.expander("Full trade breakdown"):
        st.markdown("**What changed**")
        st.write(normalize_text(row.get("what_changed")))

        st.markdown("**Why it matters**")
        st.write(normalize_text(row.get("why_it_matters")))

        st.markdown("**What to watch next**")
        st.write(normalize_text(row.get("what_to_watch_next")))

        st.markdown("**Secondary Exposures**")
        st.write(normalize_text(row.get("secondary_exposures")))

        st.markdown("**Affected Assets JSON**")
        assets = get_asset_list(row)
        if assets:
            st.json(assets)
        else:
            st.write("No asset payload available.")

        st.markdown("**Weather Details**")
        details = parse_jsonish(row.get("details"))
        if isinstance(details, dict) and details:
            st.json(details)
        else:
            st.write("No details available.")

    st.divider()


try:
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
except Exception as e:
    st.error(f"Could not read weather_global_shocks from database: {e}")
    st.stop()

if df.empty:
    st.warning("No weather shocks found yet.")
    st.stop()

for col in ["signal_level", "persistence_score", "severity_score", "market_score"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

df["global_rank_score"] = df.apply(compute_global_rank, axis=1)

# Keep only strongest recent duplicate per region/commodity/anomaly
df = (
    df.sort_values(
        by=["global_rank_score", "signal_level", "created_at"],
        ascending=[False, False, False],
    )
    .drop_duplicates(subset=["region", "commodity", "anomaly_type"], keep="first")
    .reset_index(drop=True)
)

# Sidebar filters
st.sidebar.header("Filters")

all_buckets = ["HIGH CONVICTION", "ACTIONABLE", "EARLY SIGNAL"]
bucket_filter = st.sidebar.multiselect(
    "Signal Bucket",
    options=all_buckets,
    default=all_buckets,
)

all_biases = sorted([b for b in df["trade_bias"].dropna().astype(str).unique().tolist() if b])
bias_filter = st.sidebar.multiselect(
    "Trade Bias",
    options=all_biases,
    default=all_biases,
)

all_regions = sorted(df["region"].dropna().astype(str).unique().tolist())
region_filter = st.sidebar.multiselect(
    "Region",
    options=all_regions,
    default=all_regions,
)

all_commodities = sorted(df["commodity"].dropna().astype(str).unique().tolist())
commodity_filter = st.sidebar.multiselect(
    "Commodity",
    options=all_commodities,
    default=all_commodities,
)

min_signal = st.sidebar.slider("Minimum Signal Level", min_value=1, max_value=10, value=1)
top_n = st.sidebar.slider("Show Top Global Trades", min_value=3, max_value=25, value=10)

filtered = df.copy()
filtered["signal_bucket"] = filtered["signal_bucket"].fillna(filtered["signal_level"].apply(score_bucket))

filtered = filtered[
    filtered["signal_bucket"].isin(bucket_filter)
    & filtered["trade_bias"].astype(str).isin(bias_filter)
    & filtered["region"].astype(str).isin(region_filter)
    & filtered["commodity"].astype(str).isin(commodity_filter)
    & (filtered["signal_level"] >= min_signal)
].copy()

filtered = filtered.sort_values(
    by=["global_rank_score", "signal_level", "persistence_score", "market_score"],
    ascending=[False, False, False, False],
).reset_index(drop=True)

last_update = None
if "created_at" in filtered.columns and not filtered["created_at"].isna().all():
    last_update = filtered["created_at"].max()
elif "created_at" in df.columns and not df["created_at"].isna().all():
    last_update = df["created_at"].max()

if last_update is not None:
    st.write("Last update:", format_dt(last_update))

# Summary metrics
high_count = int((filtered["signal_level"] >= 8).sum())
actionable_count = int(((filtered["signal_level"] >= 5) & (filtered["signal_level"] < 8)).sum())
early_count = int((filtered["signal_level"] < 5).sum())
bullish_count = int((filtered["trade_bias"].astype(str) == "bullish").sum())
bearish_count = int((filtered["trade_bias"].astype(str) == "bearish").sum())

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Filtered Trades", len(filtered))
m2.metric("High Conviction", high_count)
m3.metric("Actionable", actionable_count)
m4.metric("Bullish", bullish_count)
m5.metric("Bearish", bearish_count)

st.header("🌍 Top Global Trades Right Now")

if filtered.empty:
    st.write("No trades match the current filters.")
else:
    top_global = filtered.head(top_n).copy()
    for idx, (_, row) in enumerate(top_global.iterrows(), start=1):
        show_trade_card(row, rank_number=idx)

st.header("📊 Global Ranking Table")

ranking_table = filtered[
    [
        "region",
        "commodity",
        "anomaly_type",
        "trade_bias",
        "signal_bucket",
        "signal_level",
        "persistence_score",
        "severity_score",
        "market_score",
        "global_rank_score",
        "best_vehicle",
        "proxy_equities",
        "recommendation",
    ]
].copy()

ranking_table = ranking_table.rename(
    columns={
        "region": "Region",
        "commodity": "Commodity",
        "anomaly_type": "Anomaly",
        "trade_bias": "Bias",
        "signal_bucket": "Bucket",
        "signal_level": "Signal",
        "persistence_score": "Persistence",
        "severity_score": "Severity",
        "market_score": "Market",
        "global_rank_score": "Rank Score",
        "best_vehicle": "Best Vehicle",
        "proxy_equities": "Proxy Equities",
        "recommendation": "Recommendation",
    }
)

st.dataframe(ranking_table, use_container_width=True)

st.header("🔥 High Conviction Only")
high_conviction = filtered[filtered["signal_level"] >= 8].copy()
if high_conviction.empty:
    st.write("No high-conviction trades right now.")
else:
    for _, row in high_conviction.iterrows():
        with st.expander(build_top_trade_label(row)):
            st.write(normalize_text(row.get("recommendation")))
            st.write(normalize_text(row.get("what_changed")))
            st.write(normalize_text(row.get("why_it_matters")))
            st.markdown(f"**Assets:** {summarize_assets(row)}")

with st.expander("Raw filtered table"):
    st.dataframe(filtered, use_container_width=True)
    
