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

    try:
        if pd.isna(value):
            return "-"
    except Exception:
        pass

    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() in {"nat", "nan", "none"}:
            return "-"
        try:
            parsed = pd.to_datetime(text, errors="coerce")
            if pd.isna(parsed):
                return text
            return parsed.strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            return text

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M UTC")

    try:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return "-"
        return parsed.strftime("%Y-%m-%d %H:%M UTC")
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
        text = value.strip()
        if not text:
            return {}
        try:
            return json.loads(text)
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


def normalize_text(value, fallback="-") -> str:
    if value is None:
        return fallback

    try:
        if pd.isna(value):
            return fallback
    except Exception:
        pass

    text = str(value).strip()
    if not text or text.lower() in {"nat", "nan", "none"}:
        return fallback
    return text


def score_bucket(score: int) -> str:
    if score >= 8:
        return "HIGH"
    if score >= 5:
        return "MEDIUM"
    return "EARLY"


def trade_label(value: str) -> str:
    v = str(value).strip().lower()
    if v == "bullish":
        return "Long"
    if v == "bearish":
        return "Short"
    if v == "long":
        return "Long"
    if v == "short":
        return "Short"
    return "No Trade"


def conviction_badge(bucket: str) -> str:
    bucket = normalize_text(bucket, "EARLY").upper()
    if bucket == "HIGH":
        return (
            "<span style='background:#7f1d1d;color:#fecaca;padding:4px 10px;"
            "border-radius:999px;font-weight:700;font-size:0.85rem;'>HIGH</span>"
        )
    if bucket == "MEDIUM":
        return (
            "<span style='background:#78350f;color:#fde68a;padding:4px 10px;"
            "border-radius:999px;font-weight:700;font-size:0.85rem;'>MEDIUM</span>"
        )
    return (
        "<span style='background:#334155;color:#cbd5e1;padding:4px 10px;"
        "border-radius:999px;font-weight:700;font-size:0.85rem;'>EARLY</span>"
    )


def trade_badge(trade: str) -> str:
    trade = normalize_text(trade, "No Trade")
    if trade == "Long":
        return (
            "<span style='background:#14532d;color:#bbf7d0;padding:4px 10px;"
            "border-radius:999px;font-weight:700;font-size:0.85rem;'>LONG</span>"
        )
    if trade == "Short":
        return (
            "<span style='background:#991b1b;color:#fecaca;padding:4px 10px;"
            "border-radius:999px;font-weight:700;font-size:0.85rem;'>SHORT</span>"
        )
    return (
        "<span style='background:#374151;color:#e5e7eb;padding:4px 10px;"
        "border-radius:999px;font-weight:700;font-size:0.85rem;'>NO TRADE</span>"
    )


def get_asset_list(row) -> list[dict]:
    payload = parse_jsonish(row.get("affected_assets_json"))
    if isinstance(payload, list):
        return payload
    return []


def summarize_assets(row) -> str:
    assets = get_asset_list(row)
    if assets:
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
        if labels:
            return ", ".join(labels)

    proxy = normalize_text(row.get("proxy_equities"), "")
    if proxy:
        return proxy

    vehicle = normalize_text(row.get("best_vehicle"), "")
    if vehicle:
        return vehicle

    return "-"


def compute_sort_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["signal_level", "persistence_score", "severity_score", "market_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        else:
            df[col] = 0
    return df


def build_title(row) -> str:
    region = normalize_text(row.get("region"))
    anomaly = normalize_text(row.get("anomaly_type")).replace("_", " ").title()
    commodity = normalize_text(row.get("commodity"))
    return f"{region} — {anomaly} — {commodity}"


def show_trade_card(row, rank_number=None):
    title = build_title(row)
    prefix = f"#{rank_number} " if rank_number is not None else ""
    trade = trade_label(row.get("trade_bias"))
    bucket = normalize_text(row.get("signal_bucket"), score_bucket(safe_int(row.get("signal_level"))))

    st.markdown(f"### {prefix}{title}")

    badge_col1, badge_col2 = st.columns([1, 5])
    with badge_col1:
        st.markdown(trade_badge(trade), unsafe_allow_html=True)
    with badge_col2:
        st.markdown(conviction_badge(bucket), unsafe_allow_html=True)

    c1, c2 = st.columns([2, 3])
    with c1:
        st.markdown(f"**Vehicle:** {normalize_text(row.get('best_vehicle'))}")
    with c2:
        st.markdown(f"**Assets:** {summarize_assets(row)}")

    st.markdown(f"**Why this matters:** {normalize_text(row.get('why_it_matters'))}")
    st.markdown(f"**Recommendation:** {normalize_text(row.get('recommendation'))}")
    st.markdown(f"**Forecast Window:** {format_dt(row.get('forecast_start'))} → {format_dt(row.get('forecast_end'))}")

    with st.expander("Full breakdown"):
        st.markdown("**What changed**")
        st.write(normalize_text(row.get("what_changed")))

        st.markdown("**Affected market**")
        st.write(normalize_text(row.get("affected_market")))

        st.markdown("**What to watch next**")
        st.write(normalize_text(row.get("what_to_watch_next")))

        st.markdown("**Secondary exposures**")
        st.write(normalize_text(row.get("secondary_exposures")))

        st.markdown("**Internal scores**")
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Signal", safe_int(row.get("signal_level")))
        s2.metric("Persistence", safe_int(row.get("persistence_score")))
        s3.metric("Severity", safe_int(row.get("severity_score")))
        s4.metric("Market", safe_int(row.get("market_score")))

        st.markdown("**Affected assets JSON**")
        assets = get_asset_list(row)
        if assets:
            st.json(assets)
        else:
            st.write("No asset payload available.")

        st.markdown("**Weather details**")
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

df = compute_sort_columns(df)
df["signal_bucket"] = df["signal_bucket"].fillna(df["signal_level"].apply(score_bucket))
df["trade_display"] = df["trade_bias"].apply(trade_label)

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
selected_buckets = st.sidebar.multiselect(
    "Conviction",
    options=bucket_options,
    default=bucket_options,
)

trade_options = ["Long", "Short", "No Trade"]
selected_trades = st.sidebar.multiselect(
    "Trade",
    options=trade_options,
    default=trade_options,
)

region_options = sorted(df["region"].dropna().astype(str).unique().tolist())
selected_regions = st.sidebar.multiselect(
    "Region",
    options=region_options,
    default=region_options,
)

commodity_options = sorted(df["commodity"].dropna().astype(str).unique().tolist())
selected_commodities = st.sidebar.multiselect(
    "Commodity",
    options=commodity_options,
    default=commodity_options,
)

min_signal = st.sidebar.slider("Minimum Signal", min_value=1, max_value=10, value=1)
top_n = st.sidebar.slider("Top Trades to Show", min_value=3, max_value=25, value=10)

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

last_update = None
if "created_at" in filtered.columns and not pd.isna(filtered["created_at"]).all():
    last_update = filtered["created_at"].max()
elif "created_at" in df.columns and not pd.isna(df["created_at"]).all():
    last_update = df["created_at"].max()

top_long = "-"
top_short = "-"
long_df = filtered[filtered["trade_display"] == "Long"]
short_df = filtered[filtered["trade_display"] == "Short"]

if not long_df.empty:
    top_long = build_title(long_df.iloc[0])
if not short_df.empty:
    top_short = build_title(short_df.iloc[0])

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

st.header("📊 Simple Ranking Table")

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
        "best_vehicle",
        "recommendation",
    ]
].copy()

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
        "best_vehicle": "Vehicle",
        "recommendation": "Recommendation",
    }
)

table_df["Anomaly"] = table_df["Anomaly"].astype(str).str.replace("_", " ", regex=False).str.title()

st.dataframe(table_df, use_container_width=True)

with st.expander("Raw filtered table"):
    st.dataframe(filtered, use_container_width=True)
