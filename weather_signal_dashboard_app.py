import os

import pandas as pd
import psycopg
import streamlit as st

DATABASE_URL = os.environ.get("DATABASE_URL")

st.set_page_config(page_title="Global Weather Signal Dashboard", layout="wide")

st.title("Global Weather Signal Dashboard")
st.caption("Early weather intelligence for markets: what changed, why it matters, and what to do.")

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


try:
    signals = read_sql("""
        SELECT
            region,
            weather_event,
            signal_level,
            recommendation,
            current_value,
            prior_avg_value,
            delta_value,
            what_changed,
            why_it_matters,
            affected_market,
            best_vehicle,
            proxy_equities,
            what_to_watch_next,
            updated_at
        FROM weather_signals
        ORDER BY
            CASE signal_level
                WHEN 'HIGH CONVICTION' THEN 1
                WHEN 'ACTIONABLE' THEN 2
                WHEN 'EARLY SIGNAL' THEN 3
                ELSE 4
            END,
            region ASC
    """)
except Exception as e:
    st.error(f"Could not read weather_signals from database: {e}")
    st.stop()

try:
    global_shocks = read_sql("""
        SELECT
            macro_region,
            shock_type,
            signal_level,
            recommendation,
            magnitude,
            what_changed,
            why_it_matters,
            affected_market,
            best_vehicle,
            proxy_equities,
            updated_at
        FROM weather_global_shocks
        ORDER BY
            CASE signal_level
                WHEN 'HIGH CONVICTION' THEN 1
                WHEN 'ACTIONABLE' THEN 2
                WHEN 'EARLY SIGNAL' THEN 3
                ELSE 4
            END,
            macro_region ASC
    """)
except Exception:
    global_shocks = pd.DataFrame()

last_updates = []
if not signals.empty and "updated_at" in signals.columns:
    last_updates.append(str(signals["updated_at"].max()))
if not global_shocks.empty and "updated_at" in global_shocks.columns:
    last_updates.append(str(global_shocks["updated_at"].max()))

if last_updates:
    st.write("Last update:", max(last_updates))


def show_signal_block(title, row):
    st.markdown(f"### {title}")

    c1, c2, c3 = st.columns(3)
    c1.metric("Signal Level", row["signal_level"])
    c2.metric("Action", row["recommendation"])
    c3.metric("Best Vehicle", row["best_vehicle"])

    with st.expander("Full explanation"):
        st.markdown("**What changed**")
        st.write(row["what_changed"])

        st.markdown("**Why it matters**")
        st.write(row["why_it_matters"])

        st.markdown("**Affected market**")
        st.write(row["affected_market"])

        st.markdown("**Proxy equities**")
        st.write(row["proxy_equities"])

        if "what_to_watch_next" in row.index:
            st.markdown("**What to watch next**")
            st.write(row["what_to_watch_next"])

    st.divider()


high_conviction_signals = signals[signals["signal_level"] == "HIGH CONVICTION"].copy() if not signals.empty else pd.DataFrame()
actionable_signals = signals[signals["signal_level"] == "ACTIONABLE"].copy() if not signals.empty else pd.DataFrame()
early_signals = signals[signals["signal_level"] == "EARLY SIGNAL"].copy() if not signals.empty else pd.DataFrame()

high_conviction_shocks = global_shocks[global_shocks["signal_level"] == "HIGH CONVICTION"].copy() if not global_shocks.empty else pd.DataFrame()
actionable_shocks = global_shocks[global_shocks["signal_level"] == "ACTIONABLE"].copy() if not global_shocks.empty else pd.DataFrame()
early_shocks = global_shocks[global_shocks["signal_level"] == "EARLY SIGNAL"].copy() if not global_shocks.empty else pd.DataFrame()

st.header("🔥 High Conviction")

if high_conviction_signals.empty and high_conviction_shocks.empty:
    st.write("No high-conviction weather trades right now.")
else:
    if not high_conviction_signals.empty:
        st.subheader("Industry / Stock Signals")
        for _, row in high_conviction_signals.iterrows():
            show_signal_block(f"{row['region']} — {row['weather_event']}", row)

    if not high_conviction_shocks.empty:
        st.subheader("Global Weather Radar")
        for _, row in high_conviction_shocks.iterrows():
            show_signal_block(f"{row['macro_region']} — {row['shock_type']}", row)

st.header("⚠️ Actionable")

if actionable_signals.empty and actionable_shocks.empty:
    st.write("No actionable setups right now.")
else:
    if not actionable_signals.empty:
        for _, row in actionable_signals.iterrows():
            show_signal_block(f"{row['region']} — {row['weather_event']}", row)

    if not actionable_shocks.empty:
        for _, row in actionable_shocks.iterrows():
            show_signal_block(f"{row['macro_region']} — {row['shock_type']}", row)

st.header("👀 Early Signals")

if early_signals.empty and early_shocks.empty:
    st.write("No early signals right now.")
else:
    if not early_signals.empty:
        for _, row in early_signals.iterrows():
            with st.expander(f"{row['region']} — {row['weather_event']}"):
                st.markdown("**What changed**")
                st.write(row["what_changed"])
                st.markdown("**Why it matters**")
                st.write(row["why_it_matters"])
                st.markdown("**Affected market**")
                st.write(row["affected_market"])
                st.markdown("**Best vehicle**")
                st.write(row["best_vehicle"])
                st.markdown("**Proxy equities**")
                st.write(row["proxy_equities"])
                st.markdown("**What to watch next**")
                st.write(row["what_to_watch_next"])

    if not early_shocks.empty:
        for _, row in early_shocks.iterrows():
            with st.expander(f"{row['macro_region']} — {row['shock_type']}"):
                st.markdown("**What changed**")
                st.write(row["what_changed"])
                st.markdown("**Why it matters**")
                st.write(row["why_it_matters"])
                st.markdown("**Affected market**")
                st.write(row["affected_market"])
                st.markdown("**Best vehicle**")
                st.write(row["best_vehicle"])
                st.markdown("**Proxy equities**")
                st.write(row["proxy_equities"])

with st.expander("Raw weather_signals table"):
    st.dataframe(signals, use_container_width=True)

if not global_shocks.empty:
    with st.expander("Raw weather_global_shocks table"):
        st.dataframe(global_shocks, use_container_width=True)
