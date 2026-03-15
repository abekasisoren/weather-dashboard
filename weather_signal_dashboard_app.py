import os

import pandas as pd
import psycopg
import streamlit as st

DATABASE_URL = os.environ.get("DATABASE_URL")

st.set_page_config(page_title="Global Weather Signal Dashboard", layout="wide")

st.title("Global Weather Signal Dashboard")
st.caption("Show only the important things: high-score signals and watchlist items with full logic.")

if not DATABASE_URL:
    st.error("DATABASE_URL environment variable is not set.")
    st.stop()


def read_sql(query):
    with psycopg.connect(DATABASE_URL) as conn:
        return pd.read_sql(query, conn)


try:
    signals = read_sql("""
        SELECT
            region,
            weather_event,
            score,
            recommendation,
            weather_logic,
            market_logic,
            best_vehicle,
            proxy_equities,
            updated_at
        FROM weather_signals
        ORDER BY score DESC, region ASC
    """)
except Exception as e:
    st.error(f"Could not read weather_signals from database: {e}")
    st.stop()

try:
    global_shocks = read_sql("""
        SELECT
            shock_type,
            macro_region,
            magnitude,
            score,
            recommendation,
            market,
            affected_industries,
            best_vehicle,
            proxy_equities,
            weather_logic,
            market_logic,
            updated_at
        FROM weather_global_shocks
        ORDER BY score DESC, macro_region ASC
    """)
except Exception:
    global_shocks = pd.DataFrame()

if not signals.empty:
    signals["score"] = pd.to_numeric(signals["score"], errors="coerce")

if not global_shocks.empty:
    global_shocks["score"] = pd.to_numeric(global_shocks["score"], errors="coerce")

last_updates = []
if not signals.empty and "updated_at" in signals.columns:
    last_updates.append(str(signals["updated_at"].max()))
if not global_shocks.empty and "updated_at" in global_shocks.columns:
    last_updates.append(str(global_shocks["updated_at"].max()))

if last_updates:
    st.write("Last update:", max(last_updates))

actionable_signals = signals[signals["score"] >= 8].copy() if not signals.empty else pd.DataFrame()
watchlist_signals = signals[(signals["score"] >= 6) & (signals["score"] < 8)].copy() if not signals.empty else pd.DataFrame()
actionable_shocks = global_shocks[global_shocks["score"] >= 8].copy() if not global_shocks.empty else pd.DataFrame()
watchlist_shocks = global_shocks[(global_shocks["score"] >= 6) & (global_shocks["score"] < 8)].copy() if not global_shocks.empty else pd.DataFrame()

st.header("🔥 Top Actionable Signals")

if actionable_signals.empty and actionable_shocks.empty:
    st.write("No high-conviction opportunities right now.")
else:
    if not actionable_signals.empty:
        st.subheader("Trade Signals")
        for _, row in actionable_signals.iterrows():
            st.markdown(f"### {row['region']} — {row['weather_event']}")
            c1, c2, c3 = st.columns(3)
            c1.metric("Score", f"{row['score']:.2f}")
            c2.metric("Action", row["recommendation"])
            c3.metric("Best Vehicle", row["best_vehicle"])

            with st.expander("Full explanation"):
                st.markdown("**Weather Logic**")
                st.write(row["weather_logic"])
                st.markdown("**Market Logic**")
                st.write(row["market_logic"])
                st.markdown("**Proxy Equities**")
                st.write(row["proxy_equities"])
            st.divider()

    if not actionable_shocks.empty:
        st.subheader("Global Shock Radar")
        for _, row in actionable_shocks.iterrows():
            st.markdown(f"### {row['macro_region']} — {str(row['shock_type']).upper()} shock")
            c1, c2, c3 = st.columns(3)
            c1.metric("Score", f"{row['score']:.2f}")
            c2.metric("Action", row["recommendation"])
            c3.metric("Best Vehicle", row["best_vehicle"])

            with st.expander("Full explanation"):
                st.markdown("**Weather Logic**")
                st.write(row["weather_logic"])
                st.markdown("**Market Logic**")
                st.write(row["market_logic"])
                st.markdown("**Affected Industries**")
                st.write(row["affected_industries"])
                st.markdown("**Proxy Equities**")
                st.write(row["proxy_equities"])
            st.divider()

st.header("👀 Watchlist")

if watchlist_signals.empty and watchlist_shocks.empty:
    st.write("No watchlist items right now.")
else:
    if not watchlist_signals.empty:
        st.subheader("Signal Watchlist")
        for _, row in watchlist_signals.iterrows():
            with st.expander(f"{row['region']} — {row['weather_event']} | Score {row['score']:.2f}"):
                st.markdown("**Weather Logic**")
                st.write(row["weather_logic"])
                st.markdown("**Market Logic**")
                st.write(row["market_logic"])
                st.markdown("**Best Vehicle**")
                st.write(row["best_vehicle"])
                st.markdown("**Proxy Equities**")
                st.write(row["proxy_equities"])

    if not watchlist_shocks.empty:
        st.subheader("Global Shock Watchlist")
        for _, row in watchlist_shocks.iterrows():
            with st.expander(f"{row['macro_region']} — {str(row['shock_type']).upper()} shock | Score {row['score']:.2f}"):
                st.markdown("**Weather Logic**")
                st.write(row["weather_logic"])
                st.markdown("**Market Logic**")
                st.write(row["market_logic"])
                st.markdown("**Affected Industries**")
                st.write(row["affected_industries"])
                st.markdown("**Best Vehicle**")
                st.write(row["best_vehicle"])
                st.markdown("**Proxy Equities**")
                st.write(row["proxy_equities"])

with st.expander("Raw weather_signals table"):
    st.dataframe(signals, use_container_width=True)

if not global_shocks.empty:
    with st.expander("Raw weather_global_shocks table"):
        st.dataframe(global_shocks, use_container_width=True)
