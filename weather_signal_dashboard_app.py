import os

import pandas as pd
import psycopg
import streamlit as st

DATABASE_URL = os.environ.get("DATABASE_URL")

st.set_page_config(page_title="Global Weather Signal Dashboard", layout="wide")

st.title("Global Weather Signal Dashboard")
st.caption("Simple view: what changed, how important it is, what to do")

if not DATABASE_URL:
    st.error("DATABASE_URL environment variable is not set.")
    st.stop()

query = """
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
"""

try:
    with psycopg.connect(DATABASE_URL) as conn:
        signals = pd.read_sql(query, conn)
except Exception as e:
    st.error(f"Could not read signals from database: {e}")
    st.stop()

if signals.empty:
    st.warning("No signals found in database yet. Let the cron job run once, or trigger it manually.")
    st.stop()

signals["score"] = pd.to_numeric(signals["score"], errors="coerce")

top_signals = signals[signals["score"] >= 4].copy()

last_update = signals["updated_at"].max()
st.write("Last signal update:", str(last_update))

st.header("Top Weather Signals")

if top_signals.empty:
    st.write("No significant signals detected.")
else:
    for _, row in top_signals.iterrows():
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

st.header("Raw Signal Table")
st.dataframe(top_signals, use_container_width=True)
