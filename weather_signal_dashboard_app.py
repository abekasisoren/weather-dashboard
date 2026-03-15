import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")

st.title("Global Weather Signal Dashboard")

st.write("Simple view: what changed, how important it is, what to do")

try:
    signals = pd.read_csv("signals.csv")
except:
    st.error("signals.csv not found")
    st.stop()

# If score column exists, filter high signals
if "Score" in signals.columns:
    top_signals = signals[signals["Score"] >= 4]
else:
    top_signals = signals

st.header("Top Weather Signals")

if len(top_signals) == 0:
    st.write("No significant signals detected.")
else:
    for _, row in top_signals.iterrows():

        score = row.get("Score","N/A")
        region = row.get("Region","Unknown")
        event = row.get("WeatherEvent","")
        rec = row.get("Recommendation","")
        weather_logic = row.get("WeatherLogic","")
        market_logic = row.get("MarketLogic","")
        vehicle = row.get("BestVehicle","")

        st.markdown(f"### {region} — {event}")
        st.markdown(f"**Score:** {score}")
        st.markdown(f"**Action:** {rec}")

        st.markdown("**Weather Logic**")
        st.write(weather_logic)

        st.markdown("**Market Logic**")
        st.write(market_logic)

        st.markdown(f"**Best Trade Vehicle:** {vehicle}")

        st.divider()

st.header("Raw Signal Table")
st.dataframe(signals)
