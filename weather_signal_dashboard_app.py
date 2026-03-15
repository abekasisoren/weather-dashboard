import streamlit as st
import pandas as pd

st.set_page_config(page_title="Global Weather Signal Dashboard", layout="wide")

st.title("Global Weather Signal Dashboard")
st.caption("Simple view: what changed, how important it is, what to do")

# Load signals
signals = pd.read_csv("signals.csv")

# Filter meaningful signals only
signals = signals[signals["Score"] >= 6]

signals = signals.sort_values(by="Score", ascending=False)

st.header("Top Weather Signals")

if signals.empty:
    st.write("No significant signals detected.")
else:

    for _, row in signals.iterrows():

        score = row["Score"]

        if score >= 8:
            action_color = "red"
        else:
            action_color = "orange"

        st.markdown(f"### {row['Region']} — {row['WeatherEvent']}")

        col1, col2, col3 = st.columns(3)

        col1.metric("Score", round(score,2))
        col2.metric("Recommendation", row["Recommendation"])
        col3.metric("Vehicle", row["BestVehicle"])

        with st.expander("Why this signal appeared"):

            st.write("Weather Logic")
            st.write(row["WeatherLogic"])

            st.write("Market Logic")
            st.write(row["MarketLogic"])

            st.write("Best Trade Vehicle")
            st.write(row["BestVehicle"])

            st.write("Proxy Equities")
            st.write(row["ProxyEquities"])

        st.divider()

st.header("Raw Signal Table")

st.dataframe(signals)
