import streamlit as st
import pandas as pd
from pathlib import Path
import os
import datetime
import subprocess
import sys

st.set_page_config(page_title="Weather Trading Dashboard", layout="wide")

st.title("Global Weather Signal Dashboard")
st.caption("Simple view: what changed, how important it is, what to do")

signals_file = Path("signals.csv")
history_file = Path("weather_history.csv")
market_map_file = Path("weather_market_map.csv")
base_dir = Path(__file__).resolve().parent


def show_list_block(title, value):
    st.write(f"**{title}:**")
    if pd.isna(value) or str(value).strip() == "" or str(value).strip().lower() == "none":
        st.write("None")
        return
    items = [x.strip() for x in str(value).split(",") if x.strip()]
    for item in items:
        st.write(f"- {item}")


def action_badge(action):
    action = str(action).upper()
    if action == "TRADE":
        return "🔴 TRADE"
    if action == "WATCH":
        return "🟡 WATCH"
    return "⚪ IGNORE"


def shock_strength(abs_change):
    if abs_change < 2:
        return "Minor"
    if abs_change < 5:
        return "Moderate"
    if abs_change < 12:
        return "Strong"
    return "Major"


def shock_emoji(abs_change):
    if abs_change < 2:
        return "⚪"
    if abs_change < 5:
        return "🟡"
    if abs_change < 12:
        return "🟠"
    return "🔴"


def shock_note(metric, change):
    metric_lower = metric.lower()

    if "storm" in metric_lower:
        return "storm risk increasing" if change > 0 else "storm risk easing"

    if "temp" in metric_lower:
        return "forecast turning hotter" if change > 0 else "forecast turning colder"

    if "hotdry" in metric_lower:
        return "dryness / crop stress increasing" if change > 0 else "dryness / crop stress easing"

    if "precip" in metric_lower or "rain" in metric_lower:
        return "rainfall forecast increasing" if change > 0 else "rainfall forecast decreasing"

    return "meaningful forecast change"


def run_refresh():
    commands = [
        [sys.executable, "update_weather_values.py"],
        [sys.executable, "generate_signals.py"],
    ]

    outputs = []

    for cmd in commands:
        result = subprocess.run(
            cmd,
            cwd=base_dir,
            capture_output=True,
            text=True
        )
        outputs.append(
            {
                "cmd": " ".join(cmd),
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        )

        if result.returncode != 0:
            return False, outputs

    return True, outputs


region_metric_map = {
    "gulf_storm_index": ("Gulf of Mexico", "Storm Index"),
    "us_east_coast_storm_index": ("US East Coast", "Storm Index"),
    "north_sea_storm_index": ("North Sea", "Storm Index"),
    "china_east_storm_index": ("East China", "Storm Index"),
    "texas_mean_temp_c": ("Texas", "Temperature"),
    "california_mean_temp_c": ("California", "Temperature"),
    "nw_europe_mean_temp_c": ("Northwest Europe", "Temperature"),
    "cornbelt_hotdry_score": ("US Corn Belt", "Hot/Dry Score"),
    "us_wheat_hotdry_score": ("US Wheat Plains", "Hot/Dry Score"),
    "argentina_soy_hotdry_score": ("Argentina Pampas", "Hot/Dry Score"),
    "black_sea_hotdry_score": ("Black Sea Grain Region", "Hot/Dry Score"),
    "brazil_coffee_precip_mm": ("Brazil Coffee Belt", "Rainfall"),
    "west_africa_cocoa_precip_mm": ("West Africa Cocoa Belt", "Rainfall"),
    "india_monsoon_precip_mm": ("India Monsoon Belt", "Rainfall"),
    "canadian_prairies_hotdry_score": ("Canadian Prairies", "Hot/Dry Score"),
    "mato_grosso_hotdry_score": ("Mato Grosso", "Hot/Dry Score"),
    "rhine_corridor_precip_mm": ("Rhine Corridor", "Rainfall"),
    "rhine_corridor_storm_index": ("Rhine Corridor", "Storm Index"),
    "panama_canal_precip_mm": ("Panama Canal", "Rainfall"),
    "sea_palm_oil_precip_mm": ("SE Asia Palm Oil Belt", "Rainfall"),
    "sea_palm_oil_hotdry_score": ("SE Asia Palm Oil Belt", "Hot/Dry Score"),
}

# Refresh button
col1, col2 = st.columns([1, 5])
with col1:
    if st.button("Refresh Data", type="primary"):
        with st.spinner("Refreshing ECMWF data and regenerating signals..."):
            ok, outputs = run_refresh()

        if ok:
            st.success("Data refreshed successfully.")
            st.rerun()
        else:
            st.error("Refresh failed.")
            for item in outputs:
                st.write(f"**Command:** `{item['cmd']}`")
                if item["stdout"]:
                    st.code(item["stdout"])
                if item["stderr"]:
                    st.code(item["stderr"])

# Last update
if signals_file.exists():
    last_update = datetime.datetime.fromtimestamp(os.path.getmtime(signals_file))
    st.write("Last model update:", last_update)

if not signals_file.exists():
    st.error("signals.csv not found. Run: python3 generate_signals.py")
    st.stop()

df = pd.read_csv(signals_file)

if market_map_file.exists():
    market_map = pd.read_csv(market_map_file)
else:
    market_map = pd.DataFrame(
        columns=[
            "Region",
            "WeatherEvent",
            "BestVehicle",
            "PrimaryExpressions",
            "SecondaryEquityProxies",
        ]
    )

# Weather summary
st.subheader("Weather Summary")

if not df.empty:
    summaries = []

    for _, row in df.iterrows():
        action = str(row.get("Action", "")).upper()
        region = row.get("Region", "")
        event = row.get("WeatherEvent", "")

        if action == "TRADE":
            summaries.append(f"{event.lower()} in {region}")
        elif action == "WATCH":
            summaries.append(f"watching {event.lower()} in {region}")

    if summaries:
        text = "; ".join(summaries[:3])
        st.write(f"ECMWF forecast signals: {text}.")
    else:
        st.write("No significant weather forecast changes detected today.")

# Biggest weather shocks
if history_file.exists():
    hist = pd.read_csv(history_file).sort_values(["run_date", "run_time"]).reset_index(drop=True)

    if len(hist) >= 2:
        st.subheader("Biggest Weather Shocks")

        first_row = hist.iloc[0]
        last_row = hist.iloc[-1]
        prev_row = hist.iloc[-2]

        shock_rows = []

        for metric, (region, label) in region_metric_map.items():
            if metric in hist.columns:
                current = float(last_row[metric])
                previous = float(prev_row[metric])
                change_1 = current - previous
                change_5 = current - float(first_row[metric])
                abs_change = abs(change_5)

                shock_rows.append({
                    "Region": region,
                    "Metric": label,
                    "Current": round(current, 2),
                    "Change1": round(change_1, 2),
                    "Change5": round(change_5, 2),
                    "AbsChange": abs_change,
                    "Strength": shock_strength(abs_change),
                    "Note": shock_note(metric, change_5),
                    "Emoji": shock_emoji(abs_change),
                })

        shock_df = pd.DataFrame(shock_rows).sort_values("AbsChange", ascending=False).head(3)

        for i, (_, row) in enumerate(shock_df.iterrows(), start=1):
            st.markdown(
                f"**{i}. {row['Emoji']} {row['Region']} — {row['Metric']}** "
                f"({row['Strength']})"
            )
            st.write(
                f"Now: {row['Current']} | "
                f"Last run: {row['Change1']:+.2f} | "
                f"5-run change: {row['Change5']:+.2f}"
            )
            st.write(f"Meaning: {row['Note']}.")
            st.markdown("---")

# Signal board
st.subheader("Signal Board")

signal_columns = [
    "Region",
    "WeatherEvent",
    "SignalStrength",
    "Action",
    "TradeIdea",
    "AffectedArea",
    "AffectedIndustries",
    "ForecastChange",
]

existing_signal_columns = [c for c in signal_columns if c in df.columns]

display_df = df[existing_signal_columns].copy()

if "Action" in display_df.columns:
    display_df["Action"] = display_df["Action"].apply(action_badge)

st.dataframe(display_df, use_container_width=True)

# Detailed reasoning
st.subheader("Detailed Reasoning")

for _, row in df.iterrows():
    title = f"{row['Region']} — {row['WeatherEvent']}"
    with st.expander(title, expanded=False):
        st.write(f"**Action:** {action_badge(row['Action'])}")
        st.write(f"**Signal strength:** {row['SignalStrength']}")
        st.write(f"**Trade idea:** {row['TradeIdea']}")
        st.write(f"**Affected area:** {row['AffectedArea']}")
        st.write(f"**Affected industries:** {row['AffectedIndustries']}")
        st.write(f"**Forecast change:** {row['ForecastChange']}")

        match = market_map[
            (market_map["Region"] == row["Region"]) &
            (market_map["WeatherEvent"] == row["WeatherEvent"])
        ]

        if not match.empty:
            best_vehicle = match.iloc[0]["BestVehicle"]
            primary_expressions = match.iloc[0]["PrimaryExpressions"]
            secondary_proxies = match.iloc[0]["SecondaryEquityProxies"]

            st.markdown("### Best way to express the view")
            st.write(f"**Best vehicle:** {best_vehicle}")

            st.write("**Primary expressions:**")
            for x in str(primary_expressions).split(","):
                item = x.strip()
                if item:
                    st.write(f"- {item}")

            st.write("**Secondary equity proxies:**")
            for x in str(secondary_proxies).split(","):
                item = x.strip()
                if item:
                    st.write(f"- {item}")

        if "PrimaryStock" in df.columns:
            st.write(f"**Primary stock:** {row['PrimaryStock']}")
        if "PrimaryETF" in df.columns:
            st.write(f"**Primary ETF:** {row['PrimaryETF']}")
        if "PrimaryFutures" in df.columns:
            st.write(f"**Primary futures / commodity:** {row['PrimaryFutures']}")

        st.markdown("### Full exposure basket")
        if "LongStocks" in df.columns:
            show_list_block("Long stocks", row["LongStocks"])
        if "ShortStocks" in df.columns:
            show_list_block("Short stocks", row["ShortStocks"])
        if "LongETFs" in df.columns:
            show_list_block("Long ETFs", row["LongETFs"])
        if "ShortETFs" in df.columns:
            show_list_block("Short ETFs", row["ShortETFs"])

        st.markdown("### Why the model cares")
        if "WeatherReasoning" in df.columns:
            st.write(f"**Weather:** {row['WeatherReasoning']}")
        if "MarketReasoning" in df.columns:
            st.write(f"**Market:** {row['MarketReasoning']}")
        if "RiskNote" in df.columns:
            st.write(f"**Risk note:** {row['RiskNote']}")
            st.markdown("---")
st.header("🌍 Global Weather Shock Scanner")

try:

    shocks = pd.read_csv("global_shocks.csv")

    if len(shocks) == 0:
        st.info("No major global weather shocks detected.")
    else:

        shocks = shocks.sort_values("TradePriority", ascending=False)

        for _, row in shocks.iterrows():

            priority = int(row["TradePriority"])

            if priority == 4:
                badge = "🔥 Major opportunity"
            elif priority == 3:
                badge = "⚠️ Strong signal"
            elif priority == 2:
                badge = "👀 Watch"
            else:
                badge = "Low impact"

            with st.expander(
                f"{badge} — {row['type'].upper()} shock in {row['macro_region']} "
                f"(priority {priority}/4)"
            ):

                st.write("**Forecast change:**", row["type"], "shock")
                st.write("**Location:**", f"{row['lat']}, {row['lon_normal']}")
                st.write("**Magnitude:**", row["value"])

                st.write("### Market impact")
                st.write("**Theme:**", row["Market"])

                st.write("**Industries likely affected:**")
                st.write(row["AffectedIndustries"])

                st.write("### Best way to express the view")
                st.write(row["BestVehicle"])

except Exception as e:
    st.warning("Global shock scanner not available yet.")
