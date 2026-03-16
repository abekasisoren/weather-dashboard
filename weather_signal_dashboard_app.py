import json
import os

import pandas as pd
import psycopg
import streamlit as st

from weather_market_map import get_best_trade_expressions, get_event_candidates, get_event_market_map

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


def format_date_only(value) -> str:
    if is_missing(value):
        return "-"
    try:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return str(value)
        return parsed.strftime("%Y-%m-%d")
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


def clamp(value: float, low: float = 0.0, high: float = 10.0) -> float:
    return max(low, min(high, value))


def score_bucket(score: int) -> str:
    if score >= 9:
        return "PRIME"
    if score >= 7:
        return "ACTIONABLE"
    if score >= 5:
        return "WATCH"
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
        "extreme_heat": "extreme_heat",
        "heatwave": "heatwave",
        "frost": "frost",
        "cold_wave": "cold_wave",
        "heavy_rain": "heavy_rain",
        "flood_risk": "flood_risk",
        "flood": "flood",
        "storm_wind": "storm_wind",
        "wildfire_risk": "wildfire_risk",
        "wildfire": "wildfire",
        "hurricane_risk": "hurricane_risk",
        "hurricane": "hurricane",
        "drought": "drought",
        "tornado": "tornado",
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
    event_map = get_event_market_map(anomaly)

    long_names = event_map.get("equities_long_tier1", []) or event_map.get("equities_long", [])
    short_names = event_map.get("equities_short_tier1", []) or event_map.get("equities_short", [])

    if long_names and not short_names:
        return "Long"
    if short_names and not long_names:
        return "Short"

    ctype = commodity_context_type(row)
    commodity = normalize_text(row.get("commodity"), "")

    if anomaly in {"heatwave", "extreme_heat", "drought"} and ctype in {"ag", "energy", "utilities"}:
        return "Long"

    if anomaly in {"cold_wave", "frost"} and commodity in {"Natural Gas", "Power Utilities", "Wheat"}:
        return "Long"

    if anomaly in {"heavy_rain", "flood_risk", "flood"} and ctype == "ag":
        return "Short"

    if anomaly in {"heavy_rain", "flood_risk", "flood"} and ctype in {"energy", "utilities"}:
        return "Long"

    if anomaly in {"storm_wind", "hurricane_risk", "hurricane", "wildfire_risk", "wildfire"} and ctype in {"energy", "utilities"}:
        return "Long"

    return "No Trade"


def conviction_badge(bucket: str) -> str:
    bucket = normalize_text(bucket, "EARLY").upper()
    if bucket == "PRIME":
        return "<span style='background:#4c1d95;color:#e9d5ff;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>PRIME</span>"
    if bucket == "ACTIONABLE":
        return "<span style='background:#14532d;color:#bbf7d0;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>ACTIONABLE</span>"
    if bucket == "WATCH":
        return "<span style='background:#78350f;color:#fde68a;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>WATCH</span>"
    if bucket == "MIXED":
        return "<span style='background:#312e81;color:#c7d2fe;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>MIXED</span>"
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
    anomaly = normalize_anomaly_key(row.get("anomaly_type"))
    trade_map = get_best_trade_expressions(anomaly)
    vehicle = normalize_text(trade_map.get("preferred_vehicle"), "")
    if vehicle and vehicle != "-":
        return vehicle

    commodity = normalize_text(row.get("commodity"), "")
    fallback = {
        "Corn": "CORN",
        "Soybeans": "SOYB",
        "Wheat": "WEAT",
        "Coffee": "JO",
        "Sugar": "CANE",
        "Natural Gas": "UNG",
        "Oil": "USO",
        "Power Utilities": "XLU",
        "Coal": "KOL",
        "Rice": "DBA",
    }
    return fallback.get(commodity, commodity if commodity else "-")


def get_symbol_candidate(row, symbol: str, direction: str):
    anomaly = normalize_anomaly_key(row.get("anomaly_type"))
    candidates = get_event_candidates(anomaly, direction=direction.lower(), max_tier=3)
    for candidate in candidates:
        if candidate.get("symbol") == symbol:
            return candidate
    return None


def get_stock_trade_symbols(row) -> tuple[str, list[str]]:
    anomaly = normalize_anomaly_key(row.get("anomaly_type"))
    trade = infer_trade(row)
    trade_map = get_best_trade_expressions(anomaly)

    if trade == "Long":
        names = trade_map.get("best_longs", [])[:3]
        return "Long", names

    if trade == "Short":
        names = trade_map.get("best_shorts", [])[:3]
        return "Short", names

    return "No Trade", []


def get_stock_trade(row) -> str:
    trade, names = get_stock_trade_symbols(row)
    if not names:
        return "-"
    return f"{trade} {', '.join(names)}"


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

    if anomaly in {"heatwave", "extreme_heat"}:
        if temp_max is not None:
            evidence.append(f"Max temp: {temp_max:.1f}°C")
        if temp_mean is not None:
            evidence.append(f"Mean temp: {temp_mean:.1f}°C")

    if anomaly == "drought":
        if precip is not None:
            evidence.append(f"Rainfall: {precip:.1f} mm")
        if temp_mean is not None:
            evidence.append(f"Mean temp: {temp_mean:.1f}°C")

    if anomaly in {"frost", "cold_wave"}:
        if temp_min is not None:
            evidence.append(f"Min temp: {temp_min:.1f}°C")

    if anomaly in {"heavy_rain", "flood_risk", "flood"}:
        if precip is not None:
            evidence.append(f"Rainfall: {precip:.1f} mm")

    if anomaly in {"storm_wind", "hurricane_risk", "hurricane", "wildfire_risk", "wildfire"}:
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


def compute_weather_strength(row) -> float:
    signal = safe_int(row.get("signal_level"))
    persistence = safe_int(row.get("persistence_score"))
    severity = safe_int(row.get("severity_score"))
    market = safe_int(row.get("market_score"))

    base = (
        0.40 * signal +
        0.20 * persistence +
        0.20 * severity +
        0.20 * market
    )
    return round(clamp(base), 2)


def compute_mapping_quality(row, symbol: str, direction: str) -> float:
    candidate = get_symbol_candidate(row, symbol, direction)
    if not candidate:
        return 4.0

    tier = safe_int(candidate.get("tier"), 3)
    directness = safe_float(candidate.get("directness"), 0.35)

    tier_score_map = {
        1: 9.5,
        2: 7.0,
        3: 4.0,
    }
    tier_score = tier_score_map.get(tier, 4.0)
    directness_score = clamp(directness * 10.0)

    score = 0.60 * tier_score + 0.40 * directness_score
    return round(clamp(score), 2)


def compute_execution_quality(row, symbol: str) -> float:
    vehicle = get_vehicle(row)
    candidate = get_symbol_candidate(row, symbol, "long") or get_symbol_candidate(row, symbol, "short")

    if vehicle in {"CORN", "SOYB", "WEAT", "JO", "CANE", "UNG", "USO", "XLU", "KOL", "DBA"}:
        vehicle_score = 9.5
    elif vehicle not in {"-", ""}:
        vehicle_score = 8.0
    else:
        vehicle_score = 6.0

    if candidate:
        tier = safe_int(candidate.get("tier"), 3)
        directness = safe_float(candidate.get("directness"), 0.35)
        if tier == 1 and directness >= 0.85:
            symbol_score = 9.0
        elif tier <= 2 and directness >= 0.60:
            symbol_score = 7.5
        else:
            symbol_score = 5.5
    else:
        symbol_score = 6.0

    return round(clamp(0.50 * vehicle_score + 0.50 * symbol_score), 2)


def compute_conflict_cleanliness(long_score: int, short_score: int) -> float:
    if long_score > 0 and short_score == 0:
        return 10.0
    if short_score > 0 and long_score == 0:
        return 10.0
    if long_score == 0 and short_score == 0:
        return 0.0

    gross = long_score + short_score
    if gross <= 0:
        return 0.0

    dominance = abs(long_score - short_score) / gross
    score = dominance * 10.0
    return round(clamp(score), 2)


def compute_final_trade_score(weather_strength: float, mapping_quality: float, conflict_cleanliness: float, execution_quality: float) -> float:
    score = (
        0.45 * weather_strength +
        0.25 * mapping_quality +
        0.20 * conflict_cleanliness +
        0.10 * execution_quality
    )
    return round(clamp(score), 2)


def build_global_pulse_trader_table(df: pd.DataFrame) -> pd.DataFrame:
    raw_rows = []

    for _, row in df.iterrows():
        trade, symbols = get_stock_trade_symbols(row)
        if not symbols:
            continue

        signal = safe_int(row.get("signal_level"))
        weather_strength = compute_weather_strength(row)

        for symbol in symbols:
            mapping_quality = compute_mapping_quality(row, symbol, trade)
            execution_quality = compute_execution_quality(row, symbol)

            raw_rows.append(
                {
                    "Date": format_date_only(
                        row.get("created_at") if not is_missing(row.get("created_at")) else row.get("timestamp")
                    ),
                    "Symbol": symbol,
                    "Trade": trade,
                    "Raw Signal": signal,
                    "Weather Strength": weather_strength,
                    "Mapping Quality": mapping_quality,
                    "Execution Quality": execution_quality,
                    "Why": get_why_it_matters(row),
                    "Region": normalize_text(row.get("region")),
                    "Commodity": normalize_text(row.get("commodity")),
                    "Anomaly": normalize_text(row.get("anomaly_type")).replace("_", " ").title(),
                    "Vehicle": get_vehicle(row),
                    "Commodity Trade": get_commodity_trade(row),
                }
            )

    if not raw_rows:
        return pd.DataFrame()

    raw_df = pd.DataFrame(raw_rows)
    final_rows = []

    for symbol, group in raw_df.groupby("Symbol", sort=False):
        long_group = group[group["Trade"] == "Long"]
        short_group = group[group["Trade"] == "Short"]

        long_score = int(long_group["Raw Signal"].sum()) if not long_group.empty else 0
        short_score = int(short_group["Raw Signal"].sum()) if not short_group.empty else 0
        net_score = long_score - short_score
        cleanliness = compute_conflict_cleanliness(long_score, short_score)

        if long_score == 0 and short_score == 0:
            continue

        if net_score >= 3 and not long_group.empty:
            final_trade = "Long"
            winner = long_group.sort_values(
                by=["Weather Strength", "Mapping Quality", "Execution Quality", "Raw Signal"],
                ascending=[False, False, False, False],
            ).iloc[0]
            mapping_quality = float(winner["Mapping Quality"])
            execution_quality = float(winner["Execution Quality"])
            weather_strength = float(winner["Weather Strength"])
            why = winner["Why"]

        elif net_score <= -3 and not short_group.empty:
            final_trade = "Short"
            winner = short_group.sort_values(
                by=["Weather Strength", "Mapping Quality", "Execution Quality", "Raw Signal"],
                ascending=[False, False, False, False],
            ).iloc[0]
            mapping_quality = float(winner["Mapping Quality"])
            execution_quality = float(winner["Execution Quality"])
            weather_strength = float(winner["Weather Strength"])
            why = winner["Why"]

        else:
            winner = group.sort_values(
                by=["Weather Strength", "Mapping Quality", "Execution Quality", "Raw Signal"],
                ascending=[False, False, False, False],
            ).iloc[0]
            final_trade = "No Trade"
            mapping_quality = float(winner["Mapping Quality"])
            execution_quality = float(winner["Execution Quality"])
            weather_strength = float(winner["Weather Strength"])

            long_reason = long_group.sort_values("Raw Signal", ascending=False).iloc[0]["Why"] if not long_group.empty else ""
            short_reason = short_group.sort_values("Raw Signal", ascending=False).iloc[0]["Why"] if not short_group.empty else ""

            if long_reason and short_reason:
                why = f"Conflicting weather signals. Bullish case: {long_reason} Bearish case: {short_reason}"
            elif long_reason:
                why = f"Conflicting setup, but bullish signals are present: {long_reason}"
            elif short_reason:
                why = f"Conflicting setup, but bearish signals are present: {short_reason}"
            else:
                why = "Conflicting weather signals."

        final_trade_score = compute_final_trade_score(
            weather_strength=weather_strength,
            mapping_quality=mapping_quality,
            conflict_cleanliness=cleanliness,
            execution_quality=execution_quality,
        )

        conviction = "MIXED" if final_trade == "No Trade" else score_bucket(int(round(final_trade_score)))

        final_rows.append(
            {
                "Date": winner["Date"],
                "Stock Trade": symbol,
                "Trade": final_trade,
                "Why It Matters": why,
                "Region": winner["Region"],
                "Commodity": winner["Commodity"],
                "Anomaly": winner["Anomaly"],
                "Vehicle": winner["Vehicle"],
                "Commodity Trade": winner["Commodity Trade"],
                "Signal": int(round(final_trade_score)),
                "Conviction": conviction,
                "Weather Strength": round(weather_strength, 2),
                "Mapping Quality": round(mapping_quality, 2),
                "Conflict Cleanliness": round(cleanliness, 2),
                "Execution Quality": round(execution_quality, 2),
                "Long Raw": long_score,
                "Short Raw": short_score,
            }
        )

    if not final_rows:
        return pd.DataFrame()

    pulse_df = pd.DataFrame(final_rows)

    pulse_df = pulse_df.sort_values(
        by=["Signal", "Stock Trade"],
        ascending=[False, True],
    ).reset_index(drop=True)

    return pulse_df


def build_ranked_trade_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for _, row in df.iterrows():
        trade, symbols = get_stock_trade_symbols(row)
        if not symbols:
            rows.append(
                {
                    "Region": normalize_text(row.get("region")),
                    "Commodity": normalize_text(row.get("commodity")),
                    "Anomaly": normalize_text(row.get("anomaly_type")).replace("_", " ").title(),
                    "Trade": infer_trade(row),
                    "Conviction": normalize_text(row.get("signal_bucket"), score_bucket(safe_int(row.get("signal_level")))),
                    "Signal": safe_int(row.get("signal_level")),
                    "Persistence": safe_int(row.get("persistence_score")),
                    "Market": safe_int(row.get("market_score")),
                    "Severity": safe_int(row.get("severity_score")),
                    "Commodity Trade": get_commodity_trade(row),
                    "Stock Trade": "-",
                    "Vehicle": get_vehicle(row),
                    "Weather Strength": compute_weather_strength(row),
                    "Mapping Quality": 0.0,
                    "Execution Quality": 0.0,
                    "Final Trade Score": compute_weather_strength(row),
                }
            )
            continue

        best_symbol = symbols[0]
        weather_strength = compute_weather_strength(row)
        mapping_quality = compute_mapping_quality(row, best_symbol, trade)
        execution_quality = compute_execution_quality(row, best_symbol)
        final_trade_score = compute_final_trade_score(
            weather_strength=weather_strength,
            mapping_quality=mapping_quality,
            conflict_cleanliness=10.0,
            execution_quality=execution_quality,
        )

        rows.append(
            {
                "Region": normalize_text(row.get("region")),
                "Commodity": normalize_text(row.get("commodity")),
                "Anomaly": normalize_text(row.get("anomaly_type")).replace("_", " ").title(),
                "Trade": trade,
                "Conviction": score_bucket(int(round(final_trade_score))),
                "Signal": int(round(final_trade_score)),
                "Persistence": safe_int(row.get("persistence_score")),
                "Market": safe_int(row.get("market_score")),
                "Severity": safe_int(row.get("severity_score")),
                "Commodity Trade": get_commodity_trade(row),
                "Stock Trade": get_stock_trade(row),
                "Vehicle": get_vehicle(row),
                "Weather Strength": round(weather_strength, 2),
                "Mapping Quality": round(mapping_quality, 2),
                "Execution Quality": round(execution_quality, 2),
                "Final Trade Score": round(final_trade_score, 2),
            }
        )

    ranked_df = pd.DataFrame(rows)
    ranked_df = ranked_df.sort_values(
        by=["Final Trade Score", "Signal", "Region", "Commodity"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)

    return ranked_df


def show_trade_card(row, rank_number=None):
    title = build_title(row)
    prefix = f"#{rank_number} " if rank_number is not None else ""
    trade = infer_trade(row)

    trade, symbols = get_stock_trade_symbols(row)
    best_symbol = symbols[0] if symbols else ""
    weather_strength = compute_weather_strength(row)
    mapping_quality = compute_mapping_quality(row, best_symbol, trade) if best_symbol else 0.0
    execution_quality = compute_execution_quality(row, best_symbol) if best_symbol else 0.0
    final_trade_score = compute_final_trade_score(
        weather_strength=weather_strength,
        mapping_quality=mapping_quality,
        conflict_cleanliness=10.0,
        execution_quality=execution_quality,
    )
    bucket = score_bucket(int(round(final_trade_score)))

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
    st.markdown(f"**Final Trade Score:** {round(final_trade_score, 2)} / 10")
    st.markdown(f"**Weather Strength:** {round(weather_strength, 2)}")
    st.markdown(f"**Mapping Quality:** {round(mapping_quality, 2)}")
    st.markdown(f"**Execution Quality:** {round(execution_quality, 2)}")
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

        st.markdown("**Scoring breakdown**")
        st.write(f"Weather Strength: {round(weather_strength, 2)}")
        st.write(f"Mapping Quality: {round(mapping_quality, 2)}")
        st.write(f"Execution Quality: {round(execution_quality, 2)}")
        st.write(f"Final Trade Score: {round(final_trade_score, 2)}")

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

df["weather_strength"] = df.apply(compute_weather_strength, axis=1)
df["trade_display"] = df.apply(infer_trade, axis=1)

df = (
    df.sort_values(
        by=["weather_strength", "signal_level", "persistence_score", "market_score", "severity_score", "created_at"],
        ascending=[False, False, False, False, False, False],
    )
    .drop_duplicates(subset=["region", "commodity", "anomaly_type"], keep="first")
    .reset_index(drop=True)
)

st.sidebar.header("Filters")

bucket_options = ["PRIME", "ACTIONABLE", "WATCH", "EARLY"]
selected_buckets = st.sidebar.multiselect("Conviction", bucket_options, default=bucket_options)

trade_options = ["Long", "Short", "No Trade"]
selected_trades = st.sidebar.multiselect("Trade", trade_options, default=trade_options)

region_options = sorted(df["region"].dropna().astype(str).unique().tolist())
selected_regions = st.sidebar.multiselect("Region", region_options, default=region_options)

commodity_options = sorted(df["commodity"].dropna().astype(str).unique().tolist())
selected_commodities = st.sidebar.multiselect("Commodity", commodity_options, default=commodity_options)

min_signal = st.sidebar.slider("Minimum Weather Strength", min_value=1, max_value=10, value=1)
top_n = st.sidebar.slider("Top Trades to Show", min_value=3, max_value=50, value=20)

ranked_preview = build_ranked_trade_table(df)
valid_bucket_keys = set(selected_buckets)

filtered = df[
    df["trade_display"].isin(selected_trades)
    & df["region"].astype(str).isin(selected_regions)
    & df["commodity"].astype(str).isin(selected_commodities)
    & (df["weather_strength"] >= min_signal)
].copy()

filtered_ranked = build_ranked_trade_table(filtered)
filtered_ranked = filtered_ranked[filtered_ranked["Conviction"].isin(valid_bucket_keys)].copy()

last_update = filtered["created_at"].max() if "created_at" in filtered.columns and not filtered.empty else None

long_ranked = filtered_ranked[filtered_ranked["Trade"] == "Long"]
short_ranked = filtered_ranked[filtered_ranked["Trade"] == "Short"]

top_long = (
    f"{long_ranked.iloc[0]['Region']} — {long_ranked.iloc[0]['Anomaly']} — {long_ranked.iloc[0]['Commodity']}"
    if not long_ranked.empty else "-"
)
top_short = (
    f"{short_ranked.iloc[0]['Region']} — {short_ranked.iloc[0]['Anomaly']} — {short_ranked.iloc[0]['Commodity']}"
    if not short_ranked.empty else "-"
)

prime_count = int((filtered_ranked["Conviction"] == "PRIME").sum())

st.header("📡 Market Radar")
r1, r2, r3, r4 = st.columns(4)
r1.metric("Top Long", top_long)
r2.metric("Top Short", top_short)
r3.metric("Prime Trades", prime_count)
r4.metric("Last Update", format_dt(last_update))

st.header("🌍 Top Global Trades Right Now")

if filtered.empty:
    st.write("No trades match the current filters.")
else:
    top_df = filtered.copy()
    top_df["preview_score"] = top_df.apply(
        lambda row: compute_final_trade_score(
            compute_weather_strength(row),
            compute_mapping_quality(row, get_stock_trade_symbols(row)[1][0], infer_trade(row)) if get_stock_trade_symbols(row)[1] else 0.0,
            10.0,
            compute_execution_quality(row, get_stock_trade_symbols(row)[1][0]) if get_stock_trade_symbols(row)[1] else 0.0,
        ),
        axis=1,
    )
    top_df = top_df.sort_values(by=["preview_score", "weather_strength", "created_at"], ascending=[False, False, False]).head(top_n)

    for idx, (_, row) in enumerate(top_df.iterrows(), start=1):
        show_trade_card(row, rank_number=idx)

st.header("📊 Ranking Table")

ranking_table = filtered_ranked[
    [
        "Region",
        "Commodity",
        "Anomaly",
        "Trade",
        "Conviction",
        "Signal",
        "Persistence",
        "Market",
        "Severity",
        "Commodity Trade",
        "Stock Trade",
        "Vehicle",
        "Weather Strength",
        "Mapping Quality",
        "Execution Quality",
        "Final Trade Score",
    ]
].copy()

st.dataframe(ranking_table, use_container_width=True)

st.header("🌐 Global Pulse Trader")

pulse_source = filtered.copy()
pulse_table = build_global_pulse_trader_table(pulse_source)

if pulse_table.empty:
    st.write("No high-quality recommendations right now.")
else:
    pulse_table = pulse_table[pulse_table["Signal"] >= 7].copy()
    pulse_table = pulse_table[pulse_table["Trade"] != "No Trade"].copy()
    st.dataframe(pulse_table, use_container_width=True)

with st.expander("Raw filtered table"):
    st.dataframe(filtered, use_container_width=True)
