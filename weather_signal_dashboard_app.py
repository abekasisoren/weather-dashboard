import json
import os

import pandas as pd
import psycopg
import streamlit as st

from weather_market_map import get_best_trade_expressions, get_event_candidates, get_event_market_map

DATABASE_URL = os.environ.get("DATABASE_URL")

st.set_page_config(page_title="Global Weather Signal Dashboard", layout="wide")

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
        # New anomaly types
        "polar_vortex": "polar_vortex",
        "atmospheric_river": "atmospheric_river",
        "monsoon_failure": "monsoon_failure",
        "ice_storm": "ice_storm",
        "extreme_wind": "extreme_wind",
    }
    return mapping.get(raw, raw)


def commodity_context_type(row) -> str:
    commodity = normalize_text(row.get("commodity"), "")
    if commodity in {"Corn", "Soybeans", "Wheat", "Coffee", "Sugar", "Rice", "Cocoa", "Palm Oil", "Canola", "Olive Oil"}:
        return "ag"
    if commodity in {"Natural Gas", "Oil", "Coal", "LNG"}:
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

    if anomaly in {"cold_wave", "frost", "polar_vortex", "ice_storm"} and commodity in {
        "Natural Gas", "Power Utilities", "Wheat", "LNG", "Coal"
    }:
        return "Long"

    if anomaly in {"heavy_rain", "flood_risk", "flood", "atmospheric_river"} and ctype == "ag":
        return "Short"

    if anomaly in {"heavy_rain", "flood_risk", "flood", "atmospheric_river"} and ctype in {"energy", "utilities"}:
        return "Long"

    if anomaly in {"storm_wind", "hurricane_risk", "hurricane", "wildfire_risk", "wildfire", "extreme_wind"} and ctype in {"energy", "utilities"}:
        return "Long"

    if anomaly in {"monsoon_failure"} and ctype == "ag":
        return "Long"

    return "No Trade"


# ─── Badge helpers ────────────────────────────────────────────────────────────

def conviction_badge(bucket: str) -> str:
    bucket = normalize_text(bucket, "EARLY").upper()
    styles = {
        "PRIME":      ("background:#4c1d95", "color:#e9d5ff"),
        "ACTIONABLE": ("background:#14532d", "color:#bbf7d0"),
        "WATCH":      ("background:#78350f", "color:#fde68a"),
        "MIXED":      ("background:#312e81", "color:#c7d2fe"),
        "EARLY":      ("background:#334155", "color:#cbd5e1"),
    }
    bg, fg = styles.get(bucket, styles["EARLY"])
    label = bucket
    return f"<span style='{bg};{fg};padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>{label}</span>"


def trade_badge(trade: str) -> str:
    if trade == "Long":
        return "<span style='background:#14532d;color:#bbf7d0;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>LONG</span>"
    if trade == "Short":
        return "<span style='background:#991b1b;color:#fecaca;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>SHORT</span>"
    return "<span style='background:#374151;color:#e5e7eb;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.85rem;'>NO TRADE</span>"


def trend_badge(trend: str) -> str:
    t = normalize_text(trend, "new").lower()
    if t == "worsening":
        return "<span style='background:#7f1d1d;color:#fca5a5;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.82rem;'>↑ WORSENING</span>"
    if t == "new":
        return "<span style='background:#1e3a5f;color:#93c5fd;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.82rem;'>★ NEW</span>"
    if t == "recovering":
        return "<span style='background:#14532d;color:#86efac;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.82rem;'>↓ RECOVERING</span>"
    # stable or unknown
    return "<span style='background:#334155;color:#94a3b8;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.82rem;'>→ STABLE</span>"


def media_badge() -> str:
    return "<span style='background:#1e3a5f;color:#93c5fd;padding:4px 10px;border-radius:999px;font-weight:700;font-size:0.82rem;'>📰 CONFIRMED</span>"


def progress_bar_html(label: str, value: float, max_val: float = 10.0) -> str:
    pct = int(min(100, max(0, (value / max_val) * 100)))
    color = "#4ade80" if pct >= 70 else "#facc15" if pct >= 40 else "#f87171"
    return (
        f"<div style='margin:3px 0'>"
        f"<span style='font-size:0.78rem;color:#94a3b8;width:160px;display:inline-block;'>{label}</span>"
        f"<div style='display:inline-block;width:120px;background:#334155;border-radius:4px;height:8px;vertical-align:middle;margin:0 6px'>"
        f"<div style='width:{pct}%;background:{color};height:8px;border-radius:4px'></div>"
        f"</div>"
        f"<span style='font-size:0.78rem;color:#e2e8f0;'>{value:.1f}</span>"
        f"</div>"
    )


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
        "Cocoa": "NIB",
        "Palm Oil": "DBA",
        "Canola": "MOO",
        "LNG": "UNG",
        "Olive Oil": "DBA",
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

    if anomaly in {"drought", "monsoon_failure"}:
        if precip is not None:
            evidence.append(f"Rainfall: {precip:.1f} mm")
        if temp_mean is not None:
            evidence.append(f"Mean temp: {temp_mean:.1f}°C")

    if anomaly in {"frost", "cold_wave", "polar_vortex", "ice_storm"}:
        if temp_min is not None:
            evidence.append(f"Min temp: {temp_min:.1f}°C")
        if temp_mean is not None:
            evidence.append(f"Mean temp: {temp_mean:.1f}°C")

    if anomaly in {"heavy_rain", "flood_risk", "flood", "atmospheric_river"}:
        if precip is not None:
            evidence.append(f"Rainfall: {precip:.1f} mm")

    if anomaly in {"storm_wind", "hurricane_risk", "hurricane", "wildfire_risk", "wildfire", "extreme_wind"}:
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


# ─── Seasonality baseline (month → expected? → score) ───────────────────────
# Returns 0-10: high = anomalous for season, low = expected for season
SEASONAL_BASELINE = {
    "US Midwest": {
        "heatwave":     {"peak": [6, 7, 8], "fringe": [5, 9]},
        "extreme_heat": {"peak": [7, 8], "fringe": [6, 9]},
        "frost":        {"peak": [11, 12, 1, 2], "fringe": [3, 10]},
        "cold_wave":    {"peak": [12, 1, 2], "fringe": [11, 3]},
        "drought":      {"peak": [7, 8], "fringe": [6, 9]},
    },
    "US Southern Plains": {
        "heatwave":  {"peak": [6, 7, 8, 9], "fringe": [5, 10]},
        "drought":   {"peak": [6, 7, 8, 9], "fringe": [5, 10]},
        "cold_wave": {"peak": [12, 1, 2], "fringe": [11, 3]},
    },
    "Brazil": {
        "drought":    {"peak": [7, 8, 9], "fringe": [6, 10]},
        "heavy_rain": {"peak": [11, 12, 1, 2], "fringe": [3, 10]},
        "flood_risk": {"peak": [12, 1, 2], "fringe": [11, 3]},
    },
    "Mato Grosso": {
        "drought":    {"peak": [7, 8, 9], "fringe": [6, 10]},
        "heatwave":   {"peak": [9, 10, 11], "fringe": [8, 12]},
    },
    "Europe Gas Belt": {
        "cold_wave":  {"peak": [12, 1, 2], "fringe": [11, 3]},
        "polar_vortex": {"peak": [1, 2], "fringe": [12, 3]},
        "heatwave":   {"peak": [7, 8], "fringe": [6, 9]},
    },
    "India": {
        "monsoon_failure": {"peak": [6, 7, 8, 9], "fringe": [5, 10]},
        "heatwave":        {"peak": [4, 5, 6], "fringe": [3, 7]},
        "drought":         {"peak": [3, 4, 5], "fringe": [2, 6]},
    },
    "California": {
        "wildfire_risk": {"peak": [7, 8, 9, 10], "fringe": [6, 11]},
        "wildfire":      {"peak": [7, 8, 9, 10], "fringe": [6, 11]},
        "drought":       {"peak": [6, 7, 8, 9], "fringe": [5, 10]},
        "atmospheric_river": {"peak": [11, 12, 1, 2], "fringe": [3, 10]},
    },
    "US Gulf": {
        "hurricane_risk": {"peak": [8, 9, 10], "fringe": [6, 7, 11]},
        "hurricane":      {"peak": [8, 9, 10], "fringe": [6, 7, 11]},
    },
    "Southeast US": {
        "hurricane_risk": {"peak": [8, 9, 10], "fringe": [6, 7, 11]},
        "heatwave":       {"peak": [6, 7, 8], "fringe": [5, 9]},
    },
    "West Africa Cocoa Belt": {
        "drought":    {"peak": [12, 1, 2], "fringe": [11, 3]},
        "heavy_rain": {"peak": [5, 6, 7], "fringe": [4, 8]},
    },
    "Southeast Asia": {
        "monsoon_failure": {"peak": [6, 7, 8, 9], "fringe": [5, 10]},
        "heavy_rain":      {"peak": [6, 7, 8, 9], "fringe": [5, 10]},
    },
    "North Sea": {
        "storm_wind":   {"peak": [11, 12, 1, 2], "fringe": [3, 10]},
        "extreme_wind": {"peak": [11, 12, 1, 2], "fringe": [3, 10]},
        "cold_wave":    {"peak": [12, 1, 2], "fringe": [11, 3]},
    },
    "Australian East": {
        "heatwave":  {"peak": [12, 1, 2], "fringe": [11, 3]},
        "drought":   {"peak": [1, 2, 3], "fringe": [12, 4]},
    },
}


def compute_seasonality_score(row) -> float:
    """Score 0-10: how anomalous is this weather event for the current month?
    High score = off-season surprise (strong signal); low = expected season (priced in)."""
    import datetime
    region = normalize_text(row.get("region"), "")
    anomaly = normalize_anomaly_key(row.get("anomaly_type", ""))
    current_month = datetime.datetime.now().month

    region_data = SEASONAL_BASELINE.get(region, {})
    anomaly_data = region_data.get(anomaly, None)

    if anomaly_data is None:
        return 5.0  # neutral — no baseline available

    peak = anomaly_data.get("peak", [])
    fringe = anomaly_data.get("fringe", [])

    if current_month in peak:
        return 3.0   # Expected — market already pricing season
    if current_month in fringe:
        return 6.0   # Fringe — somewhat unusual, moderate signal boost
    return 9.0       # Off-season — high anomaly surprise value


def compute_trend_factor(row) -> float:
    """Score 0-10 based on trend_direction column."""
    trend = normalize_text(row.get("trend_direction", ""), "new").lower()
    mapping = {
        "worsening":  9.0,
        "new":        7.0,
        "stable":     5.0,
        "recovering": 2.0,
    }
    return mapping.get(trend, 5.0)


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

    tier_score_map = {1: 9.5, 2: 7.0, 3: 4.0}
    tier_score = tier_score_map.get(tier, 4.0)
    directness_score = clamp(directness * 10.0)

    score = 0.60 * tier_score + 0.40 * directness_score
    return round(clamp(score), 2)


def compute_execution_quality(row, symbol: str) -> float:
    vehicle = get_vehicle(row)
    candidate = get_symbol_candidate(row, symbol, "long") or get_symbol_candidate(row, symbol, "short")

    preferred_vehicles = {
        "CORN", "SOYB", "WEAT", "JO", "CANE", "UNG", "USO", "XLU", "KOL",
        "DBA", "NIB", "MOO", "XHB", "ITB", "XLE", "XLRE",
    }
    if vehicle in preferred_vehicles:
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
    return round(clamp(dominance * 10.0), 2)


def compute_confluence_bonus(df: pd.DataFrame, anomaly_type: str) -> float:
    """Bonus for same anomaly type appearing in multiple regions simultaneously."""
    count = int((df["anomaly_type"] == anomaly_type).sum())
    if count >= 3:
        return 1.0
    if count == 2:
        return 0.5
    return 0.0


def compute_final_trade_score(
    weather_strength: float,
    mapping_quality: float,
    conflict_cleanliness: float,
    execution_quality: float,
    seasonality_score: float = 5.0,
    trend_factor: float = 5.0,
    confluence_bonus: float = 0.0,
) -> float:
    score = (
        0.35 * weather_strength +
        0.20 * mapping_quality +
        0.15 * conflict_cleanliness +
        0.10 * execution_quality +
        0.10 * seasonality_score +
        0.10 * trend_factor
    ) + confluence_bonus
    return round(clamp(score), 2)


def build_global_pulse_trader_table(df: pd.DataFrame) -> pd.DataFrame:
    raw_rows = []

    for _, row in df.iterrows():
        trade, symbols = get_stock_trade_symbols(row)
        if not symbols:
            continue

        signal = safe_int(row.get("signal_level"))
        weather_strength = compute_weather_strength(row)
        seasonality_score = compute_seasonality_score(row)
        trend_factor = compute_trend_factor(row)

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
                    "Seasonality": seasonality_score,
                    "Trend Factor": trend_factor,
                    "Trend Direction": normalize_text(row.get("trend_direction", ""), "new"),
                    "Anomaly Type": normalize_anomaly_key(row.get("anomaly_type", "")),
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

        sort_cols = ["Weather Strength", "Mapping Quality", "Execution Quality", "Raw Signal"]
        sort_asc = [False, False, False, False]

        if net_score >= 3 and not long_group.empty:
            final_trade = "Long"
            winner = long_group.sort_values(by=sort_cols, ascending=sort_asc).iloc[0]
        elif net_score <= -3 and not short_group.empty:
            final_trade = "Short"
            winner = short_group.sort_values(by=sort_cols, ascending=sort_asc).iloc[0]
        else:
            winner = group.sort_values(by=sort_cols, ascending=sort_asc).iloc[0]
            final_trade = "No Trade"

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

            # Patch why on winner
            winner = winner.copy()
            winner["Why"] = why

        mapping_quality = float(winner["Mapping Quality"])
        execution_quality = float(winner["Execution Quality"])
        weather_strength = float(winner["Weather Strength"])
        seasonality_score = float(winner["Seasonality"])
        trend_factor = float(winner["Trend Factor"])
        why = winner["Why"]

        # Confluence bonus: check how many regions have the same anomaly
        anomaly_type = winner["Anomaly Type"]
        same_anomaly_count = int((raw_df["Anomaly Type"] == anomaly_type).sum())
        conf_bonus = 1.0 if same_anomaly_count >= 3 else 0.5 if same_anomaly_count == 2 else 0.0

        final_trade_score = compute_final_trade_score(
            weather_strength=weather_strength,
            mapping_quality=mapping_quality,
            conflict_cleanliness=cleanliness,
            execution_quality=execution_quality,
            seasonality_score=seasonality_score,
            trend_factor=trend_factor,
            confluence_bonus=conf_bonus,
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
                "Trend": winner.get("Trend Direction", "new"),
                "Vehicle": winner["Vehicle"],
                "Commodity Trade": winner["Commodity Trade"],
                "Signal": int(round(final_trade_score)),
                "Conviction": conviction,
                "Weather Strength": round(weather_strength, 2),
                "Mapping Quality": round(mapping_quality, 2),
                "Conflict Cleanliness": round(cleanliness, 2),
                "Execution Quality": round(execution_quality, 2),
                "Seasonality": round(seasonality_score, 2),
                "Trend Factor": round(trend_factor, 2),
                "Confluence Bonus": round(conf_bonus, 2),
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
        weather_strength = compute_weather_strength(row)
        seasonality_score = compute_seasonality_score(row)
        trend_factor = compute_trend_factor(row)
        anomaly_type = normalize_anomaly_key(row.get("anomaly_type", ""))
        conf_bonus = compute_confluence_bonus(df, anomaly_type)
        trend_dir = normalize_text(row.get("trend_direction", ""), "new")

        if not symbols:
            rows.append(
                {
                    "Region": normalize_text(row.get("region")),
                    "Commodity": normalize_text(row.get("commodity")),
                    "Anomaly": normalize_text(row.get("anomaly_type")).replace("_", " ").title(),
                    "Trade": infer_trade(row),
                    "Trend": trend_dir,
                    "Conviction": normalize_text(row.get("signal_bucket"), score_bucket(safe_int(row.get("signal_level")))),
                    "Signal": safe_int(row.get("signal_level")),
                    "Persistence": safe_int(row.get("persistence_score")),
                    "Market": safe_int(row.get("market_score")),
                    "Severity": safe_int(row.get("severity_score")),
                    "Commodity Trade": get_commodity_trade(row),
                    "Stock Trade": "-",
                    "Vehicle": get_vehicle(row),
                    "Weather Strength": weather_strength,
                    "Mapping Quality": 0.0,
                    "Execution Quality": 0.0,
                    "Seasonality": round(seasonality_score, 2),
                    "Trend Factor": round(trend_factor, 2),
                    "Final Trade Score": weather_strength,
                }
            )
            continue

        best_symbol = symbols[0]
        mapping_quality = compute_mapping_quality(row, best_symbol, trade)
        execution_quality = compute_execution_quality(row, best_symbol)
        final_trade_score = compute_final_trade_score(
            weather_strength=weather_strength,
            mapping_quality=mapping_quality,
            conflict_cleanliness=10.0,
            execution_quality=execution_quality,
            seasonality_score=seasonality_score,
            trend_factor=trend_factor,
            confluence_bonus=conf_bonus,
        )

        rows.append(
            {
                "Region": normalize_text(row.get("region")),
                "Commodity": normalize_text(row.get("commodity")),
                "Anomaly": normalize_text(row.get("anomaly_type")).replace("_", " ").title(),
                "Trade": trade,
                "Trend": trend_dir,
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
                "Seasonality": round(seasonality_score, 2),
                "Trend Factor": round(trend_factor, 2),
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

    trade, symbols = get_stock_trade_symbols(row)
    best_symbol = symbols[0] if symbols else ""
    weather_strength = compute_weather_strength(row)
    mapping_quality = compute_mapping_quality(row, best_symbol, trade) if best_symbol else 0.0
    execution_quality = compute_execution_quality(row, best_symbol) if best_symbol else 0.0
    seasonality_score = compute_seasonality_score(row)
    trend_factor = compute_trend_factor(row)
    trend_dir = normalize_text(row.get("trend_direction", ""), "new")
    media_val = row.get("media_validated")
    is_new_signal = False
    try:
        created = pd.to_datetime(row.get("created_at"), errors="coerce")
        if not pd.isna(created):
            import datetime
            now = pd.Timestamp.now(tz="UTC")
            if created.tzinfo is None:
                created = created.tz_localize("UTC")
            is_new_signal = (now - created).total_seconds() < 86400
    except Exception:
        pass

    final_trade_score = compute_final_trade_score(
        weather_strength=weather_strength,
        mapping_quality=mapping_quality,
        conflict_cleanliness=10.0,
        execution_quality=execution_quality,
        seasonality_score=seasonality_score,
        trend_factor=trend_factor,
    )
    bucket = score_bucket(int(round(final_trade_score)))

    new_tag = " ⚡" if is_new_signal else ""
    st.markdown(f"### {prefix}{title}{new_tag}")

    badge_parts = [trade_badge(trade), conviction_badge(bucket), trend_badge(trend_dir)]
    if media_val is True:
        badge_parts.append(media_badge())
    st.markdown(" &nbsp; ".join(badge_parts), unsafe_allow_html=True)

    st.markdown(f"**Commodity Trade:** {get_commodity_trade(row)}")
    st.markdown(f"**Stock Trade:** {get_stock_trade(row)}")
    st.markdown(f"**Vehicle:** {get_vehicle(row)}")
    st.markdown(f"**Why this matters:** {get_why_it_matters(row)}")
    st.markdown(f"**Final Trade Score:** {round(final_trade_score, 2)} / 10")
    st.markdown(f"**Forecast Window:** {format_dt(row.get('forecast_start'))} → {format_dt(row.get('forecast_end'))}")

    with st.expander("Score breakdown & details"):
        score_html = (
            progress_bar_html("Weather Strength", weather_strength) +
            progress_bar_html("Mapping Quality", mapping_quality) +
            progress_bar_html("Execution Quality", execution_quality) +
            progress_bar_html("Seasonality", seasonality_score) +
            progress_bar_html("Trend Factor", trend_factor) +
            progress_bar_html("Final Score", final_trade_score)
        )
        st.markdown(score_html, unsafe_allow_html=True)

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


# ─── Data load ────────────────────────────────────────────────────────────────

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
        created_at,
        COALESCE(trend_direction, 'new') AS trend_direction,
        media_validated,
        media_source,
        media_headline
    FROM weather_global_shocks
    ORDER BY created_at DESC, signal_level DESC, region ASC, commodity ASC
    """
)

if df.empty:
    st.title("Global Weather Signal Dashboard")
    st.caption("Early weather intelligence for markets ranked like a trading radar.")
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

# ─── Sidebar filters ─────────────────────────────────────────────────────────

st.sidebar.header("Filters")

bucket_options = ["PRIME", "ACTIONABLE", "WATCH", "EARLY"]
selected_buckets = st.sidebar.multiselect("Conviction", bucket_options, default=bucket_options)

trade_options = ["Long", "Short", "No Trade"]
selected_trades = st.sidebar.multiselect("Trade", trade_options, default=trade_options)

trend_options = ["worsening", "new", "stable", "recovering"]
selected_trends = st.sidebar.multiselect("Trend Direction", trend_options, default=["worsening", "new", "stable"])

region_options = sorted(df["region"].dropna().astype(str).unique().tolist())
selected_regions = st.sidebar.multiselect("Region", region_options, default=region_options)

commodity_options = sorted(df["commodity"].dropna().astype(str).unique().tolist())
selected_commodities = st.sidebar.multiselect("Commodity", commodity_options, default=commodity_options)

anomaly_options = sorted(df["anomaly_type"].dropna().astype(str).unique().tolist())
selected_anomalies = st.sidebar.multiselect("Anomaly Type", anomaly_options, default=anomaly_options)

min_signal = st.sidebar.slider("Minimum Weather Strength", min_value=1, max_value=10, value=1)
top_n = st.sidebar.slider("Top Trades to Show", min_value=3, max_value=50, value=20)

# ─── Filter application ───────────────────────────────────────────────────────

filtered = df[
    df["trade_display"].isin(selected_trades)
    & df["region"].astype(str).isin(selected_regions)
    & df["commodity"].astype(str).isin(selected_commodities)
    & df["anomaly_type"].astype(str).isin(selected_anomalies)
    & df["trend_direction"].astype(str).isin(selected_trends)
    & (df["weather_strength"] >= min_signal)
].copy()

filtered_ranked = build_ranked_trade_table(filtered)
valid_bucket_keys = set(selected_buckets)
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
worsening_count = int((df["trend_direction"] == "worsening").sum())
new_count = int((df["trend_direction"] == "new").sum())
anomaly_coverage = df["anomaly_type"].nunique()

# ─── Tab layout ───────────────────────────────────────────────────────────────

st.title("Global Weather Signal Dashboard")
st.caption("Early weather intelligence for markets ranked like a trading radar.")

tab_radar, tab_pulse, tab_media = st.tabs(["🌍 Radar", "📊 Pulse Trader", "📡 Media Signals"])

with tab_radar:
    st.header("📡 Market Radar")
    r1, r2, r3, r4, r5, r6 = st.columns(6)
    r1.metric("Top Long", top_long[:30] + "…" if len(top_long) > 30 else top_long)
    r2.metric("Top Short", top_short[:30] + "…" if len(top_short) > 30 else top_short)
    r3.metric("Prime Trades", prime_count)
    r4.metric("⚡ New Signals", new_count)
    r5.metric("↑ Worsening", worsening_count)
    r6.metric("Last Update", format_dt(last_update)[:16] if last_update else "-")

    st.header("🌍 Top Global Trades Right Now")

    if filtered.empty:
        st.write("No trades match the current filters.")
    else:
        top_df = filtered.copy()

        def _preview_score(row):
            syms = get_stock_trade_symbols(row)
            sym0 = syms[1][0] if syms[1] else ""
            ws = compute_weather_strength(row)
            mq = compute_mapping_quality(row, sym0, syms[0]) if sym0 else 0.0
            eq = compute_execution_quality(row, sym0) if sym0 else 0.0
            ss = compute_seasonality_score(row)
            tf = compute_trend_factor(row)
            return compute_final_trade_score(ws, mq, 10.0, eq, ss, tf)

        top_df["preview_score"] = top_df.apply(_preview_score, axis=1)
        top_df = top_df.sort_values(
            by=["preview_score", "weather_strength", "created_at"],
            ascending=[False, False, False],
        ).head(top_n)

        for idx, (_, row) in enumerate(top_df.iterrows(), start=1):
            show_trade_card(row, rank_number=idx)

    st.header("📊 Ranking Table")

    if filtered_ranked.empty:
        st.write("No signals match the current filters.")
    else:
        display_cols = [
            "Region", "Commodity", "Anomaly", "Trade", "Conviction", "Trend",
            "Vehicle", "Stock Trade", "Final Trade Score",
        ]
        available_cols = [c for c in display_cols if c in filtered_ranked.columns]
        st.dataframe(filtered_ranked[available_cols], use_container_width=True, height=400)

    with st.expander("Raw filtered data"):
        st.dataframe(filtered, use_container_width=True)

with tab_pulse:
    st.header("🌐 Global Pulse Trader")
    st.caption("Symbol-centric view — aggregates all weather signals per stock.")

    pulse_source = filtered.copy()
    pulse_table = build_global_pulse_trader_table(pulse_source)

    if pulse_table.empty:
        st.write("No high-quality recommendations right now.")
    else:
        high_conviction = pulse_table[pulse_table["Signal"] >= 7].copy()
        high_conviction = high_conviction[high_conviction["Trade"] != "No Trade"].copy()

        if high_conviction.empty:
            st.info("No signals above threshold. Showing all signals.")
            st.dataframe(pulse_table, use_container_width=True)
        else:
            st.dataframe(high_conviction, use_container_width=True)

        if not pulse_table.empty and len(pulse_table) > len(high_conviction if not high_conviction.empty else pulse_table):
            with st.expander(f"All signals including lower conviction ({len(pulse_table)} total)"):
                st.dataframe(pulse_table, use_container_width=True)

with tab_media:
    st.header("📡 Media Signal Validation")
    st.caption("Cross-reference weather signals with news and official alerts.")

    # Check if any signals have been media-validated
    has_media_validated = (
        "media_validated" in df.columns
        and df["media_validated"].notna().any()
        and df["media_validated"].eq(True).any()
    )

    newsapi_configured = bool(os.environ.get("NEWSAPI_KEY"))
    noaa_configured = bool(os.environ.get("NOAA_API_KEY"))

    if not newsapi_configured and not noaa_configured:
        st.info(
            "**Media validation not yet configured.**\n\n"
            "To enable real-time news confirmation of weather signals, set the following environment variables:\n"
            "- `NEWSAPI_KEY` — [newsapi.org](https://newsapi.org) API key for news headlines\n"
            "- `NOAA_API_KEY` — NOAA/NWS official weather alert API key (free)\n\n"
            "Once configured, this tab will show which signals have been confirmed by published news or official alerts."
        )
    else:
        connected = []
        if newsapi_configured:
            connected.append("NewsAPI")
        if noaa_configured:
            connected.append("NOAA/NWS")
        st.success(f"Connected: {', '.join(connected)}")

    if has_media_validated:
        st.subheader("Confirmed Signals")
        media_df = df[df["media_validated"] == True].copy()
        cols = ["region", "commodity", "anomaly_type", "signal_level", "trade_bias", "media_headline", "media_source"]
        available_media_cols = [c for c in cols if c in media_df.columns]
        st.dataframe(media_df[available_media_cols], use_container_width=True)
    else:
        st.write("No media-confirmed signals yet. When media validation runs, confirmed signals will appear here.")

    st.subheader("Anomaly Coverage")
    coverage_counts = (
        df.groupby("anomaly_type")
        .size()
        .reset_index(name="Active Signals")
        .sort_values("Active Signals", ascending=False)
    )
    st.dataframe(coverage_counts, use_container_width=True)
