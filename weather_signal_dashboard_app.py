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
    if commodity in {"Corn", "Soybeans", "Wheat", "Coffee", "Sugar", "Rice", "Cocoa", "Palm Oil",
                     "Canola", "Olive Oil", "Sunflower Oil", "Dairy", "Cattle"}:
        return "ag"
    if commodity in {"Natural Gas", "Oil", "Coal", "LNG"}:
        return "energy"
    if commodity in {"Power Utilities", "Hydropower"}:
        return "utilities"
    if commodity in {"Copper", "Lithium"}:
        return "metals"
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

    if anomaly in {"heatwave", "extreme_heat", "drought"} and ctype in {"ag", "energy", "utilities", "metals"}:
        return "Long"

    if anomaly in {"drought"} and commodity == "Hydropower":
        return "Short"  # Drought = less water = less hydro generation

    if anomaly in {"cold_wave", "frost", "polar_vortex", "ice_storm"} and commodity in {
        "Natural Gas", "Power Utilities", "Wheat", "LNG", "Coal", "Dairy"
    }:
        return "Long"

    if anomaly in {"heavy_rain", "flood_risk", "flood", "atmospheric_river"} and ctype == "ag":
        return "Short"

    if anomaly in {"heavy_rain", "flood_risk", "flood", "atmospheric_river"} and ctype in {"energy", "utilities"}:
        return "Long"

    if anomaly in {"heavy_rain", "atmospheric_river"} and commodity == "Hydropower":
        return "Long"  # More rainfall = more hydro generation

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
        # New commodities
        "Sunflower Oil": "DBA",
        "Hydropower": "XLU",
        "Copper": "COPX",
        "Lithium": "LIT",
        "Dairy": "DBA",
        "Cattle": "COW",
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
    "Australia East": {
        "heatwave":  {"peak": [12, 1, 2], "fringe": [11, 3]},
        "drought":   {"peak": [1, 2, 3], "fringe": [12, 4]},
    },
    # New regions
    "Ukraine Eastern Europe": {
        "drought":    {"peak": [6, 7, 8], "fringe": [5, 9]},
        "heatwave":   {"peak": [6, 7, 8], "fringe": [5, 9]},
        "frost":      {"peak": [11, 12, 1, 2], "fringe": [3, 10]},
        "cold_wave":  {"peak": [12, 1, 2], "fringe": [11, 3]},
    },
    "Nordic Scandinavia": {
        "cold_wave":    {"peak": [12, 1, 2], "fringe": [11, 3]},
        "polar_vortex": {"peak": [1, 2], "fringe": [12, 3]},
        "storm_wind":   {"peak": [11, 12, 1, 2], "fringe": [3, 10]},
        "extreme_wind": {"peak": [11, 12, 1, 2], "fringe": [3, 10]},
    },
    "Andes South America": {
        "drought": {"peak": [12, 1, 2, 3], "fringe": [11, 4]},
        "frost":   {"peak": [6, 7, 8], "fringe": [5, 9]},
    },
    "New Zealand": {
        "drought":    {"peak": [1, 2, 3], "fringe": [12, 4]},
        "heatwave":   {"peak": [12, 1, 2], "fringe": [11, 3]},
        "storm_wind": {"peak": [6, 7, 8], "fringe": [5, 9]},
    },
    "US Great Plains": {
        "heatwave":  {"peak": [6, 7, 8], "fringe": [5, 9]},
        "drought":   {"peak": [6, 7, 8, 9], "fringe": [5, 10]},
        "cold_wave": {"peak": [12, 1, 2], "fringe": [11, 3]},
        "frost":     {"peak": [10, 11, 3, 4], "fringe": [9, 5]},
    },
    "Central America": {
        "drought":         {"peak": [2, 3, 4], "fringe": [1, 5]},
        "heavy_rain":      {"peak": [5, 6, 7, 8, 9, 10], "fringe": [4, 11]},
        "monsoon_failure": {"peak": [5, 6, 7, 8, 9], "fringe": [4, 10]},
    },
    "East Africa": {
        "drought": {"peak": [1, 2, 3], "fringe": [12, 4]},
        "heavy_rain": {"peak": [3, 4, 5, 10, 11], "fringe": [2, 6, 9, 12]},
    },
    "China Yangtze Basin": {
        "drought":    {"peak": [6, 7, 8], "fringe": [5, 9]},
        "heavy_rain": {"peak": [6, 7, 8], "fringe": [5, 9]},
        "flood_risk": {"peak": [6, 7, 8], "fringe": [5, 9]},
        "cold_wave":  {"peak": [12, 1, 2], "fringe": [11, 3]},
    },
    "Middle East Gulf": {
        "heatwave":      {"peak": [6, 7, 8], "fringe": [5, 9]},
        "extreme_heat":  {"peak": [7, 8], "fringe": [6, 9]},
        "drought":       {"peak": [5, 6, 7, 8, 9], "fringe": [4, 10]},
        "storm_wind":    {"peak": [5, 6], "fringe": [4, 7]},
    },
    "Southern Europe": {
        "drought":   {"peak": [6, 7, 8], "fringe": [5, 9]},
        "heatwave":  {"peak": [7, 8], "fringe": [6, 9]},
        "wildfire_risk": {"peak": [7, 8, 9], "fringe": [6, 10]},
    },
    "Mato Grosso": {
        "drought":    {"peak": [7, 8, 9], "fringe": [6, 10]},
        "heatwave":   {"peak": [9, 10, 11], "fringe": [8, 12]},
        "heavy_rain": {"peak": [11, 12, 1, 2, 3], "fringe": [4, 10]},
    },
    "Canadian Prairies": {
        "drought":   {"peak": [6, 7, 8], "fringe": [5, 9]},
        "heatwave":  {"peak": [7, 8], "fringe": [6, 9]},
        "frost":     {"peak": [9, 10, 11], "fringe": [8, 12]},
        "cold_wave": {"peak": [12, 1, 2], "fringe": [11, 3]},
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


def compute_edge_score(row) -> float:
    """Score 0-10: inverse of media validation.
    Unvalidated signal = maximum alpha (market hasn't priced it yet).
    Validated = media has picked it up = alpha window closing.
    This is the core 'weather edge' thesis: ECMWF sees it 7-10 days before media."""
    media_val = row.get("media_validated")
    if media_val is True:
        return 3.0    # Media has already picked it up — reduced alpha window
    if media_val is False:
        return 9.5    # Explicitly NOT in media yet — maximum alpha window
    return 7.0        # Unknown / not yet checked — assume moderate edge


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
    """
    Bonus for same anomaly type appearing in multiple DISTINCT regions simultaneously.
    Counts unique regions, not rows — prevents commodity-row inflation from
    inflating the bonus when the same region just has multiple commodities.
    """
    region_count = int(
        df[df["anomaly_type"] == anomaly_type]["region"].nunique()
    )
    if region_count >= 3:
        return 1.0
    if region_count == 2:
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
    edge_score: float = 7.0,
) -> float:
    # Weights: weather_strength 0.30 (was 0.35), conflict_cleanliness 0.10 (was 0.15),
    # edge_score 0.10 (new) — total still 1.00 before confluence bonus
    score = (
        0.30 * weather_strength +
        0.20 * mapping_quality +
        0.10 * conflict_cleanliness +
        0.10 * execution_quality +
        0.10 * seasonality_score +
        0.10 * trend_factor +
        0.10 * edge_score
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
        edge_score = compute_edge_score(row)

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
                    "Edge Score": edge_score,
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
        edge_score = float(winner.get("Edge Score", 7.0))
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
            edge_score=edge_score,
        )

        conviction = "MIXED" if final_trade == "No Trade" else score_bucket(int(round(final_trade_score)))

        final_rows.append(
            {
                "Date": winner["Date"],
                "Stock Trade": symbol,
                "Trade": final_trade,
                "Why It Matters": why,
                "Final Trade Score": round(final_trade_score, 2),
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
                "Edge Score": round(edge_score, 2),
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
        edge_score = compute_edge_score(row)
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
                    "Edge Score": round(edge_score, 2),
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
            edge_score=edge_score,
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
                "Edge Score": round(edge_score, 2),
                "Final Trade Score": round(final_trade_score, 2),
            }
        )

    ranked_df = pd.DataFrame(rows)
    ranked_df = ranked_df.sort_values(
        by=["Final Trade Score", "Signal", "Region", "Commodity"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)

    return ranked_df


def show_weather_event_card(
    event_rows: "pd.DataFrame",
    rank_number: int,
    all_df: "pd.DataFrame",
    owned_symbols: "set | None" = None,
    cooldown_combos: "set | None" = None,
):
    """
    Radar card focused on the weather EVENT (region + anomaly type).
    Shows all affected equities across every commodity in the event group,
    ranked by their individual trading scores.

    owned_symbols  — if provided, only stocks in this set are shown (cross-event
                     ownership: each stock appears in only its highest-scoring event).
    cooldown_combos — set of (symbol, region, anomaly) tuples logged in last 24h;
                      matching stocks get a 🔁 COOLDOWN badge.
    """
    best_row = event_rows.sort_values("signal_level", ascending=False).iloc[0]

    region    = normalize_text(best_row.get("region", ""), "—").replace("_", " ").title()
    anomaly_raw = normalize_text(best_row.get("anomaly_type", ""), "—")
    anomaly   = anomaly_raw.replace("_", " ").title()
    trend_dir = normalize_text(best_row.get("trend_direction", ""), "new")
    media_val = best_row.get("media_validated")
    region_raw  = normalize_text(best_row.get("region", ""), "").strip()
    anomaly_key_raw = anomaly_raw.strip()

    # Weather-level scoring (shared by all stocks in this event)
    weather_strength  = compute_weather_strength(best_row)
    seasonality_score = compute_seasonality_score(best_row)
    trend_factor      = compute_trend_factor(best_row)
    edge_score        = compute_edge_score(best_row)
    anomaly_key       = normalize_anomaly_key(anomaly_raw)
    conf_bonus        = compute_confluence_bonus(all_df, anomaly_key)

    # Collect affected stocks — respecting cross-event ownership filter
    stock_list = []
    seen_symbols: set = set()
    for _, row in event_rows.iterrows():
        trade, symbols = get_stock_trade_symbols(row)
        if not symbols or trade == "No Trade":
            continue
        commodity_label = normalize_text(row.get("commodity", ""), "")
        for sym in symbols:
            if sym in seen_symbols:
                continue
            # Cross-event ownership: skip if this symbol belongs to a higher-scoring event
            if owned_symbols is not None and sym not in owned_symbols:
                continue
            seen_symbols.add(sym)
            mq = compute_mapping_quality(row, sym, trade)
            eq = compute_execution_quality(row, sym)
            score = compute_final_trade_score(
                weather_strength=weather_strength,
                mapping_quality=mq,
                conflict_cleanliness=10.0,
                execution_quality=eq,
                seasonality_score=seasonality_score,
                trend_factor=trend_factor,
                edge_score=edge_score,
                confluence_bonus=conf_bonus,
            )
            candidate = get_symbol_candidate(row, sym, trade.lower()) or {}
            role = candidate.get("role", commodity_label).replace("_", " ")
            # Cooldown check: was this (symbol, region, anomaly) logged in last 24h?
            on_cooldown = (
                cooldown_combos is not None
                and (sym, region_raw, anomaly_key_raw) in cooldown_combos
            )
            stock_list.append({
                "symbol": sym,
                "direction": trade,
                "score": round(score, 1),
                "role": role,
                "commodity": commodity_label,
                "cooldown": on_cooldown,
            })

    # Sort stocks by score descending
    stock_list.sort(key=lambda x: x["score"], reverse=True)

    # Event-level score = top stock score (or weather strength if no stocks)
    event_score = stock_list[0]["score"] if stock_list else round(weather_strength, 1)
    bucket      = score_bucket(int(round(event_score)))

    # Commodities this event covers
    commodities = sorted({
        normalize_text(r.get("commodity", ""), "")
        for _, r in event_rows.iterrows()
        if normalize_text(r.get("commodity", ""), "")
    })

    # Explanation from the best row
    why = get_why_it_matters(best_row)

    # Accent colour: green if mostly longs, red if mostly shorts, amber if mixed
    n_long  = sum(1 for s in stock_list if s["direction"] == "Long")
    n_short = sum(1 for s in stock_list if s["direction"] == "Short")
    if n_long >= n_short and n_long:
        accent = "#2ECC71"
    elif n_short > n_long:
        accent = "#E74C3C"
    else:
        accent = "#F39C12"

    trend_map = {
        "worsening":  "↑ Worsening",
        "new":        "★ New",
        "stable":     "→ Stable",
        "recovering": "↓ Recovering",
    }
    trend_label  = trend_map.get(trend_dir, trend_dir.title())
    rank_str     = f"#{rank_number}" if rank_number else ""
    media_str    = "&nbsp;&nbsp;📰 CONFIRMED" if media_val is True else ""
    score_pct    = min(int(event_score * 10), 100)
    commodities_str = "  ·  ".join(commodities)

    # Build individual stock rows
    stock_rows_html = ""
    for s in stock_list[:10]:
        d_color  = "#2ECC71" if s["direction"] == "Long" else "#E74C3C"
        d_arrow  = "▲" if s["direction"] == "Long" else "▼"
        # Cooldown indicator: greyed out with 🔁 when same (sym+region+anomaly) was logged <24h ago
        if s.get("cooldown"):
            sym_color  = "#555"
            score_color = "#444"
            cd_badge   = '<span style="font-size:9px;color:#444;margin-left:4px;">🔁</span>'
        else:
            sym_color  = "#f0f0f0"
            score_color = d_color
            cd_badge   = ""
        stock_rows_html += (
            f'<div style="display:flex;align-items:center;gap:8px;padding:3px 0;'
            f'border-bottom:1px solid rgba(255,255,255,0.04);">'
            f'<span style="color:{d_color};font-size:10px;font-weight:900;width:12px;flex-shrink:0;">{d_arrow}</span>'
            f'<span style="font-family:monospace;font-size:12px;font-weight:700;color:{sym_color};width:48px;flex-shrink:0;">{s["symbol"]}</span>'
            f'<span style="font-size:11px;font-weight:700;color:{score_color};width:28px;flex-shrink:0;">{s["score"]:.1f}</span>'
            f'<span style="font-size:10px;color:#555;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;">{s["role"]}</span>'
            f'{cd_badge}'
            f'</div>'
        )

    card_html = (
        f'<div style="border-left:3px solid {accent};border-radius:6px;'
        f'background:rgba(255,255,255,0.03);padding:16px 18px 14px 18px;'
        f'margin-bottom:2px;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;">'

        f'<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;">'
        f'<span style="font-size:10px;font-weight:700;color:#666;letter-spacing:1px;text-transform:uppercase;">'
        f'{rank_str}&nbsp;&nbsp;{bucket}{media_str}</span>'
        f'<div style="text-align:right;line-height:1;">'
        f'<span style="font-size:26px;font-weight:700;color:white;">{event_score:.1f}</span>'
        f'<span style="font-size:11px;color:#555;">&thinsp;/10</span>'
        f'</div></div>'

        f'<div style="font-size:17px;font-weight:700;color:#f0f0f0;margin-bottom:2px;">{region}</div>'
        f'<div style="font-size:13px;font-weight:700;color:{accent};letter-spacing:0.3px;margin-bottom:4px;">{anomaly}</div>'
        f'<div style="font-size:11px;color:#666;margin-bottom:10px;">'
        f'<span style="color:#888;">{trend_label}</span>&nbsp;&nbsp;·&nbsp;&nbsp;{commodities_str}</div>'

        f'<div style="font-size:12px;color:#888;line-height:1.7;margin-bottom:12px;">{why}</div>'

        f'<div style="background:#1a1a1a;border-radius:3px;height:2px;margin-bottom:12px;">'
        f'<div style="background:{accent};width:{score_pct}%;height:2px;border-radius:3px;"></div></div>'

        f'<div style="font-size:9px;font-weight:700;color:#444;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:6px;">AFFECTED MARKETS</div>'
        + stock_rows_html +
        f'</div>'
    )
    st.markdown(card_html, unsafe_allow_html=True)

    with st.expander("Score breakdown & raw data"):
        d1, d2 = st.columns(2)
        d1.markdown(f"**Commodities**  \n{', '.join(commodities)}")
        d1.markdown(f"**Vehicle**  \n{get_vehicle(best_row)}")
        d1.markdown(
            f"**Forecast window**  \n"
            f"{format_dt(best_row.get('forecast_start'))} → {format_dt(best_row.get('forecast_end'))}"
        )
        score_html = (
            progress_bar_html("Weather Strength", weather_strength) +
            progress_bar_html("Seasonality",      seasonality_score) +
            progress_bar_html("Trend Factor",     trend_factor) +
            progress_bar_html("Edge Score",       edge_score) +
            progress_bar_html("Event Score",      event_score)
        )
        d2.markdown(score_html, unsafe_allow_html=True)

        st.markdown("**Why this signal triggered**")
        for item in build_trigger_evidence(best_row):
            st.caption(f"— {item}")

        details = parse_jsonish(best_row.get("details"))
        if isinstance(details, dict) and details:
            with st.expander("Raw weather data"):
                st.json(details)


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

# Normalise anomaly_type and region — eliminates groupby duplicates from whitespace/casing
df["anomaly_type"] = df["anomaly_type"].str.lower().str.strip()
df["region"] = df["region"].str.strip()  # strip whitespace; keep original casing for display

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

tab_radar, tab_pulse, tab_media, tab_aftermath = st.tabs(["🌍 Radar", "📊 Pulse Trader", "📡 Media Signals", "📈 Aftermath"])

with tab_radar:

    # ── KPI bar ───────────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Prime Signals",  prime_count)
    k2.metric("New Today",      new_count)
    k3.metric("↑ Worsening",    worsening_count)
    k4.metric("Last Update",    format_dt(last_update)[:16] if last_update else "—")
    st.divider()

    # ── Weather-event card grid ────────────────────────────────────────────────
    if filtered.empty:
        st.info("No signals match the current filters.")
    else:
        # Per-row preview score (used only for sorting within a group)
        def _preview_score(row):
            syms = get_stock_trade_symbols(row)
            sym0 = syms[1][0] if syms[1] else ""
            ws = compute_weather_strength(row)
            mq = compute_mapping_quality(row, sym0, syms[0]) if sym0 else 0.0
            eq = compute_execution_quality(row, sym0) if sym0 else 0.0
            ss = compute_seasonality_score(row)
            tf = compute_trend_factor(row)
            es = compute_edge_score(row)
            return compute_final_trade_score(ws, mq, 10.0, eq, ss, tf, edge_score=es)

        top_df = filtered.copy()
        top_df["preview_score"] = top_df.apply(_preview_score, axis=1)

        # Group rows by (region, anomaly_type) — each group = one weather event
        event_groups = []
        for (region_key, anomaly_key), grp in top_df.groupby(
            ["region", "anomaly_type"], sort=False
        ):
            event_score = float(grp["preview_score"].max())
            event_groups.append((event_score, grp))

        # Sort events by best score; deduplicate safety net for any residual casing collisions
        event_groups.sort(key=lambda x: x[0], reverse=True)
        seen_event_keys: set = set()
        deduped: list = []
        for _score, _grp in event_groups:
            key = (
                _grp["region"].iloc[0].strip().lower(),
                _grp["anomaly_type"].iloc[0].strip().lower(),
            )
            if key not in seen_event_keys:
                seen_event_keys.add(key)
                deduped.append((_score, _grp))
        event_groups = deduped[:top_n]

        # ── Cross-event symbol ownership ──────────────────────────────────────
        # Each stock symbol is "owned" by the event where it scores highest.
        # This prevents the same stock from cluttering multiple cards when it
        # is triggered by the same anomaly type across many regions.
        symbol_event_owner: dict[str, int] = {}  # symbol -> event index
        for ei, (evt_score, grp) in enumerate(event_groups):
            for _, _row in grp.iterrows():
                _, syms = get_stock_trade_symbols(_row)
                for _sym in syms:
                    if _sym not in symbol_event_owner or evt_score > event_groups[symbol_event_owner[_sym]][0]:
                        symbol_event_owner[_sym] = ei

        event_owned_symbols: list[set] = [set() for _ in event_groups]
        for _sym, ei in symbol_event_owner.items():
            event_owned_symbols[ei].add(_sym)

        # ── Load 24h cooldown combos (cached in session to avoid repeated DB hits) ──
        if "cooldown_combos" not in st.session_state:
            try:
                from recommendations_tracker import get_recently_recommended_combos
                st.session_state["cooldown_combos"] = get_recently_recommended_combos(hours=24)
            except Exception:
                st.session_state["cooldown_combos"] = set()
        _cooldown_combos = st.session_state.get("cooldown_combos", set())

        # ── Render 2-column grid of weather-event cards ────────────────────────
        for i in range(0, len(event_groups), 2):
            col_l, col_r = st.columns(2, gap="medium")
            with col_l:
                show_weather_event_card(
                    event_groups[i][1], rank_number=i + 1, all_df=filtered,
                    owned_symbols=event_owned_symbols[i],
                    cooldown_combos=_cooldown_combos,
                )
            if i + 1 < len(event_groups):
                with col_r:
                    show_weather_event_card(
                        event_groups[i + 1][1], rank_number=i + 2, all_df=filtered,
                        owned_symbols=event_owned_symbols[i + 1],
                        cooldown_combos=_cooldown_combos,
                    )

    # ── Ranking table (collapsed by default) ──────────────────────────────────
    with st.expander("📊 Full Ranking Table"):
        if filtered_ranked.empty:
            st.write("No signals match the current filters.")
        else:
            display_cols = [
                "Region", "Commodity", "Anomaly", "Trade", "Conviction",
                "Trend", "Vehicle", "Stock Trade", "Final Trade Score",
            ]
            available_cols = [c for c in display_cols if c in filtered_ranked.columns]
            st.dataframe(filtered_ranked[available_cols], use_container_width=True, height=380)

with tab_pulse:
    st.header("🌐 Global Pulse Trader")
    st.caption("One row per equity — aggregates all weather signals per stock.")

    pulse_source = filtered.copy()
    pulse_table = build_global_pulse_trader_table(pulse_source)

    _pulse_cols = ["Date", "Stock Trade", "Trade", "Why It Matters", "Final Trade Score"]

    if pulse_table.empty:
        st.write("No recommendations right now.")
    else:
        display_pulse = pulse_table[
            [c for c in _pulse_cols if c in pulse_table.columns]
        ].copy()
        st.dataframe(display_pulse, use_container_width=True, height=500)

        with st.expander(f"Full detail ({len(pulse_table)} rows)"):
            st.dataframe(pulse_table, use_container_width=True)

with tab_media:
    st.header("📡 Media Signal Validation")
    st.caption("Cross-reference active weather signals against NOAA/NWS official alerts and NewsAPI headlines.")

    from media_validator import MediaValidator, write_validation_to_db

    validator = MediaValidator()
    newsapi_configured = bool(os.environ.get("NEWSAPI_KEY"))

    # ── Status bar ────────────────────────────────────────────────────────────
    status_cols = st.columns(2)
    status_cols[0].success("✅ NOAA/NWS — always live (free public API)")
    if newsapi_configured:
        status_cols[1].success("✅ NewsAPI — connected")
    else:
        status_cols[1].warning("⚠️ NewsAPI — not configured (set NEWSAPI_KEY for news headlines)")

    st.divider()

    # ── Run validation ────────────────────────────────────────────────────────
    top_signals = filtered_ranked[filtered_ranked["Conviction"].isin({"PRIME", "ACTIONABLE"})].copy()

    col_run, col_info = st.columns([1, 4])
    run_clicked = col_run.button("🔍 Run Validation", type="primary")
    col_info.caption(
        f"Will validate **{min(len(top_signals), 20)}** PRIME/ACTIONABLE signals against NOAA alerts"
        + (" and NewsAPI headlines." if newsapi_configured else ". Add NEWSAPI_KEY to also check news headlines.")
    )

    if run_clicked:
        # Build signal list from top ranked rows (map back to DB rows)
        signal_rows = []
        for _, ranked_row in top_signals.head(20).iterrows():
            match = df[
                (df["region"].astype(str) == str(ranked_row.get("Region", ""))) &
                (df["commodity"].astype(str) == str(ranked_row.get("Commodity", ""))) &
                (df["anomaly_type"].astype(str) == normalize_anomaly_key(str(ranked_row.get("Anomaly", "")).lower().replace(" ", "_")))
            ]
            if not match.empty:
                db_row = match.iloc[0]
                signal_rows.append({
                    "id": None,  # no id column in current query; write skipped
                    "region": str(db_row.get("region", "")),
                    "anomaly_type": str(db_row.get("anomaly_type", "")),
                    "commodity": str(db_row.get("commodity", "")),
                    "conviction": ranked_row.get("Conviction", ""),
                    "final_score": ranked_row.get("Final Trade Score", 0),
                    "trade": ranked_row.get("Trade", ""),
                })

        if not signal_rows:
            st.info("No PRIME/ACTIONABLE signals found to validate.")
        else:
            live_results = []
            progress = st.progress(0, text="Validating signals…")
            for i, sig in enumerate(signal_rows):
                summary = validator.validate_signal(
                    signal_id=sig["id"],
                    region=sig["region"],
                    anomaly=sig["anomaly_type"],
                    commodity=sig["commodity"],
                )
                if summary.is_confirmed and summary.best_result:
                    br = summary.best_result
                    live_results.append({
                        "Region": sig["region"],
                        "Anomaly": sig["anomaly_type"].replace("_", " ").title(),
                        "Commodity": sig["commodity"],
                        "Trade": sig["trade"],
                        "Score": sig["final_score"],
                        "Conviction": sig["conviction"],
                        "Source": br.source,
                        "Headline": br.headline,
                        "Match Score": round(br.score, 1),
                        "URL": br.url,
                    })
                progress.progress((i + 1) / len(signal_rows), text=f"Checked {i+1}/{len(signal_rows)}: {sig['region']} {sig['anomaly_type']}")

            progress.empty()

            if live_results:
                st.success(f"✅ {len(live_results)} signal(s) confirmed by external sources")
                live_df = pd.DataFrame(live_results).sort_values("Score", ascending=False)
                st.dataframe(live_df.drop(columns=["URL"]), use_container_width=True)
                st.caption("Sources:")
                for r in live_results:
                    if r["URL"]:
                        st.markdown(f"- [{r['Headline'][:100]}]({r['URL']}) — *{r['Source']}*")
            else:
                st.info("No external confirmation found for current PRIME/ACTIONABLE signals. NOAA may have no active alerts in these regions right now.")

    st.divider()

    # ── DB-persisted confirmations ────────────────────────────────────────────
    has_media_validated = (
        "media_validated" in df.columns
        and df["media_validated"].notna().any()
        and df["media_validated"].eq(True).any()
    )

    st.subheader("📰 Previously Confirmed (from DB)")
    if has_media_validated:
        media_df = df[df["media_validated"] == True].copy()
        cols = ["region", "commodity", "anomaly_type", "signal_level", "trade_bias", "media_headline", "media_source"]
        st.dataframe(media_df[[c for c in cols if c in media_df.columns]], use_container_width=True)
    else:
        st.write("No DB-persisted confirmations yet.")

    st.divider()

    # ── Anomaly coverage ──────────────────────────────────────────────────────
    st.subheader("Anomaly Coverage")
    coverage_counts = (
        df.assign(anomaly_type=df["anomaly_type"].str.lower().str.strip())
        .groupby("anomaly_type")
        .size()
        .reset_index(name="Active Signals")
        .sort_values("Active Signals", ascending=False)
    )
    st.dataframe(coverage_counts, use_container_width=True)

with tab_aftermath:
    st.header("📈 Aftermath — Recommendation Performance")
    st.caption("Tracks every stock recommendation made by the Pulse Trader. Log today's picks, then check back to see how they performed.")

    from recommendations_tracker import (
        ensure_schema, log_recommendations, get_aftermath_table,
        get_performance_summary, get_fetch_errors,
    )

    # Ensure DB table exists
    try:
        ensure_schema()
    except Exception as e:
        st.error(f"DB schema error: {e}")
        st.stop()

    # ── Log today's recommendations ───────────────────────────────────────────
    st.subheader("Log Today's Recommendations")
    aftermath_pulse = build_global_pulse_trader_table(filtered.copy())

    col_log, col_log_info = st.columns([1, 4])
    log_clicked = col_log.button("📌 Log Today's Picks", type="primary")

    _pulse_preview_cols = ["Date", "Stock Trade", "Trade", "Conviction", "Final Trade Score"]
    _available_preview = [c for c in _pulse_preview_cols if c in aftermath_pulse.columns]

    if not aftermath_pulse.empty:
        col_log_info.caption(
            f"**{len(aftermath_pulse[aftermath_pulse['Trade'] != 'No Trade'])}** tradeable recommendations ready to log "
            f"(entry prices fetched live from Yahoo Finance)"
        )
        with st.expander("Preview recommendations to log"):
            st.dataframe(aftermath_pulse[_available_preview], use_container_width=True)
    else:
        col_log_info.caption("No recommendations available with current filters.")

    if log_clicked:
        if aftermath_pulse.empty:
            st.warning("No recommendations to log.")
        else:
            with st.spinner("Fetching entry prices and logging…"):
                try:
                    n = log_recommendations(aftermath_pulse)
                    if n > 0:
                        st.success(f"✅ Logged **{n}** new recommendation(s) with live entry prices.")
                    else:
                        st.info("All of today's recommendations are already logged.")
                except Exception as e:
                    st.error(f"Failed to log: {e}")

    st.divider()

    # ── Live quote refresh ────────────────────────────────────────────────────
    st.subheader("📊 Cumulative Track Record")

    import datetime as _dt
    _now_str = _dt.datetime.now().strftime("%H:%M:%S")

    qcol1, qcol2 = st.columns([1, 4])
    fetch_clicked = qcol1.button("🔄 Fetch Quotes Now", type="secondary")

    # Session state: cache table + timestamp so it doesn't re-fetch on every widget interaction
    if "aftermath_df" not in st.session_state or "aftermath_fetched_at" not in st.session_state:
        st.session_state["aftermath_df"] = None
        st.session_state["aftermath_fetched_at"] = None

    # Auto-fetch on first load OR when button clicked
    if st.session_state["aftermath_df"] is None or fetch_clicked:
        try:
            with st.spinner("Fetching live prices from Yahoo Finance…"):
                st.session_state["aftermath_df"] = get_aftermath_table()
                st.session_state["aftermath_fetched_at"] = _dt.datetime.now().strftime("%H:%M:%S")
        except Exception as e:
            st.error(f"Failed to fetch prices: {e}")
            st.session_state["aftermath_df"] = pd.DataFrame()

    fetched_at = st.session_state.get("aftermath_fetched_at", "—")
    qcol2.caption(f"Prices last fetched at **{fetched_at}**. Click to refresh mid-session.")

    aftermath_df = st.session_state["aftermath_df"] if st.session_state["aftermath_df"] is not None else pd.DataFrame()

    # ── API key status (always visible) ──────────────────────────────────────
    import os as _os
    _fhkey = _os.environ.get("FINNHUB_API_KEY", "")
    if _fhkey:
        st.success(f"✅ FINNHUB_API_KEY detected (starts: `{_fhkey[:6]}…`)")
    else:
        st.error(
            "❌ FINNHUB_API_KEY is NOT visible to this server process.  \n"
            "In Render → your service → **Environment** → confirm the variable exists, "
            "then click **Manual Deploy → Deploy latest commit**."
        )

    # ── Price fetch debug info ────────────────────────────────────────────────
    fetch_errs = get_fetch_errors()
    if fetch_errs:
        failed = sum(1 for e in fetch_errs if e.startswith("⛔"))
        with st.expander(f"🔍 Price fetch debug — {failed} symbol(s) failed, click to inspect"):
            for line in fetch_errs:
                st.caption(line)

    if aftermath_df.empty:
        st.info("No recommendations logged yet. Click **Log Today's Picks** above to start tracking.")
    else:
        # ── Performance metrics ───────────────────────────────────────────────
        perf = get_performance_summary(aftermath_df)
        if perf:
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Total Logged", perf["total"])
            m2.metric("Win Rate", f"{perf['win_rate']}%")
            m3.metric("Avg P&L", f"{perf['avg_pnl']:+.2f}%")
            m4.metric("Best Trade", perf["best_trade"])
            m5.metric("Worst Trade", perf["worst_trade"])

        st.divider()

        # ── Colour-coded P&L table ────────────────────────────────────────────
        display_cols = ["Date Logged", "Stock", "Trade", "Entry Price",
                        "Current Price", "P&L %", "Outcome", "Score",
                        "Conviction", "Region", "Anomaly"]

        visible = aftermath_df[[c for c in display_cols if c in aftermath_df.columns]].copy()

        # Filter controls
        fc1, fc2, fc3 = st.columns(3)
        trade_filter = fc1.multiselect("Trade", ["Long", "Short"], default=["Long", "Short"])
        outcome_filter = fc2.multiselect("Outcome", ["✅ Win", "❌ Loss", "➖ Flat", "—"], default=["✅ Win", "❌ Loss", "➖ Flat", "—"])
        conviction_vals = sorted(aftermath_df["Conviction"].dropna().unique().tolist())
        conviction_filter = fc3.multiselect("Conviction", conviction_vals, default=conviction_vals)

        visible = visible[
            visible["Trade"].isin(trade_filter) &
            visible["Outcome"].isin(outcome_filter) &
            visible["Conviction"].isin(conviction_filter)
        ]

        st.dataframe(visible, use_container_width=True, height=500)

        # ── Why column in expander ────────────────────────────────────────────
        with st.expander("Show 'Why It Mattered' for all recommendations"):
            why_cols = ["Date Logged", "Stock", "Trade", "P&L %", "Outcome", "Why"]
            st.dataframe(
                aftermath_df[[c for c in why_cols if c in aftermath_df.columns]],
                use_container_width=True,
            )

        # ── ML Scorer ─────────────────────────────────────────────────────────
        st.markdown("---")
        st.subheader("🤖 ML Trade Scorer")
        st.caption(
            "Trains an XGBoost model on your logged trade outcomes to learn which "
            "weather/region/anomaly combinations actually win. Improves as your "
            "track record grows."
        )

        try:
            from ml_scorer import (
                get_labeled_data, train_model, load_model,
                predict_win_prob, model_info, MIN_SAMPLES,
            )

            labeled_df = get_labeled_data(aftermath_df)
            n_labeled = len(labeled_df)
            info = model_info()

            ml_c1, ml_c2, ml_c3, ml_c4 = st.columns(4)
            ml_c1.metric("Labeled Trades", n_labeled)
            ml_c2.metric("Needed to Train", MIN_SAMPLES)
            ml_c3.metric("Model", "✅ Ready" if info["trained"] else "⏳ Not trained")
            if info["trained"]:
                ml_c4.metric("Trained on", f"{info['n_trained']} trades")

            if n_labeled < MIN_SAMPLES:
                st.info(
                    f"Need **{MIN_SAMPLES - n_labeled} more** labeled trade outcomes. "
                    "Once Finnhub is connected, fetch quotes daily — P&L data "
                    "accumulates automatically."
                )
            else:
                if st.button("🔄 Train / Retrain ML Model", type="primary"):
                    with st.spinner("Training model on your trade history…"):
                        results = train_model(aftermath_df)
                    if "error" in results:
                        st.error(results["error"])
                    else:
                        st.success(
                            f"**{results['model_name']}** trained on "
                            f"{results['n_samples']} trades — "
                            f"CV Accuracy: **{results['accuracy']:.1%}** "
                            f"(±{results['accuracy_std']:.1%})  |  "
                            f"Wins: {results['wins']}  Losses: {results['losses']}"
                        )
                        # Feature importance bar chart
                        feat_df = pd.DataFrame(
                            results["top_features"], columns=["Feature", "Importance"]
                        ).set_index("Feature")
                        st.markdown("**Feature Importance**")
                        st.bar_chart(feat_df)

            # Show ML win-probability on current signals if model is ready
            if info["trained"]:
                st.markdown("**ML Win Probability — Current Signals**")
                ml_pulse = build_global_pulse_trader_table(filtered.copy())
                if not ml_pulse.empty:
                    # Map column names to what ml_scorer expects
                    ml_input = ml_pulse.rename(columns={
                        "Final Trade Score": "Score",
                        "Anomaly Type": "Anomaly",
                    })
                    probs = predict_win_prob(ml_input)
                    if probs is not None:
                        ml_pulse["ML Win Prob"] = (probs * 100).round(1).astype(str) + "%"
                        display_ml = ml_pulse[
                            [c for c in ["Stock Trade", "Trade", "Conviction",
                                         "Final Trade Score", "ML Win Prob",
                                         "Region", "Anomaly"] if c in ml_pulse.columns]
                        ].sort_values("ML Win Prob", ascending=False)
                        st.dataframe(display_ml, use_container_width=True)
                    else:
                        st.info("Could not generate predictions — retrain the model.")

        except Exception as ml_err:
            st.warning(f"ML Scorer unavailable: {ml_err}")
