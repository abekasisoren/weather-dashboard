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


# ─── Z-Score normalization ────────────────────────────────────────────────────
# Maps raw signal_level (1-9) to approximate σ deviations from climatological norm
# Professional meteorological convention: 1σ ≈ notable, 2σ ≈ rare, 3σ ≈ extreme
_SIGNAL_LEVEL_TO_SIGMA: dict[int, float] = {
    1: 0.5,   # Marginal anomaly
    2: 0.8,   # Weak signal
    3: 1.0,   # Moderate (1σ)
    4: 1.2,   # Above moderate
    5: 1.5,   # Notable anomaly
    6: 1.8,   # Significant (approaching 2σ)
    7: 2.2,   # Strong anomaly (2σ+)
    8: 2.6,   # Severe anomaly
    9: 3.0,   # Extreme event (3σ — very rare)
}


def compute_anomaly_zscore(row) -> float:
    """
    Convert signal_level to a Z-score (σ from climatological mean).
    Adjusts for seasonality: off-season events have higher true anomaly σ
    because the baseline climatology is already biased toward calm conditions.
    Returns sigma value (typically 0.5 – 3.5).
    """
    signal = safe_int(row.get("signal_level"))
    base_sigma = _SIGNAL_LEVEL_TO_SIGMA.get(signal, 1.0)

    # Seasonality adjustment: off-season anomalies are climatologically rarer
    season_score = compute_seasonality_score(row)
    if season_score >= 8.0:      # Off-season — true sigma is materially higher
        sigma_multiplier = 1.40
    elif season_score >= 5.5:    # Fringe season — mild boost
        sigma_multiplier = 1.15
    else:                        # Peak season — already priced in, sigma as-is
        sigma_multiplier = 1.00

    return round(base_sigma * sigma_multiplier, 2)


def _zscore_to_weather_boost(sigma: float) -> float:
    """
    Convert Z-score (σ) to a weather strength multiplier.
    1σ = neutral (1.0×), 3σ = +35% boost, <0.5σ = −35% penalty.
    Uses a linear interpolation scheme (pragmatic approximation of hedge-fund
    exponential weighting — keeps scores interpretable on the 0-10 scale).
    """
    sigma = max(0.3, min(sigma, 3.5))
    if sigma <= 1.0:
        boost = 0.65 + (sigma - 0.3) / (1.0 - 0.3) * 0.35
    elif sigma <= 2.0:
        boost = 1.00 + (sigma - 1.0) / (2.0 - 1.0) * 0.20
    else:
        boost = 1.20 + (sigma - 2.0) / (3.5 - 2.0) * 0.15
    return round(boost, 3)


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


# ─── Phenological timing calendar ────────────────────────────────────────────
# Maps (commodity, anomaly_type) → {month: sensitivity_multiplier}
# 2.0 = critical crop stage (maximum market impact)
# 1.0 = neutral impact
# 0.3 = dormancy / off-season (minimal impact)
# Source: professional agricultural trading desk phenological calendars
_PHENO_CALENDAR: dict[tuple[str, str], dict[int, float]] = {
    # CORN (US Midwest: planted Apr-May, pollinates Jul, harvests Sep-Oct)
    ("corn", "drought"):    {1: 0.2, 2: 0.2, 3: 0.3, 4: 0.6, 5: 0.9, 6: 1.4, 7: 2.0, 8: 1.7, 9: 1.1, 10: 0.5, 11: 0.2, 12: 0.2},
    ("corn", "heatwave"):   {1: 0.1, 2: 0.1, 3: 0.2, 4: 0.5, 5: 0.8, 6: 1.3, 7: 2.0, 8: 1.6, 9: 0.9, 10: 0.4, 11: 0.1, 12: 0.1},
    ("corn", "frost"):      {1: 0.3, 2: 0.3, 3: 0.8, 4: 1.6, 5: 2.0, 6: 1.2, 7: 0.4, 8: 0.4, 9: 1.5, 10: 1.8, 11: 0.3, 12: 0.3},
    ("corn", "heavy_rain"): {1: 0.2, 2: 0.2, 3: 0.5, 4: 1.2, 5: 1.8, 6: 1.0, 7: 0.8, 8: 0.7, 9: 0.6, 10: 0.4, 11: 0.2, 12: 0.2},
    # SOYBEANS (planted May-Jun, pod-fills Aug, harvests Sep-Oct)
    ("soybeans", "drought"):  {1: 0.2, 2: 0.2, 3: 0.2, 4: 0.4, 5: 0.7, 6: 1.2, 7: 1.7, 8: 2.0, 9: 1.3, 10: 0.6, 11: 0.2, 12: 0.2},
    ("soybeans", "heatwave"): {1: 0.1, 2: 0.1, 3: 0.2, 4: 0.4, 5: 0.6, 6: 1.1, 7: 1.6, 8: 2.0, 9: 1.2, 10: 0.5, 11: 0.1, 12: 0.1},
    ("soybeans", "frost"):    {1: 0.2, 2: 0.2, 3: 0.6, 4: 1.8, 5: 2.0, 6: 0.8, 7: 0.3, 8: 0.3, 9: 1.3, 10: 1.7, 11: 0.2, 12: 0.2},
    # WHEAT (winter: planted Oct, vernalises Dec-Feb, heads May-Jun, harvests Jul)
    ("wheat", "drought"):   {1: 0.7, 2: 0.8, 3: 1.3, 4: 1.6, 5: 2.0, 6: 1.8, 7: 0.5, 8: 1.2, 9: 0.6, 10: 1.1, 11: 1.0, 12: 0.8},
    ("wheat", "frost"):     {1: 1.0, 2: 1.2, 3: 1.8, 4: 2.0, 5: 1.5, 6: 0.4, 7: 0.3, 8: 0.4, 9: 0.8, 10: 1.5, 11: 1.3, 12: 1.0},
    ("wheat", "heatwave"):  {1: 0.4, 2: 0.5, 3: 0.9, 4: 1.4, 5: 2.0, 6: 1.7, 7: 0.5, 8: 0.9, 9: 0.4, 10: 0.5, 11: 0.4, 12: 0.4},
    ("wheat", "cold_wave"): {1: 1.2, 2: 1.4, 3: 1.8, 4: 2.0, 5: 0.8, 6: 0.3, 7: 0.2, 8: 0.3, 9: 0.6, 10: 1.3, 11: 1.2, 12: 1.1},
    # COFFEE (Brazil: flowering Aug-Sep, cherry develops Oct-Apr, harvest May-Aug)
    ("coffee", "drought"):  {1: 0.7, 2: 0.7, 3: 0.6, 4: 0.8, 5: 1.2, 6: 1.6, 7: 2.0, 8: 1.9, 9: 1.5, 10: 0.9, 11: 0.7, 12: 0.7},
    ("coffee", "frost"):    {1: 0.8, 2: 0.9, 3: 0.7, 4: 0.5, 5: 1.0, 6: 1.8, 7: 2.0, 8: 1.7, 9: 1.0, 10: 0.6, 11: 0.6, 12: 0.7},
    ("coffee", "heatwave"): {1: 0.5, 2: 0.5, 3: 0.6, 4: 0.8, 5: 1.0, 6: 1.3, 7: 1.7, 8: 2.0, 9: 1.6, 10: 0.9, 11: 0.6, 12: 0.5},
    # COCOA (West Africa: main harvest Oct-Mar, mid-crop Apr-Sep)
    ("cocoa", "drought"):    {1: 0.8, 2: 0.7, 3: 0.7, 4: 1.0, 5: 1.3, 6: 1.6, 7: 2.0, 8: 1.8, 9: 1.5, 10: 0.9, 11: 0.8, 12: 0.8},
    ("cocoa", "heavy_rain"): {1: 1.0, 2: 0.9, 3: 1.2, 4: 1.5, 5: 1.8, 6: 1.4, 7: 0.8, 8: 0.7, 9: 0.9, 10: 1.6, 11: 1.3, 12: 1.0},
    # NATURAL GAS (demand-driven by temperature extremes, not a crop)
    ("natural gas", "polar_vortex"): {1: 2.0, 2: 1.8, 3: 1.0, 4: 0.5, 5: 0.3, 6: 0.3, 7: 0.4, 8: 0.4, 9: 0.5, 10: 0.8, 11: 1.4, 12: 1.9},
    ("natural gas", "cold_wave"):    {1: 1.9, 2: 1.7, 3: 1.1, 4: 0.6, 5: 0.3, 6: 0.3, 7: 0.3, 8: 0.4, 9: 0.5, 10: 0.9, 11: 1.5, 12: 1.8},
    ("natural gas", "heatwave"):     {1: 0.3, 2: 0.3, 3: 0.5, 4: 0.7, 5: 1.0, 6: 1.5, 7: 2.0, 8: 1.8, 9: 1.2, 10: 0.6, 11: 0.4, 12: 0.3},
    ("natural gas", "ice_storm"):    {1: 1.8, 2: 1.6, 3: 0.9, 4: 0.4, 5: 0.3, 6: 0.3, 7: 0.3, 8: 0.3, 9: 0.4, 10: 0.7, 11: 1.3, 12: 1.7},
    # SUGAR (Brazil: planting Dec-Jan, crush Apr-Nov)
    ("sugar", "drought"): {1: 0.6, 2: 0.7, 3: 0.9, 4: 1.3, 5: 1.6, 6: 1.8, 7: 2.0, 8: 1.9, 9: 1.5, 10: 1.1, 11: 0.7, 12: 0.6},
    ("sugar", "frost"):   {1: 0.5, 2: 0.5, 3: 0.6, 4: 0.8, 5: 1.0, 6: 1.4, 7: 1.8, 8: 1.6, 9: 1.1, 10: 0.7, 11: 0.5, 12: 0.5},
    # COTTON (planted Apr-May, boll sets Jul-Aug, harvests Sep-Oct)
    ("cotton", "drought"):  {1: 0.3, 2: 0.3, 3: 0.5, 4: 0.8, 5: 1.2, 6: 1.6, 7: 2.0, 8: 1.9, 9: 1.4, 10: 0.8, 11: 0.4, 12: 0.3},
    ("cotton", "heatwave"): {1: 0.2, 2: 0.2, 3: 0.4, 4: 0.7, 5: 1.1, 6: 1.5, 7: 2.0, 8: 1.8, 9: 1.2, 10: 0.6, 11: 0.3, 12: 0.2},
    # RICE (SE Asia: transplant Apr-Jun, grain-fill Aug, harvest Sep-Nov)
    ("rice", "monsoon_failure"): {1: 0.3, 2: 0.3, 3: 0.5, 4: 1.0, 5: 1.5, 6: 2.0, 7: 2.0, 8: 1.8, 9: 1.5, 10: 1.0, 11: 0.5, 12: 0.3},
    ("rice", "drought"):         {1: 0.4, 2: 0.4, 3: 0.6, 4: 1.0, 5: 1.5, 6: 2.0, 7: 1.9, 8: 1.7, 9: 1.3, 10: 0.8, 11: 0.4, 12: 0.4},
    ("rice", "flood_risk"):      {1: 0.5, 2: 0.5, 3: 0.6, 4: 0.9, 5: 1.4, 6: 1.8, 7: 2.0, 8: 1.8, 9: 1.4, 10: 1.0, 11: 0.5, 12: 0.5},
    # PALM OIL (SE Asia: year-round, peak yield Jul-Sep)
    ("palm oil", "drought"):         {1: 0.6, 2: 0.6, 3: 0.7, 4: 0.8, 5: 1.0, 6: 1.2, 7: 1.8, 8: 2.0, 9: 1.8, 10: 1.2, 11: 0.7, 12: 0.6},
    ("palm oil", "monsoon_failure"): {1: 0.5, 2: 0.5, 3: 0.6, 4: 0.8, 5: 1.2, 6: 1.6, 7: 2.0, 8: 2.0, 9: 1.7, 10: 1.1, 11: 0.6, 12: 0.5},
    # OLIVE OIL (Mediterranean: flowering Apr-Jun, harvest Oct-Dec)
    ("olive oil", "drought"):  {1: 0.5, 2: 0.5, 3: 0.8, 4: 1.5, 5: 2.0, 6: 1.8, 7: 1.3, 8: 1.0, 9: 0.8, 10: 1.4, 11: 1.0, 12: 0.5},
    ("olive oil", "heatwave"): {1: 0.4, 2: 0.4, 3: 0.7, 4: 1.4, 5: 2.0, 6: 1.9, 7: 1.4, 8: 1.1, 9: 0.8, 10: 0.6, 11: 0.4, 12: 0.4},
    # CANOLA (Canadian Prairies: planted Apr, flowers Jun, harvest Aug-Sep)
    ("canola", "drought"): {1: 0.2, 2: 0.2, 3: 0.3, 4: 0.7, 5: 1.2, 6: 2.0, 7: 1.8, 8: 1.5, 9: 0.7, 10: 0.3, 11: 0.2, 12: 0.2},
    ("canola", "frost"):   {1: 0.3, 2: 0.3, 3: 0.8, 4: 1.8, 5: 2.0, 6: 1.2, 7: 0.5, 8: 0.6, 9: 1.0, 10: 0.4, 11: 0.3, 12: 0.3},
    # OIL (hurricane season peaks Aug-Sep; geopolitical risk year-round)
    ("oil", "hurricane_risk"): {1: 0.3, 2: 0.3, 3: 0.3, 4: 0.4, 5: 0.6, 6: 0.9, 7: 1.4, 8: 2.0, 9: 2.0, 10: 1.2, 11: 0.4, 12: 0.3},
    ("oil", "extreme_wind"):   {1: 0.7, 2: 0.7, 3: 0.8, 4: 0.9, 5: 0.9, 6: 1.0, 7: 1.1, 8: 1.3, 9: 1.5, 10: 1.3, 11: 1.0, 12: 0.8},
    ("oil", "storm_wind"):     {1: 0.8, 2: 0.8, 3: 0.9, 4: 0.9, 5: 1.0, 6: 1.1, 7: 1.2, 8: 1.4, 9: 1.6, 10: 1.4, 11: 1.0, 12: 0.9},
}

# Human-readable crop/demand stage names per (commodity, anomaly) + month
_PHENO_STAGE_NAMES: dict[tuple[str, str], dict[int, str]] = {
    ("corn", "drought"):         {4: "Planting", 5: "Emergence", 6: "Silking", 7: "Pollination", 8: "Grain Fill", 9: "Harvest", 10: "Harvest"},
    ("corn", "heatwave"):        {5: "Emergence", 6: "Silking", 7: "Pollination", 8: "Grain Fill"},
    ("corn", "frost"):           {4: "Planting", 5: "Emergence", 9: "Harvest", 10: "Harvest"},
    ("soybeans", "drought"):     {5: "Emergence", 6: "Vegetative", 7: "Flowering", 8: "Pod Fill", 9: "Maturation"},
    ("soybeans", "heatwave"):    {7: "Flowering", 8: "Pod Fill", 9: "Maturation"},
    ("soybeans", "frost"):       {4: "Planting", 5: "Emergence", 9: "Maturation", 10: "Harvest"},
    ("wheat", "drought"):        {4: "Tillering", 5: "Heading", 6: "Grain Fill"},
    ("wheat", "frost"):          {3: "Green-Up", 4: "Tillering", 5: "Heading", 10: "Winter Plant", 11: "Winter Plant"},
    ("wheat", "heatwave"):       {5: "Heading", 6: "Grain Fill"},
    ("coffee", "drought"):       {6: "Fruit Set", 7: "Cherry Dev", 8: "Cherry Fill", 9: "Pre-Harvest"},
    ("coffee", "frost"):         {6: "Cherry Dev", 7: "Cherry Dev", 8: "Cherry Fill"},
    ("natural gas", "polar_vortex"): {12: "Peak Demand", 1: "Peak Demand", 2: "Peak Demand"},
    ("natural gas", "cold_wave"):    {12: "Winter Demand", 1: "Winter Demand", 2: "Winter Demand"},
    ("natural gas", "heatwave"):     {7: "Cooling Demand", 8: "Peak Cooling"},
    ("natural gas", "ice_storm"):    {12: "Demand Surge", 1: "Demand Surge", 2: "Demand Surge"},
    ("sugar", "drought"):        {7: "Stalk Dev", 8: "Stalk Dev"},
    ("cotton", "drought"):       {7: "Boll Set", 8: "Boll Fill"},
    ("rice", "monsoon_failure"): {6: "Transplanting", 7: "Tillering"},
    ("palm oil", "drought"):     {7: "Peak Yield", 8: "Peak Yield"},
    ("olive oil", "drought"):    {4: "Pre-Flower", 5: "Flowering"},
    ("canola", "drought"):       {5: "Rosette", 6: "Flowering"},
    ("oil", "hurricane_risk"):   {8: "Peak Season", 9: "Peak Season"},
}


def compute_phenological_multiplier(row) -> tuple[float, str]:
    """
    Return (multiplier, stage_label) for the current commodity + anomaly + month.
    multiplier: 0.3 (dormancy/off-season) to 2.0 (critical stage)
    stage_label: human-readable crop stage (e.g. "Pollination", "Peak Demand")
    Returns (1.0, "") when no calendar entry exists — neutral default.
    """
    import datetime
    commodity = normalize_text(row.get("commodity"), "").lower().strip()
    anomaly   = normalize_anomaly_key(row.get("anomaly_type", ""))
    month     = datetime.datetime.now().month

    key = (commodity, anomaly)
    calendar = _PHENO_CALENDAR.get(key)
    if calendar is None:
        return 1.0, ""

    multiplier = calendar.get(month, 1.0)
    stage_name = _PHENO_STAGE_NAMES.get(key, {}).get(month, "")
    return round(multiplier, 2), stage_name


def compute_weather_strength(row) -> float:
    """Compute weather strength score (0-10), boosted by Z-score rarity of the event.
    Rare off-season anomalies (high σ) are multiplied up; weak marginal signals are penalised."""
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
    # Apply Z-score boost: rare off-season anomalies get a strength multiplier
    sigma = compute_anomaly_zscore(row)
    boost = _zscore_to_weather_boost(sigma)
    return round(clamp(base * boost), 2)


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
    pheno_multiplier: float = 1.0,
) -> float:
    """Compute final trade score (0-10).
    Weights: weather_strength 0.30, mapping_quality 0.20, conflict_cleanliness 0.10,
             execution_quality 0.10, seasonality 0.10, trend 0.10, edge_score 0.10.
    After weighted sum + confluence bonus, apply phenological multiplier:
      2.0 = critical crop stage (score boosted up to 2×, capped at 10)
      1.0 = neutral (no change)
      0.3 = dormancy / off-season (score heavily discounted)
    """
    score = (
        0.30 * weather_strength +
        0.20 * mapping_quality +
        0.10 * conflict_cleanliness +
        0.10 * execution_quality +
        0.10 * seasonality_score +
        0.10 * trend_factor +
        0.10 * edge_score
    ) + confluence_bonus
    # Phenological multiplier: crop-stage sensitivity adjusts the final score
    pheno = max(0.3, min(pheno_multiplier, 2.0))
    score = score * pheno
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
        sigma_score = compute_anomaly_zscore(row)

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
                    "Sigma Score": sigma_score,
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

        # Pheno multiplier from winner row (original df row context)
        pheno_mult_pulse, _pheno_stage_pulse = compute_phenological_multiplier(winner)
        final_trade_score = compute_final_trade_score(
            weather_strength=weather_strength,
            mapping_quality=mapping_quality,
            conflict_cleanliness=cleanliness,
            execution_quality=execution_quality,
            seasonality_score=seasonality_score,
            trend_factor=trend_factor,
            confluence_bonus=conf_bonus,
            edge_score=edge_score,
            pheno_multiplier=pheno_mult_pulse,
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
                "Sigma Score": round(float(winner.get("Sigma Score", 1.0)), 3),
                "Pheno Mult": round(pheno_mult_pulse, 3),
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
        pheno_mult, _pheno_stage = compute_phenological_multiplier(row)
        final_trade_score = compute_final_trade_score(
            weather_strength=weather_strength,
            mapping_quality=mapping_quality,
            conflict_cleanliness=10.0,
            execution_quality=execution_quality,
            seasonality_score=seasonality_score,
            trend_factor=trend_factor,
            confluence_bonus=conf_bonus,
            edge_score=edge_score,
            pheno_multiplier=pheno_mult,
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
                "Pheno Mult": round(pheno_mult, 2),
                "Final Trade Score": round(final_trade_score, 2),
            }
        )

    ranked_df = pd.DataFrame(rows)
    ranked_df = ranked_df.sort_values(
        by=["Final Trade Score", "Signal", "Region", "Commodity"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)

    return ranked_df


# ─── Reasoning infrastructure ─────────────────────────────────────────────────
# Trade thesis: causal chain from weather event to equity price impact.
# Keys: (anomaly_type_key, "Long" | "Short")
TRADE_THESIS: dict[tuple[str, str], str] = {
    # HEATWAVE
    ("heatwave", "Long"): (
        "Heatwaves drive a surge in cooling energy demand — natural gas and power grid stress lift "
        "utility revenues and HVAC manufacturers. Agricultural commodities (corn, soybeans, wheat) "
        "face yield risk which lifts ETF prices. The market typically under-prices sustained "
        "multi-day heat before the first confirmed crop condition downgrades."
    ),
    ("heatwave", "Short"): (
        "Extreme heat stresses crops during sensitive growth stages, raising input costs for food "
        "manufacturers and compressing margins at consumer staples companies. Airlines and logistics "
        "face operational disruptions. Property & casualty insurers see elevated heat-related claims."
    ),
    # EXTREME_HEAT
    ("extreme_heat", "Long"): (
        "Extreme heat events are structurally bullish for energy infrastructure, HVAC, and water "
        "utilities — demand spikes that utilities often cannot fully hedge in advance. Agricultural "
        "commodity futures benefit as yield outlooks are revised sharply downward."
    ),
    ("extreme_heat", "Short"): (
        "Food companies face abrupt input cost spikes as crop yields deteriorate. Property & casualty "
        "insurers accumulate heat-related claims from infrastructure damage. Outdoor "
        "labour-intensive industries (construction, agriculture) see productivity and revenue declines."
    ),
    # FROST
    ("frost", "Long"): (
        "Late-season frost destroys standing crops and damages perennial plants (coffee, grapes, citrus) "
        "— a direct supply shock that takes months to recover. Commodity ETFs and futures respond "
        "sharply within 48–72 hours of confirmation. Fertiliser companies benefit as farmers replant. "
        "Heating fuel demand spikes short-term."
    ),
    ("frost", "Short"): (
        "Food and beverage companies relying on affected crops face margin compression as raw material "
        "costs surge. Airlines and logistics face disruption costs. Reinsurance companies with "
        "agricultural crop book exposure accumulate losses."
    ),
    # DROUGHT
    ("drought", "Long"): (
        "Drought is the single most powerful supply-side shock to agricultural commodities. Persistent "
        "multi-week deficits at critical crop stages cause irreversible yield loss — the market "
        "typically takes 4–6 weeks to fully price the supply reduction. Water utility stocks benefit "
        "from elevated pricing power. Fertiliser demand spikes in the following season as "
        "farmers rebuild yields."
    ),
    ("drought", "Short"): (
        "Agricultural commodity consumers (food manufacturers, ethanol producers, livestock feed buyers) "
        "face sharply higher input costs. Hydropower utilities see reduced generation capacity. "
        "Shipping companies with river-route exposure (Mississippi, Rhine, Paraná) face draft "
        "restrictions and delayed cargoes."
    ),
    # HEAVY_RAIN
    ("heavy_rain", "Long"): (
        "Heavy rainfall events create reconstruction demand — homebuilders, building material companies, "
        "and home improvement retailers historically see elevated revenues in the 1–3 months following "
        "major flood events. Infrastructure and water management firms benefit from government "
        "emergency spending."
    ),
    ("heavy_rain", "Short"): (
        "Property & casualty insurers face elevated claims. Agricultural producers suffer waterlogged "
        "fields that delay planting, cause root rot, and destroy standing crops. Logistics companies "
        "face route disruptions and shipment delays."
    ),
    # FLOOD_RISK
    ("flood_risk", "Long"): (
        "Flood risk signals translate to reconstruction spend — home improvement, building materials, "
        "and infrastructure plays typically outperform for 2–6 months post-event. Water management "
        "infrastructure companies see accelerated project pipelines. Agricultural futures spike "
        "on supply disruption."
    ),
    ("flood_risk", "Short"): (
        "Catastrophe insurers and reinsurers bear the direct financial loss from flood claims. "
        "Historical Brazilian and Southeast Asian flood events have triggered $2–8B in industry "
        "claims. Affected agricultural exporters (sugar, coffee, soy) face logistics disruption "
        "and quality degradation."
    ),
    # COLD_WAVE
    ("cold_wave", "Long"): (
        "Cold waves create immediate natural gas demand spikes — utilities scramble to secure supply "
        "and spot prices can double within 48 hours of onset. Heating fuel distributors and LNG "
        "shippers benefit. HVAC service companies see emergency repair surges."
    ),
    ("cold_wave", "Short"): (
        "Cold snaps disrupt logistics networks and outdoor operations. Agricultural sectors face crop "
        "freeze risk. Airlines face delays and cancellations. Retail foot traffic drops sharply "
        "in affected regions, pressuring consumer discretionary revenues."
    ),
    # STORM_WIND
    ("storm_wind", "Long"): (
        "Severe wind events disrupt energy infrastructure and logistics — short-term power price spikes "
        "benefit merchant generators. Reconstruction plays (roofing, building materials) see elevated "
        "demand in the 1–4 months following. Offshore energy operators may face disruption premiums."
    ),
    ("storm_wind", "Short"): (
        "Property insurers face structural and roof damage claims. Offshore energy producers face "
        "temporary shutdown costs. Shipping and logistics companies experience port delays and cargo "
        "damage. Agricultural producers lose standing crops to wind damage."
    ),
    # WILDFIRE_RISK
    ("wildfire_risk", "Long"): (
        "Wildfire risk creates elevated demand for emergency management, fire retardant chemicals, and "
        "rebuilding materials. Air quality deterioration boosts indoor air purification product sales. "
        "Backup power generators (GNRC) see accelerated demand as grid stability is threatened."
    ),
    ("wildfire_risk", "Short"): (
        "Utilities in fire-prone regions face enormous liability exposure — Pacific Gas & Electric's "
        "2018 bankruptcy is the template. Property insurers face escalating claims and may withdraw "
        "coverage from high-risk areas. Tourism and outdoor recreation revenues collapse during "
        "active fire events."
    ),
    # HURRICANE_RISK
    ("hurricane_risk", "Long"): (
        "Hurricane events create one of the largest near-term reconstruction demand surges in any "
        "weather category — home improvement retailers (HD, LOW) historically see 5–15% revenue "
        "lifts in the quarter following a major storm. Offshore energy disruption is bullish for "
        "oil prices short-term."
    ),
    ("hurricane_risk", "Short"): (
        "Catastrophe reinsurers face peak loss events — a single major Gulf Coast hurricane can wipe "
        "out an entire year of underwriting profit. Offshore oil producers suffer production shutdowns "
        "averaging 2–6 weeks per major event. Coastal real estate companies face pricing and "
        "insurance market disruption."
    ),
    # POLAR_VORTEX
    ("polar_vortex", "Long"): (
        "Polar vortex events are the most extreme natural gas demand shock in the weather trading "
        "playbook. The February 2021 Texas event sent Henry Hub spot prices from $3/MMBtu to over "
        "$1,200/MMBtu in 72 hours. Pipeline operators, LNG shippers, and gas storage companies "
        "benefit enormously. Heating oil distributors face a demand surge they cannot always fulfil."
    ),
    ("polar_vortex", "Short"): (
        "Industrial manufacturers face sharp production cost spikes as energy prices surge. Airlines "
        "suffer mass cancellations and operational disruption. Automotive and retail logistics break "
        "down across entire supply chains. Property insurers face burst-pipe and freeze damage "
        "claims at scale."
    ),
    # ATMOSPHERIC_RIVER
    ("atmospheric_river", "Long"): (
        "Atmospheric river events in California and the Pacific Northwest deliver extreme precipitation "
        "that simultaneously creates flood damage (reconstruction demand) and replenishes reservoirs "
        "(hydropower benefit). Water infrastructure companies and drought-recovery plays benefit."
    ),
    ("atmospheric_river", "Short"): (
        "Flooding from atmospheric rivers disrupts California's produce belt and Central Valley farming "
        "operations. Property insurers face elevated claims. Infrastructure companies face project "
        "delays as construction halts during flood events."
    ),
    # MONSOON_FAILURE
    ("monsoon_failure", "Long"): (
        "A failed monsoon in South or Southeast Asia is a major supply shock for rice, sugar, palm oil, "
        "and cotton. Global food price inflation follows 3–6 months later. Agricultural commodity "
        "ETFs and fertiliser companies benefit as farmers rebuild depleted soil on 6–12 month horizons."
    ),
    ("monsoon_failure", "Short"): (
        "Food companies with heavy Asia-Pacific sourcing exposure face sharply higher input costs. "
        "Indian and Southeast Asian consumer staples companies suffer margin compression. Micro-finance "
        "institutions with rural agricultural loan books face elevated default risk."
    ),
    # ICE_STORM
    ("ice_storm", "Long"): (
        "Ice storms are highly disruptive to power grids — ice accumulation on lines causes outages "
        "that take days to restore. Natural gas demand spikes as backup heating kicks in. Emergency "
        "generator companies and utilities with peaker capacity benefit. Road salt and de-icing "
        "companies see elevated demand."
    ),
    ("ice_storm", "Short"): (
        "Property insurers face roof collapse, vehicle damage, and structural claims. Airlines suffer "
        "severe cancellations and fleet damage. Retailers see foot traffic collapse. Infrastructure "
        "operators face costly repair programmes for power lines and roads."
    ),
    # EXTREME_WIND
    ("extreme_wind", "Long"): (
        "Extreme wind events at offshore energy installations force production shutdowns — bullish for "
        "crude oil and natural gas spot prices short-term. Emergency response contractors and marine "
        "insurance companies benefit. Reconstruction of damaged infrastructure drives materials demand."
    ),
    ("extreme_wind", "Short"): (
        "Offshore oil and gas producers face forced shutdowns and equipment damage. Marine insurers "
        "accumulate claims. Shipping companies face port closures and cargo delays. Coastal agriculture "
        "faces crop damage that takes multiple seasons to recover."
    ),
}

# What would weaken or invalidate each signal — checklist for the analyst
INVALIDATION_CONDITIONS: dict[str, str] = {
    "heatwave": (
        "**Watch for:** ECMWF next-run showing the heat dome weakening or moving offshore. Check the "
        "500hPa geopotential height ridge — if it breaks, temperatures normalise within 72h. Also watch: "
        "La Niña transition (historically cools Midwest summers). Conviction drops if signal_level falls "
        "below 5 or if USDA crop condition Good/Excellent ratings hold above 60%."
    ),
    "extreme_heat": (
        "**Watch for:** Upper-level ridge pattern breaking down in the 5-day ensemble. Pacific SST "
        "anomalies — a marine layer incursion can cap coastal temperatures within 48h. Conviction drops "
        "sharply if persistence falls to zero consecutive model runs."
    ),
    "frost": (
        "**Watch for:** Warm air mass advection forecast within the next 5 days. Check 850hPa temperature "
        "anomalies — if they flip positive, the cold outbreak is over. Also critical: if the affected "
        "crop has already passed its vulnerable growth stage (post-harvest), market impact is minimal."
    ),
    "drought": (
        "**Watch for:** ECMWF showing significant precipitation (≥25mm in 7 days) in the affected region, "
        "or La Niña transitioning to ENSO-neutral (typically ends dry spells across the Americas). "
        "Key confirmation: USDA weekly crop condition ratings — if Good/Excellent rises above 65%, "
        "the market is healing and the trade thesis has largely played out."
    ),
    "heavy_rain": (
        "**Watch for:** The weather system dissipating with the next 7-day forecast showing normal "
        "precipitation. River gauge levels — once they recede below flood stage, the acute disruption "
        "phase ends. Also: USDA replanting window — if farmers can replant within 2 weeks, yield "
        "loss is limited and the bullish commodity thesis weakens."
    ),
    "flood_risk": (
        "**Watch for:** The upstream weather system weakening, or river basin drainage occurring faster "
        "than forecast (check real-time gauge data). Government flood relief announcements can dampen "
        "insurance stock downside by capping private sector liability. Conviction drops if no confirmed "
        "infrastructure damage is reported within 48h of onset."
    ),
    "cold_wave": (
        "**Watch for:** The arctic air mass retreating faster than forecast — track 850hPa temperature "
        "contours. Natural gas storage data (weekly EIA report) reveals whether the market has already "
        "priced the demand spike (gas price flat despite cold = trade largely priced in)."
    ),
    "storm_wind": (
        "**Watch for:** The storm system weakening below gale-force thresholds in the next ECMWF run. "
        "National Hurricane Center advisories (for tropical systems) or European met agency severe weather "
        "warnings. Check Baltic Dry Index for early routing changes by shipping companies."
    ),
    "wildfire_risk": (
        "**Watch for:** Onshore wind patterns shifting (reducing fire spread risk) or humidity rising "
        "above 30% in the affected region. NIFC containment percentages — above 75% containment, the "
        "acute stock impact is mostly priced. Also watch: utility emergency shutoff orders being lifted "
        "as a leading indicator of normalisation."
    ),
    "hurricane_risk": (
        "**Watch for:** National Hurricane Center downgrading the system or tracking it away from land. "
        "Offshore energy impact depends critically on whether the storm passes within 100 miles of Gulf "
        "production infrastructure. Daily NHC forecast cone narrowing is the key data point. Below "
        "26°C Atlantic SSTs, hurricane systems weaken rapidly."
    ),
    "polar_vortex": (
        "**Watch for:** The stratospheric warming event (SSW) that splits the polar vortex beginning to "
        "recover — typically a 3–4 week process. Track 10hPa temperature at 90°N. Also critical: LNG "
        "import capacity and storage inventory levels — a well-supplied market absorbs the demand shock "
        "more easily and the gas price spike may be capped earlier than the weather signal implies."
    ),
    "atmospheric_river": (
        "**Watch for:** The atmospheric river train (consecutive ARs) breaking as the Pacific jet stream "
        "weakens or shifts north. Track integrated vapour transport (IVT) forecasts from NOAA. Key pivot: "
        "if ARs are refilling reservoirs without causing flooding, the signal shifts from bearish "
        "crop/infrastructure to bullish hydropower."
    ),
    "monsoon_failure": (
        "**Watch for:** IMD (India Meteorological Department) or BMKG (Indonesia) upgrading the monsoon "
        "outlook. Track the Indian Ocean Dipole (IOD) index — a positive IOD is the key driver of "
        "monsoon failure; if it reverses, expect rapid recovery. Government-subsidised irrigation can "
        "partially compensate for rainfall deficit, limiting the yield loss."
    ),
    "ice_storm": (
        "**Watch for:** Surface temperatures rising above 2°C within the forecast window — precipitation "
        "shifts from freezing rain to regular rain. Track surface dew point and 850hPa wet-bulb zero "
        "level. Also: utility companies' ice storm restoration ETAs — rapid grid restoration (<48h) "
        "contains the insurance loss estimates."
    ),
    "extreme_wind": (
        "**Watch for:** The storm system weakening in the next ECMWF run or changing track away from "
        "offshore infrastructure. Check offshore rig operator daily status reports and vessel tracking "
        "portals — early platform evacuation signals the market is already pricing the disruption. "
        "If no production shutdown is announced within 24h of onset, market impact is likely overstated."
    ),
}

# Crop stage narratives: (commodity, anomaly_type) → detailed explanation
PHENO_NARRATIVE: dict[tuple[str, str], str] = {
    ("corn", "drought"): (
        "Corn's most drought-sensitive period is **pollination (July)**, when the tassel sheds pollen "
        "and the silk must be receptive simultaneously. A 5-day drought during pollination can cause "
        "30–50% yield loss. Outside this window (e.g., January–March), corn is dormant or not yet "
        "planted — drought has virtually no crop market impact."
    ),
    ("corn", "heatwave"): (
        "Heat stress above **35°C during pollination (July)** kills pollen and causes kernel abortion. "
        "Studies show a 6% yield loss per degree above 29°C during grain fill (August). Before May "
        "planting, heat has minimal crop impact but may signal a difficult growing season ahead."
    ),
    ("corn", "frost"): (
        "Frost at **planting time (April–May)** destroys germinating seeds and forces replanting, "
        "delaying the growing season by 2–4 weeks. Late-season frost (September–October) kills the "
        "plant before harvest, locking in yield loss. Mid-summer frost is extremely rare and "
        "immediately market-moving."
    ),
    ("soybeans", "drought"): (
        "Soybeans are most sensitive to drought during **pod fill (August)**, when water stress directly "
        "reduces the number of seeds per pod. A 2-week August drought can cut yields by 20–40%. "
        "Pre-planting drought (before May) has minimal direct yield impact but signals a late-start "
        "season and elevated risk later in the season."
    ),
    ("soybeans", "heatwave"): (
        "Heat stress above **32°C during flowering (July)** disrupts pollen viability and pod set. "
        "The critical window is narrow — just 2–3 weeks — but irreversible. August heat during pod "
        "fill compounds the damage. Off-season heat (winter months) has negligible crop impact."
    ),
    ("soybeans", "frost"): (
        "**Planting-time frost (April–May)** is extremely damaging, forcing replanting that shifts the "
        "whole season later. Early harvest frost (September–October) kills plants before full pod "
        "maturity, directly reducing protein content and yield. Mid-summer frost is very rare for "
        "soybeans in commercial growing regions."
    ),
    ("wheat", "drought"): (
        "Wheat is most vulnerable to drought at **heading and grain fill (May–June** for winter wheat). "
        "Spring dryness reduces the number of kernels per head; grain-fill drought shrinks kernel size. "
        "Combined, these can cut yields by 25–45%. Winter dormancy drought (December–February) has "
        "limited immediate impact but increases spring moisture stress risk."
    ),
    ("wheat", "frost"): (
        "**Green-up frost (March–April)** is the most damaging — the plant has broken dormancy but is "
        "still vulnerable to tissue freeze. A single night below −4°C at heading destroys the crop. "
        "Vernalisation frosts (December–February) are actually necessary and beneficial for winter "
        "wheat yield — the plant requires cold to properly flower."
    ),
    ("wheat", "heatwave"): (
        "Heat stress above **30°C during heading and early grain fill (May–June)** accelerates "
        "maturation and reduces kernel weight. This is sometimes called 'heat blasting' — "
        "a rapid deterioration from which the crop cannot recover. A 1-week heat event during "
        "this window can reduce yields by 15–25%."
    ),
    ("coffee", "drought"): (
        "Coffee trees require consistent moisture during **cherry development (July–September** in "
        "Brazil). Drought stress at this stage causes premature cherry drop and smaller bean size. "
        "The 2021 Brazilian drought-frost combination cut global Arabica production by ~30%, driving "
        "coffee ETF (JO) +120% over 12 months. Off-season drought affects the following year's crop."
    ),
    ("coffee", "frost"): (
        "Coffee trees are extremely frost-sensitive — **a single night below 0°C during cherry "
        "development (June–August)** destroys the crop. Brazil's July frost events are the most feared "
        "weather risk in the soft commodities market. Recovery takes 1–2 years for mature trees to "
        "resume full yield — making this a multi-year supply story."
    ),
    ("coffee", "heatwave"): (
        "Sustained heat above **34°C during cherry fill (August)** causes heat stress that reduces "
        "bean quality and weight. Unlike frost, heat damage is gradual. The impact on tradeable "
        "prices tends to emerge 2–4 months after the growing season, when shipment quality "
        "assessments confirm the damage."
    ),
    ("cocoa", "drought"): (
        "Cocoa pods develop over **5–6 months** and require consistent moisture throughout. Drought "
        "during pod development (the long dry season, June–August in West Africa) causes premature "
        "pod death and reduces mid-crop output. The 2023–24 West Africa drought was the largest supply "
        "shock in 20 years, driving cocoa prices to all-time highs above $10,000/tonne."
    ),
    ("cocoa", "heavy_rain"): (
        "Excessive rainfall during cocoa harvest (October–March, main crop) promotes **black pod "
        "disease** — a fungal pathogen (Phytophthora palmivora) that can destroy 20–30% of a crop. "
        "Heavy rain at harvest also delays picking and fermentation, reducing quality and export value."
    ),
    ("natural gas", "polar_vortex"): (
        "Polar vortex events create the **single largest short-term natural gas demand surge** in the "
        "weather trading calendar. The February 2021 Texas event drove Henry Hub spot prices from "
        "$3/MMBtu to over $1,200/MMBtu — a 400× spike — in under 72 hours. Pipeline freeze-offs "
        "simultaneously cut supply while demand surges, creating a perfect demand-supply dislocation."
    ),
    ("natural gas", "cold_wave"): (
        "Cold waves drive residential and industrial heating demand well above seasonal norms. "
        "The **EIA weekly storage report** is the key confirmation signal — a draw larger than the "
        "5-year seasonal average typically accelerates the gas price move. The trade thesis is "
        "strongest in early winter (November–January) when storage is still being actively drawn."
    ),
    ("natural gas", "heatwave"): (
        "Summer heatwaves drive natural gas demand for **power generation cooling**. Gas-fired peaker "
        "plants are dispatched to meet air conditioning load. The hottest weeks of summer (July–August) "
        "see gas burns at power plants approach winter heating levels. This is the second seasonal "
        "demand peak for gas, often underpriced by the market."
    ),
    ("natural gas", "ice_storm"): (
        "Ice storms disrupt **pipeline and LNG terminal operations** while simultaneously spiking "
        "heating demand. Ice accumulation on meters, regulators, and compressor stations can cause "
        "emergency shutdowns. Combined demand surge and supply disruption is a powerful short-term "
        "gas price catalyst."
    ),
    ("sugar", "drought"): (
        "Sugarcane is a 12-month crop — drought during the **vegetative growth phase (June–August** "
        "in Brazil's Center-South) reduces sucrose content and stalk biomass. A 10% rainfall deficit "
        "during this period typically translates to a 5–8% production shortfall. Brazil produces ~40% "
        "of global sugar — its weather is the single largest price driver for the commodity."
    ),
    ("cotton", "drought"): (
        "Cotton is most vulnerable to drought during **boll set and fill (July–August)**. Water stress "
        "at this stage causes boll shedding, reducing the number of harvestable bolls. A prolonged "
        "drought during the boll development window can cut yields by 30–40%, directly impacting "
        "textile manufacturers' raw material costs."
    ),
    ("rice", "monsoon_failure"): (
        "The Asian rice crop is almost entirely rain-fed — **a failed or delayed monsoon is existential** "
        "for the growing season. Transplanting requires standing water; if the monsoon doesn't "
        "arrive by June–July, farmers delay or abandon the main season crop. India, Thailand, and "
        "Vietnam together supply 65% of global rice exports — a monsoon failure affects global food security."
    ),
    ("rice", "drought"): (
        "Rice requires consistent water availability throughout the growing season, making it one of "
        "the most drought-sensitive staple crops. The **tillering and heading stages (June–August** in "
        "Asia) are most critical — water deficit at this point directly reduces the number of grains "
        "per panicle and final yield."
    ),
    ("palm oil", "drought"): (
        "Palm oil production is relatively drought-resilient but a prolonged dry period (3+ months) "
        "causes stress that reduces **fresh fruit bunch (FFB) yields 9–18 months later** — a long-lag "
        "effect that the market often underprices at the time of the drought. July–September "
        "corresponds to peak annual production in Malaysia and Indonesia."
    ),
    ("palm oil", "monsoon_failure"): (
        "A failed monsoon dramatically reduces the soil moisture available for palm oil trees during "
        "the **critical mid-year growing period**. Unlike seasonal crops, palm oil effects materialise "
        "with a lag of 6–18 months as the trees respond to cumulative water deficit. This "
        "lagged impact is a consistent source of market mispricing."
    ),
    ("olive oil", "drought"): (
        "The **olive flowering period (April–May** in Mediterranean regions) is critically sensitive "
        "to water stress — drought at this stage reduces fruit set and directly constrains annual "
        "production. European olive oil prices hit 40-year highs in 2023–24 as successive Iberian "
        "droughts decimated back-to-back crops."
    ),
    ("olive oil", "heatwave"): (
        "Heat above **38°C during flowering (April–May)** causes pollen sterility and abscission of "
        "young fruit. The effect is irreversible within the season. Since olive trees are biennial "
        "(alternating heavy and light years), a heat-damaged flowering year creates a multi-year "
        "supply shortfall."
    ),
    ("canola", "drought"): (
        "Canadian canola **flowering (June)** is the most water-sensitive growth stage — heat and "
        "drought during bloom cause pod shatter and blank seeds. The Canadian Prairies produce 75% "
        "of global canola exports. A drought signal in June/July is extremely market-moving for "
        "canola futures and NTR (Nutrien) as farmers face replanting decisions."
    ),
    ("canola", "frost"): (
        "Spring frost at **planting time (April–May)** is the primary frost risk for canola — "
        "seedlings are extremely vulnerable to temperatures below −3°C. A late-season frost "
        "(September) can also damage pods before harvest. The 2020 Alberta early frost destroyed "
        "~15% of the crop within a single week."
    ),
    ("oil", "hurricane_risk"): (
        "The US Gulf of Mexico hosts ~15% of US oil production in shallow offshore platforms. "
        "A **Category 3+ storm tracking within 100 miles** of the main production corridor triggers "
        "mandatory evacuation — historically removing 1.5–2.5 million barrels/day for 2–6 weeks. "
        "The market typically begins pricing this 72h before landfall."
    ),
    ("oil", "extreme_wind"): (
        "Extreme wind events force **emergency shutdown of offshore platforms and pipeline systems**. "
        "North Sea shutdowns during winter storms (October–March) are a recurring supply disruption. "
        "Each major shutdown removes 500k–1.5M bbl/day. The oil price response is typically "
        "immediate (within 24h of shutdown announcement) and mean-reverts within 2–3 weeks."
    ),
}


def build_teacher_narrative(
    best_row,
    event_sigma: float,
    weather_strength: float,
    seasonality_score: float,
    trend_factor: float,
    edge_score: float,
    conf_bonus: float,
    stock_list: list,
    event_score: float,
    region: str,
    anomaly_key: str,
    anomaly_display: str,
    commodities: list,
    trend_dir: str,
    rank_number: int,
    bucket: str,
    all_df: "pd.DataFrame",
) -> str:
    """
    Generate a flowing, conversational 'teacher explains to classroom' narrative
    for a weather event card. Returns a markdown string.
    Every sentence is data-driven — no hardcoded facts, only templates filled
    from computed score values.
    """
    import datetime

    lines: list[str] = []
    month_name = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December",
    }.get(datetime.datetime.now().month, "")

    norm_key = anomaly_key.lower().replace(" ", "_")
    long_stocks  = [s for s in stock_list if s["direction"] == "Long"  and not s.get("cooldown")]
    short_stocks = [s for s in stock_list if s["direction"] == "Short" and not s.get("cooldown")]
    top_commodity = commodities[0] if commodities else ""

    # ── Para 1: Opening ────────────────────────────────────────────────────────
    rank_str = f"#{rank_number} " if rank_number else ""
    conviction_language = {
        "PRIME":      "This is a high-conviction signal — the evidence is strong enough to act on right now.",
        "ACTIONABLE": "This is a solid signal — strong enough to consider an active position.",
        "WATCH":      "This is a developing signal — worth monitoring but not quite ready for a full position.",
        "EARLY":      "This is an early-stage signal — the system spotted something, but conviction is still building.",
    }.get(bucket, "This is an active weather signal.")

    lines.append(
        f"**{region}** is showing **{anomaly_display}** and the system has ranked it "
        f"{rank_str}with a score of **{event_score:.1f}/10** ({bucket}). {conviction_language}"
    )

    # ── Para 2: Sigma ──────────────────────────────────────────────────────────
    if event_sigma >= 3.0:
        sigma_plain = (
            f"The **{event_sigma:.1f}σ** badge is the first thing to understand here. "
            f"That sigma number — standing for standard deviations from the long-run average — "
            f"tells you this is a generational extreme. Think of it this way: if you looked at "
            f"weather records for {region} going back 100 years, you'd barely find a handful of "
            f"events this intense at this time of year. The market is very well-equipped to price "
            f"expected seasonal risk — a summer heatwave in Texas, a winter freeze in Europe. "
            f"What it consistently fails to price is the truly extraordinary outlier. At {event_sigma:.1f}σ, "
            f"we are firmly in that territory."
        )
    elif event_sigma >= 2.0:
        sigma_plain = (
            f"The **{event_sigma:.1f}σ** badge is key context. That number measures how far this "
            f"event deviates from the long-run average for {region} at this time of year — in terms "
            f"of standard deviations. At {event_sigma:.1f}σ, you're looking at an event that occurs "
            f"roughly once every 20–50 years in this region and season. The market does a decent job "
            f"pricing the routine seasonal patterns, but rare events like this one tend to be "
            f"systematically underpriced — because most models and most traders anchor to "
            f"what is 'normal', not what is possible."
        )
    elif event_sigma >= 1.5:
        sigma_plain = (
            f"The **{event_sigma:.1f}σ** badge tells you this is an unusual but not extreme event — "
            f"something that happens roughly once per decade in this region. At 1.5σ+, institutional "
            f"weather desks start taking the signal seriously. Below 1.5σ, most trading desks would "
            f"treat the event as seasonal noise. We're above that threshold here, which is why "
            f"the system has generated a trade recommendation."
        )
    else:
        sigma_plain = (
            f"The **{event_sigma:.1f}σ** badge reflects a moderate anomaly — notable, but not unusual "
            f"enough that the market is caught off guard. Most institutional weather desks require at "
            f"least 1.5σ before entering a position. The score here is being driven more by "
            f"mapping quality, trend, and execution factors than by the raw weather intensity. "
            f"If the signal strengthens in the next ECMWF run, conviction will increase."
        )
    lines.append(sigma_plain)

    # ── Para 3: Score drivers ──────────────────────────────────────────────────
    ws_desc = (
        "very high" if weather_strength >= 8 else
        "strong"    if weather_strength >= 6 else
        "moderate"  if weather_strength >= 4 else
        "relatively weak"
    )
    score_driver_parts = [
        f"The **weather strength component** — which carries 30% of the final score — is coming in at "
        f"**{weather_strength:.1f}/10**, which is {ws_desc}."
    ]
    if weather_strength >= 7:
        score_driver_parts.append(
            f"That tells you the underlying ECMWF data is compelling: the signal level, persistence "
            f"(how many consecutive forecast runs have confirmed this), severity of the physical "
            f"readings, and market relevance of the affected commodities are all pointing in the "
            f"same direction."
        )
    elif weather_strength >= 5:
        score_driver_parts.append(
            f"The ECMWF data shows a credible signal, but it hasn't been persistent enough across "
            f"multiple model runs to reach maximum conviction. Watch whether the next 12z or 00z "
            f"run confirms or weakens this reading."
        )
    else:
        score_driver_parts.append(
            f"The ECMWF data is flagging something, but the physical readings aren't yet extreme "
            f"enough on their own to drive a high score. The score here is being sustained more by "
            f"the mapping quality and execution factors."
        )
    lines.append(" ".join(score_driver_parts))

    # ── Para 4: Seasonality ────────────────────────────────────────────────────
    if seasonality_score >= 8.0:
        season_para = (
            f"Here's something important: {anomaly_display} in **{month_name}** is genuinely "
            f"unexpected for {region}. The system scores this as an off-season event "
            f"(seasonality: {seasonality_score:.1f}/10), which significantly boosts the final "
            f"score. The logic is straightforward — markets price seasonal risk reasonably well. "
            f"A flood in Brazil during peak wet season? Partially expected. A flood during a "
            f"month when conditions are normally calm? The market hasn't positioned for it, "
            f"which is exactly where the alpha lives."
        )
    elif seasonality_score >= 5.0:
        season_para = (
            f"In terms of timing, {anomaly_display} in {month_name} is somewhat unusual "
            f"for {region} — a fringe-season event (seasonality: {seasonality_score:.1f}/10). "
            f"The market has some awareness of seasonal risk, but not full positioning. "
            f"Think of it as the market being partially asleep — it knows this could happen, "
            f"but probably hasn't fully hedged."
        )
    else:
        season_para = (
            f"The timing here is worth noting: {anomaly_display} in {month_name} is actually "
            f"typical for {region} — this is peak season for this type of event "
            f"(seasonality: {seasonality_score:.1f}/10). That means the market is already "
            f"pricing some level of seasonal risk. The edge here comes from the **magnitude** "
            f"being larger than the seasonal baseline, not from the event being a surprise."
        )
    lines.append(season_para)

    # ── Para 5: Trend ──────────────────────────────────────────────────────────
    trend_paras = {
        "new": (
            f"Critically, this signal is **brand new** — it appeared for the first time in the "
            f"latest ECMWF model run. It is not yet in Reuters, Bloomberg, or any sell-side "
            f"weather report. This is the core of the weather-trading alpha thesis: systematic "
            f"early detection. ECMWF typically detects meaningful weather events **7–10 days** "
            f"before they appear in financial media. Right now, you have a **48–96 hour** window "
            f"before the wider market begins to notice."
        ),
        "worsening": (
            f"The signal is **actively getting worse** — it has intensified since the previous "
            f"model run. This is actually when conviction should be highest: the system has "
            f"confirmed a trend rather than a one-off data point, and conditions are still "
            f"moving in the direction that makes the trade more compelling. "
            f"At this stage, the urgency to act is real — once the signal peaks and begins "
            f"recovering, the best entry window closes."
        ),
        "stable": (
            f"The signal has been **stable** across multiple model runs — the same intensity, "
            f"confirmed day after day. That persistence is actually a double-edged sword. "
            f"On one hand, it tells you this is a reliable, well-established signal that "
            f"the model isn't going to revise away overnight. On the other, the market has "
            f"had more time to notice it, so some of the alpha may already be priced. "
            f"The trade is valid but the entry window has narrowed."
        ),
        "recovering": (
            f"Pay close attention here: the signal is **recovering** — it was stronger in "
            f"previous model runs and is now easing. In trading terms, this usually means "
            f"the best entry opportunity has already passed. If you're already in a position, "
            f"this is a signal to think about trimming. If you're not in yet, the risk/reward "
            f"has shifted — the weather event is moving past its peak impact on the market."
        ),
    }
    lines.append(trend_paras.get(trend_dir, f"Signal trend: {trend_dir}."))

    # ── Para 6: Stock recommendations ─────────────────────────────────────────
    if long_stocks or short_stocks:
        stock_para_parts: list[str] = ["**Now, let's talk about the stock recommendations.**"]

        if long_stocks:
            sym_list = ", ".join(s["symbol"] for s in long_stocks[:5])
            roles = list({s["role"] for s in long_stocks[:5] if s.get("role")})
            role_str = f" ({', '.join(roles[:3])})" if roles else ""
            long_thesis = TRADE_THESIS.get((norm_key, "Long"), "")
            stock_para_parts.append(
                f"The system is recommending **Long** positions in: **{sym_list}**{role_str}. "
                f"{'Here is the causal chain: ' + long_thesis if long_thesis else ''}"
            )

        if short_stocks:
            sym_list = ", ".join(s["symbol"] for s in short_stocks[:5])
            roles = list({s["role"] for s in short_stocks[:5] if s.get("role")})
            role_str = f" ({', '.join(roles[:3])})" if roles else ""
            short_thesis = TRADE_THESIS.get((norm_key, "Short"), "")
            stock_para_parts.append(
                f"The system is recommending **Short** positions in: **{sym_list}**{role_str}. "
                f"{'These companies get hurt because: ' + short_thesis if short_thesis else ''}"
            )

        top_score = stock_list[0]["score"] if stock_list else event_score
        if top_score >= 8:
            stock_para_parts.append(
                f"All the recommended stocks are scoring close to **{top_score:.1f}/10** — "
                f"they all inherit the same strong event-level inputs (weather strength, "
                f"seasonality, trend, edge) and the mapping from this weather event to these "
                f"sectors is clean and direct."
            )
        lines.append(" ".join(stock_para_parts))

    # ── Para 7: Pheno ──────────────────────────────────────────────────────────
    top_pheno_stage = next((s["pheno_stage"] for s in stock_list if s.get("pheno_stage")), "")
    top_pheno_mult  = next((s["pheno_mult"]  for s in stock_list if s.get("pheno_stage")), 1.0)
    pheno_key = (top_commodity.lower().strip(), norm_key)
    pheno_text = PHENO_NARRATIVE.get(pheno_key, "")

    if pheno_text and top_pheno_stage:
        if top_pheno_mult >= 1.7:
            pheno_intro = (
                f"The crop stage multiplier deserves special attention here. "
                f"**{top_commodity.title()}** is currently in its **{top_pheno_stage}** stage "
                f"— this is the most sensitive window in the entire growing season, which is "
                f"why the system applies a **{top_pheno_mult:.1f}×** multiplier to the final score. "
            )
        elif top_pheno_mult >= 1.2:
            pheno_intro = (
                f"The crop stage is relevant here: **{top_commodity.title()}** is in "
                f"**{top_pheno_stage}** ({top_pheno_mult:.1f}× sensitivity). "
            )
        elif top_pheno_mult <= 0.6:
            pheno_intro = (
                f"Interestingly, the crop stage is actually **dampening** this signal. "
                f"**{top_commodity.title()}** is in **{top_pheno_stage}** — a low-sensitivity "
                f"period — which is why the system applies only a **{top_pheno_mult:.1f}×** multiplier. "
                f"The weather event is real, but the crop doesn't care much about it right now. "
            )
        else:
            pheno_intro = (
                f"**{top_commodity.title()}** is in its **{top_pheno_stage}** stage "
                f"({top_pheno_mult:.1f}× sensitivity). "
            )
        lines.append(pheno_intro + pheno_text)

    # ── Para 8: Confluence ─────────────────────────────────────────────────────
    norm_anomaly_for_lookup = norm_key
    same_anomaly_regions = all_df[
        all_df["anomaly_type"].str.strip().str.lower().str.replace(" ", "_") == norm_anomaly_for_lookup
    ]["region"].dropna().unique().tolist()

    if len(same_anomaly_regions) >= 3:
        confluence_para = (
            f"One of the most powerful aspects of this signal is **regional confluence**. "
            f"It's not just {region} — **{len(same_anomaly_regions)} separate regions** are all "
            f"showing {anomaly_display} at the same time: "
            f"*{', '.join(same_anomaly_regions[:5])}*. "
            f"When the same weather anomaly hits multiple production origins simultaneously, "
            f"the commodity price impact is magnified far beyond what any single-region event "
            f"would cause. This is the kind of signal that moves markets at scale."
        )
    elif len(same_anomaly_regions) == 2:
        other = [r for r in same_anomaly_regions if r.strip().lower() != region.strip().lower()]
        other_str = other[0] if other else same_anomaly_regions[0]
        confluence_para = (
            f"This signal has a **dual-region dimension**: both **{region}** and **{other_str}** "
            f"are showing {anomaly_display} simultaneously. When two major production regions "
            f"are affected at once, the commodity market impact is typically larger and more "
            f"durable than a single-origin event."
        )
    else:
        confluence_para = (
            f"This is a **single-region signal** — only {region} is showing this anomaly right now. "
            f"That's not a problem, but it's worth knowing: the largest commodity price moves "
            f"historically come from multi-region confluence events. If you see similar signals "
            f"developing in other {top_commodity} production regions, that would significantly "
            f"increase conviction."
        )
    lines.append(confluence_para)

    # ── Para 9: Edge/timing ────────────────────────────────────────────────────
    media_val = best_row.get("media_validated")
    if media_val is True:
        edge_para = (
            f"**One important caveat on timing:** this event has already been picked up by "
            f"financial media. That means the market is actively digesting this information. "
            f"The edge score reflects this — at 3.0/10, we're telling you the alpha window is "
            f"closing. This doesn't mean the trade is wrong, but it does mean the best entry "
            f"prices may have already passed. Size accordingly."
        )
    else:
        edge_para = (
            f"**The alpha window:** right now, this event is not in financial media. ECMWF "
            f"is detecting it before Reuters, Bloomberg, or any sell-side weather desk has "
            f"written about it. That gap — between when the atmosphere tells you something "
            f"and when the market finally hears about it — is where weather-driven trading "
            f"generates its edge. This window is typically **48–96 hours** for events of this "
            f"type, and you're at the beginning of it."
        )
    lines.append(edge_para)

    # ── Para 10: What to watch ─────────────────────────────────────────────────
    invalidation = INVALIDATION_CONDITIONS.get(norm_key, "")
    if invalidation:
        # Extract just the first sentence/clause for a brief closing note
        first_clause = invalidation.split(".")[0].replace("**Watch for:** ", "").strip()
        lines.append(
            f"**Finally, what to watch:** the signal would weaken or invalidate if "
            f"{first_clause.lower()}. See the *What Would Invalidate This Signal* section "
            f"below for the full checklist of data points to monitor."
        )

    return "\n\n".join(lines)


def _sigma_frequency_label(sigma: float) -> str:
    """Convert Z-score to plain-English frequency estimate."""
    if sigma >= 3.0:
        return "generational extreme — fewer than 0.3% of historical records reach this intensity"
    elif sigma >= 2.5:
        return "once-in-a-generation event — occurs roughly 1 in 80–100 years in this region and season"
    elif sigma >= 2.0:
        return "rare event — occurs roughly 1 in every 20–50 years in this region and season"
    elif sigma >= 1.5:
        return "unusual event — occurs roughly 1 in every 10–15 years for this region and season"
    elif sigma >= 1.0:
        return "notable but not unprecedented — occurs a few times per decade in this region"
    else:
        return "marginal deviation from normal — market is likely already partially aware of conditions"


def build_score_decomposition_html(
    weather_strength: float,
    mapping_quality: float,
    seasonality_score: float,
    trend_factor: float,
    edge_score: float,
    execution_quality: float,
    conf_bonus: float,
    top_pheno_mult: float,
    event_score: float,
    accent: str = "#5DADE2",
) -> str:
    """Build an HTML score decomposition table showing every component's weighted contribution."""
    components = [
        ("Weather Strength",  0.30, weather_strength),
        ("Mapping Quality",   0.20, mapping_quality),
        ("Seasonality",       0.10, seasonality_score),
        ("Trend Factor",      0.10, trend_factor),
        ("Edge Score",        0.10, edge_score),
        ("Execution Quality", 0.10, execution_quality),
    ]

    rows_html = ""
    for name, weight, score in components:
        contrib = weight * score
        pct = min(int(score * 10), 100)
        bar_color = "#2ECC71" if score >= 7 else "#F39C12" if score >= 5 else "#E74C3C"
        rows_html += (
            f'<tr style="border-bottom:1px solid rgba(255,255,255,0.03);">'
            f'<td style="padding:5px 10px;font-size:11px;color:#aaa;">{name}</td>'
            f'<td style="padding:5px 10px;font-size:10px;color:#555;text-align:center;">{weight:.0%}</td>'
            f'<td style="padding:5px 10px;">'
            f'<div style="display:flex;align-items:center;gap:6px;">'
            f'<div style="background:#1a1a1a;border-radius:2px;height:4px;width:64px;flex-shrink:0;">'
            f'<div style="background:{bar_color};width:{pct}%;height:4px;border-radius:2px;"></div></div>'
            f'<span style="font-size:11px;color:#ddd;font-weight:600;">{score:.1f}</span>'
            f'</div></td>'
            f'<td style="padding:5px 10px;font-size:11px;color:#777;text-align:right;">+{contrib:.2f}</td>'
            f'</tr>'
        )

    if conf_bonus > 0:
        rows_html += (
            f'<tr style="border-bottom:1px solid rgba(255,255,255,0.03);">'
            f'<td style="padding:5px 10px;font-size:11px;color:#aaa;">Confluence Bonus</td>'
            f'<td style="padding:5px 10px;font-size:10px;color:#555;text-align:center;">+add</td>'
            f'<td style="padding:5px 10px;font-size:11px;color:#F39C12;">multi-region</td>'
            f'<td style="padding:5px 10px;font-size:11px;color:#F39C12;text-align:right;">+{conf_bonus:.2f}</td>'
            f'</tr>'
        )

    if top_pheno_mult != 1.0:
        pheno_color = "#F39C12" if top_pheno_mult >= 1.5 else "#5DADE2" if top_pheno_mult >= 1.0 else "#E74C3C"
        rows_html += (
            f'<tr style="border-bottom:1px solid rgba(255,255,255,0.03);">'
            f'<td style="padding:5px 10px;font-size:11px;color:#aaa;">Pheno Multiplier</td>'
            f'<td style="padding:5px 10px;font-size:10px;color:#555;text-align:center;">×mult</td>'
            f'<td style="padding:5px 10px;font-size:11px;color:{pheno_color};">{top_pheno_mult:.2f}×</td>'
            f'<td style="padding:5px 10px;font-size:11px;color:{pheno_color};text-align:right;">×crop stage</td>'
            f'</tr>'
        )

    rows_html += (
        f'<tr style="border-top:1px solid rgba(255,255,255,0.10);">'
        f'<td colspan="3" style="padding:8px 10px;font-size:12px;font-weight:700;color:#f0f0f0;">FINAL SCORE</td>'
        f'<td style="padding:8px 10px;font-size:15px;font-weight:700;color:{accent};text-align:right;">{event_score:.1f}/10</td>'
        f'</tr>'
    )

    return (
        f'<table style="width:100%;border-collapse:collapse;background:rgba(0,0,0,0.25);'
        f'border-radius:6px;overflow:hidden;margin-bottom:4px;">'
        f'<thead>'
        f'<tr style="border-bottom:1px solid rgba(255,255,255,0.07);">'
        f'<th style="padding:6px 10px;font-size:9px;font-weight:700;color:#444;letter-spacing:1px;'
        f'text-transform:uppercase;text-align:left;">Component</th>'
        f'<th style="padding:6px 10px;font-size:9px;font-weight:700;color:#444;letter-spacing:1px;'
        f'text-transform:uppercase;text-align:center;">Weight</th>'
        f'<th style="padding:6px 10px;font-size:9px;font-weight:700;color:#444;letter-spacing:1px;'
        f'text-transform:uppercase;text-align:left;">Score</th>'
        f'<th style="padding:6px 10px;font-size:9px;font-weight:700;color:#444;letter-spacing:1px;'
        f'text-transform:uppercase;text-align:right;">Contrib</th>'
        f'</tr>'
        f'</thead>'
        f'<tbody>{rows_html}</tbody>'
        f'</table>'
    )


def build_detailed_reasoning(
    best_row,
    event_sigma: float,
    weather_strength: float,
    seasonality_score: float,
    trend_factor: float,
    edge_score: float,
    conf_bonus: float,
    stock_list: list,
    event_score: float,
    region: str,
    anomaly_key: str,
    anomaly_display: str,
    commodities: list,
    trend_dir: str,
    rank_number: int,
    bucket: str,
    all_df: "pd.DataFrame",
    accent: str = "#5DADE2",
    media_article_url: str = "",
    media_headline: str = "",
    media_source: str = "",
    media_pickup_ago: str = "",
) -> None:
    """Render detailed reasoning inside an st.expander — educational breakdown of every score component."""
    import datetime

    # ── Derive top-stock context ───────────────────────────────────────────────
    top_stock = stock_list[0] if stock_list else {}
    top_pheno_mult  = top_stock.get("pheno_mult", 1.0)
    top_pheno_stage = top_stock.get("pheno_stage", "")
    top_commodity   = top_stock.get("commodity", "")
    top_symbol      = top_stock.get("symbol", "")
    top_direction   = top_stock.get("direction", "Long")
    mq = compute_mapping_quality(best_row, top_symbol, top_direction) if top_symbol else 0.0
    eq = compute_execution_quality(best_row, top_symbol) if top_symbol else 0.0

    current_month = datetime.datetime.now().month
    month_name = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December",
    }.get(current_month, "")

    # ── 0. Teacher narrative (conversational classroom-style explanation) ──────
    st.markdown("#### 🎓 Plain-English Explanation")
    teacher_text = build_teacher_narrative(
        best_row=best_row,
        event_sigma=event_sigma,
        weather_strength=weather_strength,
        seasonality_score=seasonality_score,
        trend_factor=trend_factor,
        edge_score=edge_score,
        conf_bonus=conf_bonus,
        stock_list=stock_list,
        event_score=event_score,
        region=region,
        anomaly_key=anomaly_key,
        anomaly_display=anomaly_display,
        commodities=commodities,
        trend_dir=trend_dir,
        rank_number=rank_number,
        bucket=bucket,
        all_df=all_df,
    )
    st.markdown(teacher_text)
    st.markdown("---")

    # ── Seasonality narrative ──────────────────────────────────────────────────
    if seasonality_score >= 8.0:
        season_ctx = (
            f"**Off-season anomaly** — {anomaly_key.replace('_',' ')} in {month_name} is "
            f"climatologically unexpected for {region}. The market is very likely under-positioned. "
            f"(Seasonality score: **{seasonality_score:.1f}/10**)"
        )
    elif seasonality_score >= 5.0:
        season_ctx = (
            f"**Fringe-season event** — {anomaly_key.replace('_',' ')} in {month_name} is unusual "
            f"but not unprecedented for {region}. Some seasonal positioning already exists. "
            f"(Seasonality score: **{seasonality_score:.1f}/10**)"
        )
    else:
        season_ctx = (
            f"**Peak-season event** — {anomaly_key.replace('_',' ')} is expected in {month_name} "
            f"for {region}. The market is partially or fully pricing seasonal risk. The alpha comes "
            f"from magnitude, not timing. (Seasonality score: **{seasonality_score:.1f}/10**)"
        )

    # ── Trend narrative ────────────────────────────────────────────────────────
    trend_narratives = {
        "worsening":  (
            "**↑ Worsening** — the signal has intensified since the previous ECMWF model run. "
            "Increasing urgency: the market has less time to position and trade conviction is at "
            "its peak. Consider acting before the next major model run confirms the trend."
        ),
        "new": (
            "**★ New** — this signal appeared for the first time in the latest ECMWF run. It is "
            "not yet in financial media. The alpha window is at its maximum — typically **48–96 hours** "
            "before the event appears in sell-side weather reports or financial news."
        ),
        "stable": (
            "**→ Stable** — the signal has persisted across multiple consecutive model runs without "
            "material change. The market may have partially priced this. Conviction is maintained "
            "but the information edge is diminishing with each passing day."
        ),
        "recovering": (
            "**↓ Recovering** — the weather event is easing. The supply/demand impact is already "
            "largely priced by the market. Consider fading or trimming existing positions — the "
            "trade thesis has likely passed its optimal entry window."
        ),
    }
    trend_narrative = trend_narratives.get(trend_dir, f"Trend direction: {trend_dir.title()}")

    # ── Confluence narrative ───────────────────────────────────────────────────
    norm_anomaly_key = anomaly_key.lower().replace(" ", "_")
    same_anomaly_df = all_df[
        all_df["anomaly_type"].str.strip().str.lower().str.replace(" ", "_") == norm_anomaly_key
    ]
    same_anomaly_regions = same_anomaly_df["region"].dropna().unique().tolist()
    if len(same_anomaly_regions) >= 3:
        confluence_text = (
            f"**Multi-region convergence (+1.0 bonus):** {len(same_anomaly_regions)} separate regions "
            f"are simultaneously showing {anomaly_key.replace('_',' ')}: "
            f"*{', '.join(same_anomaly_regions[:5])}*. "
            f"This is a global-scale signal — commodity supply disruption across multiple production "
            f"origins is historically one of the most powerful and durable price catalysts."
        )
    elif len(same_anomaly_regions) == 2:
        confluence_text = (
            f"**Dual-region signal (+0.5 bonus):** {' and '.join(same_anomaly_regions)} are both "
            f"showing {anomaly_key.replace('_',' ')} simultaneously. Concurrent multi-origin "
            f"disruption amplifies the commodity price impact beyond what either event would "
            f"cause independently."
        )
    else:
        confluence_text = (
            f"**Single-region event (no confluence bonus):** Only {region} is showing this anomaly. "
            f"The trade thesis is valid but lacks the multi-origin amplification that drives the "
            f"largest commodity moves. Monitor other production regions for developing signals."
        )

    # ── Edge score narrative ───────────────────────────────────────────────────
    media_val = best_row.get("media_validated")
    if media_val is True:
        edge_text = (
            "**Edge score 3.0/10 — Closing window.** This event has been picked up by financial "
            "media. The market is actively pricing this signal. Position sizing should reflect "
            "reduced information advantage — the easiest gains are already made."
        )
    elif media_val is False:
        edge_text = (
            "**Edge score 9.5/10 — Maximum alpha window.** This event is NOT yet in financial "
            "media. ECMWF is detecting it 7–10 days before news coverage typically emerges. "
            "This is the core premise of weather-driven trading — systematic early detection "
            "of supply/demand shocks before they appear in sell-side research."
        )
    else:
        edge_text = (
            "**Edge score 7.0/10 — Assumed pre-media.** Media validation status is unconfirmed. "
            "Assume moderate information advantage. Before sizing a position, quickly check "
            "Bloomberg, Reuters, and Refinitiv for any recent coverage of this weather event."
        )

    # ── Pheno narrative ────────────────────────────────────────────────────────
    pheno_key = (top_commodity.lower().strip(), norm_anomaly_key)
    pheno_text = PHENO_NARRATIVE.get(pheno_key, "")

    # ── Trade thesis ───────────────────────────────────────────────────────────
    long_thesis  = TRADE_THESIS.get((norm_anomaly_key, "Long"), "")
    short_thesis = TRADE_THESIS.get((norm_anomaly_key, "Short"), "")

    # ── Invalidation ──────────────────────────────────────────────────────────
    invalidation = INVALIDATION_CONDITIONS.get(norm_anomaly_key, "")

    # ── RENDER ─────────────────────────────────────────────────────────────────
    # 1. Score decomposition table
    st.markdown("#### 📊 Score Decomposition")
    decomp_html = build_score_decomposition_html(
        weather_strength=weather_strength,
        mapping_quality=mq,
        seasonality_score=seasonality_score,
        trend_factor=trend_factor,
        edge_score=edge_score,
        execution_quality=eq,
        conf_bonus=conf_bonus,
        top_pheno_mult=top_pheno_mult,
        event_score=event_score,
        accent=accent,
    )
    st.markdown(decomp_html, unsafe_allow_html=True)

    # 2. Z-score
    st.markdown("#### 🔬 Weather Anomaly Intensity")
    freq_label = _sigma_frequency_label(event_sigma)
    st.markdown(
        f"**{event_sigma:.2f}σ** — {freq_label}.  \n"
        f"The Z-score measures how extreme this event is relative to the climatological mean for "
        f"this region and time of year. Institutional weather desks typically require **≥1.5σ** before "
        f"entering a weather-driven trade; **≥2.0σ** events are where the largest market "
        f"mispricings historically occur because consensus forecasters treat them as outliers."
    )

    # 3. Seasonal context
    st.markdown("#### 📅 Seasonal Context")
    st.markdown(season_ctx)

    # 4. Trend
    st.markdown("#### 📈 Signal Trend")
    st.markdown(trend_narrative)

    # 5. Pheno
    if pheno_text:
        st.markdown("#### 🌱 Crop & Demand Stage")
        stage_str = f" — **{top_pheno_stage}** ({top_pheno_mult:.1f}×)" if top_pheno_stage else f" ({top_pheno_mult:.1f}×)"
        st.markdown(f"**{top_commodity.title()}{stage_str}**  \n{pheno_text}")

    # 6. Confluence
    st.markdown("#### 🌐 Regional Confluence")
    st.markdown(confluence_text)

    # 7. Edge
    st.markdown("#### ⚡ Information Edge")
    st.markdown(edge_text)
    # Article link — shown when media has confirmed this event
    if media_article_url:
        label = media_headline[:100] if media_headline else media_source or "Source"
        timing = f" · {media_pickup_ago}" if media_pickup_ago else ""
        st.markdown(
            f"📰 **Media source{timing}:** [{label}]({media_article_url})",
        )
    elif media_source and not media_article_url:
        timing = f" · {media_pickup_ago}" if media_pickup_ago else ""
        st.caption(f"📰 Confirmed by {media_source}{timing} (no direct link stored)")

    # 8. Trade thesis
    if long_thesis or short_thesis:
        st.markdown("#### 💼 Trade Thesis")
        if long_thesis:
            st.markdown(f"**🟢 Long:**  \n{long_thesis}")
        if short_thesis:
            st.markdown(f"**🔴 Short:**  \n{short_thesis}")

    # 9. Invalidation checklist
    if invalidation:
        st.markdown("#### ⚠️ What Would Invalidate This Signal")
        st.markdown(invalidation)

    # 10. Raw evidence (compact)
    with st.expander("🔢 Raw weather measurements & data"):
        ev_cols = st.columns(2)
        ev_cols[0].markdown(f"**Vehicle:** {get_vehicle(best_row)}")
        ev_cols[0].markdown(f"**Commodities:** {', '.join(commodities)}")
        ev_cols[0].markdown(
            f"**Forecast window:**  \n"
            f"{format_dt(best_row.get('forecast_start'))} → {format_dt(best_row.get('forecast_end'))}"
        )
        ev_cols[1].markdown("**Trigger evidence:**")
        for item in build_trigger_evidence(best_row):
            ev_cols[1].caption(f"— {item}")
        details = parse_jsonish(best_row.get("details"))
        if isinstance(details, dict) and details:
            st.json(details)


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
    media_val         = best_row.get("media_validated")
    media_headline_val = best_row.get("media_headline", "") or ""
    media_source_val   = best_row.get("media_source", "") or ""
    media_article_url  = best_row.get("media_article_url", "") or ""
    media_pickup_at    = best_row.get("media_pickup_at")
    region_raw  = normalize_text(best_row.get("region", ""), "").strip()
    anomaly_key_raw = anomaly_raw.strip()

    # Weather-level scoring (shared by all stocks in this event)
    weather_strength  = compute_weather_strength(best_row)
    seasonality_score = compute_seasonality_score(best_row)
    trend_factor      = compute_trend_factor(best_row)
    edge_score        = compute_edge_score(best_row)
    anomaly_key       = normalize_anomaly_key(anomaly_raw)
    conf_bonus        = compute_confluence_bonus(all_df, anomaly_key)
    # Z-score for display badge on card header
    event_sigma       = compute_anomaly_zscore(best_row)

    # Collect affected stocks — respecting cross-event ownership filter
    stock_list = []
    seen_symbols: set = set()
    for _, row in event_rows.iterrows():
        trade, symbols = get_stock_trade_symbols(row)
        if not symbols or trade == "No Trade":
            continue
        commodity_label = normalize_text(row.get("commodity", ""), "")
        # Per-commodity phenological multiplier — adjusts score by crop stage sensitivity
        pheno_mult, pheno_stage = compute_phenological_multiplier(row)
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
                pheno_multiplier=pheno_mult,
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
                "pheno_stage": pheno_stage,
                "pheno_mult": pheno_mult,
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

    # ── Media pickup time-ago label ────────────────────────────────────────────
    media_pickup_ago = ""
    if media_val is True and media_pickup_at is not None:
        try:
            from datetime import timezone as _tz
            if hasattr(media_pickup_at, "tzinfo") and media_pickup_at.tzinfo:
                delta = datetime.now(_tz.utc) - media_pickup_at
            else:
                delta = datetime.utcnow() - media_pickup_at
            hours = int(delta.total_seconds() / 3600)
            if hours < 2:
                media_pickup_ago = "just now"
            elif hours < 24:
                media_pickup_ago = f"{hours}h ago"
            else:
                media_pickup_ago = f"{hours // 24}d ago"
        except Exception:
            media_pickup_ago = ""
    commodities_str = "  ·  ".join(commodities)

    # Sigma badge — colour ramps from grey (1σ) to amber (2σ) to red (3σ)
    if event_sigma >= 2.5:
        sigma_color = "#E74C3C"   # red — extreme
    elif event_sigma >= 1.8:
        sigma_color = "#F39C12"   # amber — significant
    elif event_sigma >= 1.2:
        sigma_color = "#5DADE2"   # blue — notable
    else:
        sigma_color = "#555"      # grey — marginal
    sigma_badge = (
        f'<span style="font-size:9px;font-weight:700;color:{sigma_color};'
        f'border:1px solid {sigma_color};border-radius:3px;padding:1px 4px;'
        f'margin-left:6px;letter-spacing:0.5px;">{event_sigma:.1f}σ</span>'
    )

    # Build individual stock rows
    stock_rows_html = ""
    for s in stock_list[:10]:
        d_color  = "#2ECC71" if s["direction"] == "Long" else "#E74C3C"
        d_arrow  = "▲" if s["direction"] == "Long" else "▼"
        # Cooldown indicator: greyed out with 🔁 when same (sym+region+anomaly) was logged <24h ago
        if s.get("cooldown"):
            sym_color   = "#555"
            score_color = "#444"
            cd_badge    = '<span style="font-size:9px;color:#444;margin-left:4px;">🔁</span>'
        else:
            sym_color   = "#f0f0f0"
            score_color = d_color
            cd_badge    = ""
        # Phenological stage badge: show when multiplier is materially high or low
        pm = s.get("pheno_mult", 1.0)
        ps = s.get("pheno_stage", "")
        if ps and pm >= 1.6:
            pheno_badge = (
                f'<span style="font-size:8px;font-weight:700;color:#F39C12;'
                f'border:1px solid #F39C12;border-radius:2px;padding:0px 3px;'
                f'margin-left:4px;white-space:nowrap;">🌱 {ps.upper()}</span>'
            )
        elif ps and pm <= 0.5:
            pheno_badge = (
                f'<span style="font-size:8px;color:#444;'
                f'border:1px solid #333;border-radius:2px;padding:0px 3px;'
                f'margin-left:4px;white-space:nowrap;">💤 {ps.upper()}</span>'
            )
        else:
            pheno_badge = ""
        stock_rows_html += (
            f'<div style="display:flex;align-items:center;gap:8px;padding:3px 0;'
            f'border-bottom:1px solid rgba(255,255,255,0.06);">'
            f'<span style="color:{d_color};font-size:10px;font-weight:900;width:12px;flex-shrink:0;">{d_arrow}</span>'
            f'<span style="font-family:monospace;font-size:12px;font-weight:700;color:{sym_color};width:48px;flex-shrink:0;">{s["symbol"]}</span>'
            f'<span style="font-size:11px;font-weight:700;color:{score_color};width:28px;flex-shrink:0;">{s["score"]:.1f}</span>'
            f'<span style="font-size:10px;color:#777;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;">{s["role"]}</span>'
            f'{pheno_badge}{cd_badge}'
            f'</div>'
        )

    card_html = (
        # Explicit dark background so text colours are always correct regardless of Streamlit theme
        f'<div style="border-left:3px solid {accent};border-radius:6px;'
        f'background:#16181D;padding:16px 18px 14px 18px;'
        f'margin-bottom:2px;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;">'

        f'<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;">'
        f'<span style="font-size:10px;font-weight:700;color:#777;letter-spacing:1px;text-transform:uppercase;">'
        f'{rank_str}&nbsp;&nbsp;{bucket}{media_str}</span>'
        f'<div style="text-align:right;line-height:1;">'
        f'<span style="font-size:26px;font-weight:700;color:{accent};">{event_score:.1f}</span>'
        f'<span style="font-size:11px;color:#555;">&thinsp;/10</span>'
        f'{sigma_badge}'
        f'</div></div>'

        f'<div style="font-size:17px;font-weight:700;color:#E8E8E8;margin-bottom:2px;">{region}</div>'
        f'<div style="font-size:13px;font-weight:700;color:{accent};letter-spacing:0.3px;margin-bottom:4px;">{anomaly}</div>'
        f'<div style="font-size:11px;color:#666;margin-bottom:10px;">'
        f'<span style="color:#888;">{trend_label}</span>&nbsp;&nbsp;·&nbsp;&nbsp;{commodities_str}</div>'

        f'<div style="font-size:12px;color:#888;line-height:1.7;margin-bottom:12px;">{why}</div>'

        f'<div style="background:#2a2a2a;border-radius:3px;height:2px;margin-bottom:12px;">'
        f'<div style="background:{accent};width:{score_pct}%;height:2px;border-radius:3px;"></div></div>'

        f'<div style="font-size:9px;font-weight:700;color:#555;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:6px;">AFFECTED MARKETS</div>'
        + stock_rows_html
        + (
            # ── EXIT SIGNAL banner — shown when media confirms the event ──────
            f'<div style="margin-top:12px;padding:10px 12px;'
            f'background:rgba(124,45,18,0.35);border-left:3px solid #f97316;border-radius:4px;">'
            f'<div style="font-size:11px;font-weight:700;color:#fed7aa;letter-spacing:0.5px;margin-bottom:3px;">'
            f'📰 MEDIA CONFIRMED — EXIT WINDOW OPEN</div>'
            + (f'<div style="font-size:10px;color:#fdba74;margin-bottom:4px;">{media_headline_val[:120]}</div>'
               if media_headline_val else "")
            + (f'<a href="{media_article_url}" target="_blank" '
               f'style="font-size:9px;color:#fb923c;text-decoration:none;">'
               f'{media_source_val}'
               + (f" · {media_pickup_ago}" if media_pickup_ago else "")
               + f' → Read article</a>'
               if media_article_url else
               f'<span style="font-size:9px;color:#fb923c;">'
               f'{media_source_val}'
               + (f" · {media_pickup_ago}" if media_pickup_ago else "")
               + f'</span>')
            + f'</div>'
            if media_val is True else ""
        )
        + f'</div>'
    )
    st.markdown(card_html, unsafe_allow_html=True)

    with st.expander("📖 Score breakdown & reasoning"):
        build_detailed_reasoning(
            best_row=best_row,
            event_sigma=event_sigma,
            weather_strength=weather_strength,
            seasonality_score=seasonality_score,
            trend_factor=trend_factor,
            edge_score=edge_score,
            conf_bonus=conf_bonus,
            stock_list=stock_list,
            event_score=event_score,
            region=region,
            anomaly_key=anomaly_key,
            anomaly_display=anomaly,
            commodities=commodities,
            trend_dir=trend_dir,
            rank_number=rank_number,
            bucket=bucket,
            all_df=all_df,
            accent=accent,
            media_article_url=media_article_url,
            media_headline=media_headline_val,
            media_source=media_source_val,
            media_pickup_ago=media_pickup_ago,
        )


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
        media_headline,
        media_pickup_at,
        media_article_url
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

# Media-confirmed events are in the EXIT window — hide by default
hide_confirmed = st.sidebar.checkbox(
    "🚪 Hide media-confirmed events",
    value=True,
    help="When checked, events already confirmed by media (EXIT window open) are removed "
         "from the Radar and Pulse Trader. The alpha window is closed once media picks it up.",
)

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

# ── Exclude media-confirmed events (EXIT window open — alpha gone) ─────────
# Region families: subregions and parent regions share the same weather event.
# If "Brazil Center-South / heavy_rain" is confirmed, "Brazil / flood_risk"
# is the same story and must also be hidden.
_REGION_FAMILY: dict[str, frozenset] = {
    "Brazil":               frozenset({"Brazil", "Brazil Center-South", "Mato Grosso"}),
    "Brazil Center-South":  frozenset({"Brazil", "Brazil Center-South", "Mato Grosso"}),
    "Mato Grosso":          frozenset({"Brazil", "Brazil Center-South", "Mato Grosso"}),
    "US Midwest":           frozenset({"US Midwest", "US Southern Plains"}),
    "US Southern Plains":   frozenset({"US Midwest", "US Southern Plains"}),
    "Europe Gas Belt":      frozenset({"Europe Gas Belt", "Southern Europe", "North Sea"}),
    "Southern Europe":      frozenset({"Europe Gas Belt", "Southern Europe", "North Sea"}),
    "North Sea":            frozenset({"Europe Gas Belt", "Southern Europe", "North Sea"}),
    "Australia East":       frozenset({"Australia East", "Western Australia"}),
    "Western Australia":    frozenset({"Australia East", "Western Australia"}),
    "Black Sea":            frozenset({"Black Sea", "Canadian Prairies"}),
}

_n_confirmed_hidden = 0
if hide_confirmed and "media_validated" in df.columns:
    # Collect ALL confirmed regions from full df (not just filtered)
    _confirmed_raw = set(
        df.loc[df["media_validated"] == True, "region"].dropna().astype(str).unique()
    )
    # Expand each confirmed region to its full family
    _confirmed_regions: set[str] = set()
    for _r in _confirmed_raw:
        _confirmed_regions.add(_r)
        _confirmed_regions.update(_REGION_FAMILY.get(_r, frozenset()))

    # Hide ALL anomaly types for confirmed region families
    _confirmed_mask = filtered["region"].astype(str).isin(_confirmed_regions)
    _n_confirmed_hidden = int(filtered[_confirmed_mask]["region"].nunique())
    filtered = filtered[~_confirmed_mask].copy()

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

    if hide_confirmed and _n_confirmed_hidden > 0:
        st.info(
            f"🚪 **{_n_confirmed_hidden} region(s) hidden** — media confirmed, EXIT window open. "
            f"Expand below to see them, or uncheck '🚪 Hide media-confirmed events' in the sidebar.",
            icon=None,
        )
        # ── Expandable list of hidden confirmed events ─────────────────────────
        with st.expander(f"🔍 View {_n_confirmed_hidden} hidden media-confirmed event(s)"):
            _confirmed_rows = df[
                df["region"].astype(str).isin(_confirmed_regions)
            ].drop_duplicates(subset=["region", "anomaly_type"]).copy()

            if not _confirmed_rows.empty:
                _show_cols = ["region", "anomaly_type", "commodity", "trade_bias",
                              "media_headline", "media_source", "media_pickup_at"]
                _avail = [c for c in _show_cols if c in _confirmed_rows.columns]

                # Format pickup time — guard against NaT and None
                if "media_pickup_at" in _confirmed_rows.columns:
                    def _fmt_hidden_ts(ts):
                        if ts is None:
                            return ""
                        try:
                            if pd.isna(ts):
                                return ""
                        except Exception:
                            pass
                        try:
                            return pd.Timestamp(ts).strftime("%b %d %Y %H:%M UTC")
                        except Exception:
                            return str(ts)[:16]
                    _confirmed_rows["media_pickup_at"] = _confirmed_rows["media_pickup_at"].apply(_fmt_hidden_ts)
                st.dataframe(_confirmed_rows[_avail], use_container_width=True)
                st.caption(
                    "These events have been picked up by media — the trade thesis is now public. "
                    "Consider these as EXIT signals for any open positions."
                )
            else:
                st.write("No confirmed event details available.")

    st.divider()

    # ── Weather-event card grid ────────────────────────────────────────────────
    if filtered.empty:
        st.info("No signals match the current filters.")
    else:
        # Per-row preview score (used for sorting — mirrors the card's top-stock score)
        def _preview_score(row):
            trade, symbols = get_stock_trade_symbols(row)
            if not symbols:
                return 0.0
            ws  = compute_weather_strength(row)
            ss  = compute_seasonality_score(row)
            tf  = compute_trend_factor(row)
            es  = compute_edge_score(row)
            pheno_mult, _ = compute_phenological_multiplier(row)
            anomaly_key = normalize_anomaly_key(str(row.get("anomaly_type", "")))
            cb  = compute_confluence_bonus(filtered, anomaly_key)
            # Evaluate all symbols (cap at 5) and return the best — same as card logic
            best = 0.0
            for sym in symbols[:5]:
                mq = compute_mapping_quality(row, sym, trade)
                eq = compute_execution_quality(row, sym)
                s  = compute_final_trade_score(ws, mq, 10.0, eq, ss, tf,
                                               confluence_bonus=cb, edge_score=es,
                                               pheno_multiplier=pheno_mult)
                if s > best:
                    best = s
            return best

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

        # ── Re-sort by effective displayed score (owned symbols only) ──────────
        # After ownership assignment, an event's best symbol may have been
        # claimed by a higher-scoring event. The card displays only owned symbols,
        # so the sort order must reflect that — otherwise card #1 can show a
        # lower score than card #2.
        def _owned_event_score(grp: "pd.DataFrame", owned: set) -> float:
            if not owned:
                return 0.0
            # Mirror show_weather_event_card exactly:
            # shared weather-level metrics come from best_row (highest signal_level),
            # while stock-level metrics (pheno_mult, mapping_quality, exec_quality)
            # are computed per commodity row.
            best_row_local = grp.sort_values("signal_level", ascending=False).iloc[0]
            _ws  = compute_weather_strength(best_row_local)
            _ss  = compute_seasonality_score(best_row_local)
            _tf  = compute_trend_factor(best_row_local)
            _es  = compute_edge_score(best_row_local)
            _ak  = normalize_anomaly_key(str(best_row_local.get("anomaly_type", "")))
            _cb  = compute_confluence_bonus(filtered, _ak)
            best = 0.0
            for _, _row in grp.iterrows():
                _trade, _syms = get_stock_trade_symbols(_row)
                _pm, _ = compute_phenological_multiplier(_row)
                for _sym in _syms[:5]:
                    if _sym not in owned:
                        continue
                    _mq = compute_mapping_quality(_row, _sym, _trade)
                    _eq = compute_execution_quality(_row, _sym)
                    _s  = compute_final_trade_score(_ws, _mq, 10.0, _eq, _ss, _tf,
                                                    confluence_bonus=_cb, edge_score=_es,
                                                    pheno_multiplier=_pm)
                    if _s > best:
                        best = _s
            return best

        event_groups = [
            (_owned_event_score(grp, event_owned_symbols[ei]), grp)
            for ei, (_, grp) in enumerate(event_groups)
        ]
        event_groups.sort(key=lambda x: x[0], reverse=True)
        # Re-assign owned symbols after re-sort (indices changed)
        symbol_event_owner2: dict[str, int] = {}
        for ei2, (es2, grp2) in enumerate(event_groups):
            for _, _row2 in grp2.iterrows():
                _, syms2 = get_stock_trade_symbols(_row2)
                for _sym2 in syms2:
                    if _sym2 not in symbol_event_owner2 or es2 > event_groups[symbol_event_owner2[_sym2]][0]:
                        symbol_event_owner2[_sym2] = ei2
        event_owned_symbols = [set() for _ in event_groups]
        for _sym2, ei2 in symbol_event_owner2.items():
            event_owned_symbols[ei2].add(_sym2)

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

    # ── Entry Gate: classify each signal as Enter / Monitor / Avoid ───────────
    # 🟢 ENTER  — pre-media, event is new or worsening (highest alpha window)
    # 🟡 MONITOR — event is stable; worth watching but alpha is narrowing
    # 🔴 AVOID  — event is recovering (fading); alpha window likely closed
    def _entry_gate(row) -> str:
        trend = str(row.get("Trend", "")).lower()
        if "worsening" in trend:
            return "🟢 Enter — Worsening"
        if "new" in trend:
            return "🟢 Enter — New Signal"
        if "stable" in trend:
            return "🟡 Monitor"
        if "recovering" in trend:
            return "🔴 Avoid — Fading"
        return "🟡 Monitor"

    if not pulse_table.empty:
        pulse_table.insert(0, "Entry Gate", pulse_table.apply(_entry_gate, axis=1))

    _pulse_cols = [
        "Entry Gate", "Date", "Stock Trade", "Trade",
        "Why It Matters", "Final Trade Score",
    ]

    if pulse_table.empty:
        st.write("No recommendations right now.")
    else:
        display_pulse = pulse_table[
            [c for c in _pulse_cols if c in pulse_table.columns]
        ].copy()
        st.dataframe(display_pulse, use_container_width=True, height=500)

        st.caption(
            "**Entry Gate:** "
            "🟢 Enter = event actively growing, pre-media — highest alpha window.  "
            "🟡 Monitor = stable signal, alpha narrowing — size down or wait.  "
            "🔴 Avoid = event fading in GRIB data — thesis deteriorating, skip or exit."
        )

        with st.expander(f"Full detail ({len(pulse_table)} rows)"):
            st.dataframe(pulse_table, use_container_width=True)

with tab_media:
    st.header("📡 Media Signal Validation")
    st.caption(
        "When media picks up a weather event it's the **EXIT signal** — "
        "the trade window is closing. Confirmations are saved to DB and shown as 🟠 EXIT banners on Radar cards."
    )

    from media_validator import MediaValidator, write_validation_to_db
    import psycopg as _psycopg

    validator = MediaValidator()
    newsapi_configured = bool(os.environ.get("NEWSAPI_KEY"))
    _db_url = os.environ.get("DATABASE_URL", "")

    # ── Status bar ────────────────────────────────────────────────────────────
    status_cols = st.columns(5)
    status_cols[0].success("✅ NOAA/NWS")
    status_cols[1].success("✅ Google News")
    status_cols[2].success("✅ GDELT")
    status_cols[3].success("✅ Bing News")
    if newsapi_configured:
        status_cols[4].success("✅ NewsAPI")
    else:
        status_cols[4].warning("⚠️ NewsAPI (optional)")

    st.divider()

    # ── Buttons ───────────────────────────────────────────────────────────────
    top_signals = filtered_ranked[filtered_ranked["Conviction"].isin({"PRIME", "ACTIONABLE"})].copy()

    col_run, col_monitor, col_info = st.columns([1, 1, 4])
    run_clicked     = col_run.button("🔍 Validate Top 20", type="primary",
                                     help="Check PRIME/ACTIONABLE signals and save confirmations to DB")
    monitor_clicked = col_monitor.button("🔄 Run Full Monitor",
                                         help="Run the same scan as the 4x-daily cron job (all events, last 14 days)")
    col_info.caption(
        f"**{min(len(top_signals), 20)}** PRIME/ACTIONABLE weather events · "
        f"sources: NOAA/NWS, Google News, GDELT, Bing News"
        + (", NewsAPI" if newsapi_configured else "")
        + " · **Confirmations saved to DB** → EXIT banners appear on Radar cards"
    )

    # ── Full monitor (same as cron job) ───────────────────────────────────────
    if monitor_clicked:
        from media_validator import run_scheduled_monitor as _run_monitor
        with st.spinner("Running full media monitor (all active events, last 14 days)…"):
            import io, sys as _sys
            buf = io.StringIO()
            old_stdout = _sys.stdout
            _sys.stdout = buf
            try:
                _run_monitor()
            finally:
                _sys.stdout = old_stdout
            output = buf.getvalue()
        st.success("✅ Full monitor complete — Radar cards will show EXIT banners for confirmed events")
        with st.expander("Monitor log"):
            st.code(output)
        st.rerun()

    # ── Validate Top 20 ───────────────────────────────────────────────────────
    if run_clicked:
        signal_rows = []
        for _, ranked_row in top_signals.head(20).iterrows():
            match = df[
                (df["region"].astype(str) == str(ranked_row.get("Region", ""))) &
                (df["anomaly_type"].astype(str) == normalize_anomaly_key(
                    str(ranked_row.get("Anomaly", "")).lower().replace(" ", "_")))
            ]
            if not match.empty:
                db_row = match.iloc[0]
                signal_rows.append({
                    "region":      str(db_row.get("region", "")),
                    "anomaly_type": str(db_row.get("anomaly_type", "")),
                    "commodity":   str(db_row.get("commodity", "")),
                    "conviction":  ranked_row.get("Conviction", ""),
                    "final_score": ranked_row.get("Final Trade Score", 0),
                    "trade":       ranked_row.get("Trade", ""),
                })

        if not signal_rows:
            st.info("No PRIME/ACTIONABLE signals found to validate.")
        else:
            live_results = []
            saved_count  = 0
            progress = st.progress(0, text="Validating signals…")
            for i, sig in enumerate(signal_rows):
                summary = validator.validate_signal(
                    signal_id=None,
                    region=sig["region"],
                    anomaly=sig["anomaly_type"],
                    commodity=sig["commodity"],
                )
                if summary.is_confirmed and summary.best_result:
                    br = summary.best_result
                    live_results.append({
                        "Region":      sig["region"],
                        "Anomaly":     sig["anomaly_type"].replace("_", " ").title(),
                        "Commodity":   sig["commodity"],
                        "Trade":       sig["trade"],
                        "Score":       sig["final_score"],
                        "Conviction":  sig["conviction"],
                        "Source":      br.source,
                        "Headline":    br.headline,
                        "Match Score": round(br.score, 1),
                        "URL":         br.url,
                    })
                    # ── Persist to DB so EXIT banner shows on Radar card ──────
                    if _db_url:
                        try:
                            with _psycopg.connect(_db_url) as _conn:
                                write_validation_to_db(_conn, None, summary)
                            saved_count += 1
                        except Exception:
                            pass
                progress.progress(
                    (i + 1) / len(signal_rows),
                    text=f"Checked {i+1}/{len(signal_rows)}: {sig['region']} / {sig['anomaly_type']}"
                )

            progress.empty()

            if live_results:
                st.success(
                    f"✅ {len(live_results)} confirmed · "
                    f"{saved_count} saved to DB · Radar cards will show EXIT banners after reload"
                )
                live_df = pd.DataFrame(live_results).sort_values("Score", ascending=False)
                st.dataframe(live_df.drop(columns=["URL"]), use_container_width=True)
                st.caption("Article links:")
                for r in live_results:
                    if r["URL"]:
                        st.markdown(f"- [{r['Headline'][:100]}]({r['URL']}) — *{r['Source']}*")
            else:
                st.info("No external confirmation found. NOAA may have no active alerts in these regions right now.")

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

        # Format media_pickup_at as a readable date string
        if "media_pickup_at" in media_df.columns:
            def _fmt_pickup(ts):
                if ts is None or (hasattr(ts, "__class__") and ts.__class__.__name__ == "NaTType"):
                    return ""
                try:
                    import datetime as _dt
                    if hasattr(ts, "tzinfo"):
                        ts = ts.replace(tzinfo=None)
                    return ts.strftime("%b %d %Y, %H:%M UTC") if hasattr(ts, "strftime") else str(ts)[:16]
                except Exception:
                    return str(ts)[:16]
            media_df["Picked Up"] = media_df["media_pickup_at"].apply(_fmt_pickup)

        cols = ["region", "commodity", "anomaly_type", "signal_level", "trade_bias",
                "Picked Up", "media_headline", "media_source"]
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
        STOP_LOSS_PCT, TAKE_PROFIT_PCT,
        close_positions_stop_loss,
    )

    # Ensure DB table exists
    try:
        ensure_schema()
    except Exception as e:
        st.error(f"DB schema error: {e}")
        st.stop()

    # ── Position management settings ─────────────────────────────────────────
    with st.expander("⚙️ Position Management Settings", expanded=False):
        pm_col1, pm_col2 = st.columns(2)
        _sl_pct = pm_col1.slider(
            "🛑 Stop-Loss %", min_value=1.0, max_value=20.0,
            value=float(STOP_LOSS_PCT), step=0.5,
            help="Close position if it moves this % against you",
        )
        _tp_pct = pm_col2.slider(
            "🎯 Take-Profit %", min_value=2.0, max_value=50.0,
            value=float(TAKE_PROFIT_PCT), step=0.5,
            help="Close position if it gains this % in your favour",
        )
        if st.button("🔄 Apply & Scan Now", help="Run stop-loss/take-profit check immediately with these thresholds"):
            with st.spinner("Scanning open positions…"):
                try:
                    _n_closed = close_positions_stop_loss(
                        stop_loss_pct=_sl_pct,
                        take_profit_pct=_tp_pct,
                    )
                    if _n_closed:
                        st.success(f"✅ Closed {_n_closed} position(s) via stop-loss/take-profit.")
                        # Invalidate cached aftermath so it reloads
                        st.session_state["aftermath_df"] = None
                    else:
                        st.info("No open positions hit the thresholds.")
                except Exception as _e:
                    st.error(f"Error scanning: {_e}")

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
            f"(entry price via Finnhub; T+3/T+5 outcomes auto-evaluated after 3–5 business days)"
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
            with st.spinner("Fetching T+3/T+5 snapshots + live prices…"):
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
            m1, m2, m3, m4, m5, m6 = st.columns(6)
            m1.metric("Total Logged",   perf["total"])
            m2.metric("🚪 Closed",      perf.get("closed_count", 0),
                      help="Positions auto-closed when media confirmed the weather event")
            m3.metric("Win Rate",       f"{perf['win_rate']}%",
                      help="Based on best available P&L (Exit > T+10 > T+7 > T+5 > T+3 > Day 0)")
            m4.metric("Avg P&L",        f"{perf['avg_pnl']:+.2f}%")
            m5.metric("Best Trade",     perf["best_trade"])
            m6.metric("Worst Trade",    perf["worst_trade"])

            e1, e2, e3, e4, e5 = st.columns(5)
            e1.metric("🚪 Exit Evaluated", perf.get("exit_evaluated", 0),
                      help="Positions with actual exit price at media confirmation date")
            e2.metric("T+3 Evaluated",  perf.get("t3_evaluated",  0),
                      help="Trades with a 3-business-day closing price snapshot")
            e3.metric("T+5 Evaluated",  perf.get("t5_evaluated",  0),
                      help="Trades with a 5-business-day closing price snapshot")
            e4.metric("T+7 Evaluated",  perf.get("t7_evaluated",  0),
                      help="Trades with a 7-business-day closing price snapshot")
            e5.metric("T+10 Evaluated", perf.get("t10_evaluated", 0),
                      help="Trades with a 10-business-day closing price snapshot (~2 weeks)")

        st.caption(
            "📅 **Exit P&L** = actual close at media confirmation date (highest priority). "
            "T+3/T+5/T+7/T+10 = theoretical horizons. "
            "🚪 Closed positions were auto-exited when media confirmed the weather event."
        )

        st.divider()

        # ── Colour-coded P&L table ────────────────────────────────────────────
        display_cols = [
            "Date Logged", "Status", "Stock", "Trade", "Entry",
            "Exit P&L", "Exit α SPY",        # actual close at media pickup
            "Day 0 P&L",
            "T+3 P&L",  "T+3 α SPY",
            "T+5 P&L",  "T+5 α SPY",
            "T+7 P&L",  "T+7 α SPY",
            "T+10 P&L", "T+10 α SPY",
            "Outcome", "Score", "Conviction", "Region", "Anomaly",
        ]

        visible = aftermath_df[[c for c in display_cols if c in aftermath_df.columns]].copy()

        # Filter controls
        fc1, fc2, fc3, fc4 = st.columns(4)
        trade_filter      = fc1.multiselect("Trade", ["Long", "Short"], default=["Long", "Short"])
        all_outcomes      = ["✅ Win", "❌ Loss", "➖ Flat", "⏳ Pending", "—"]
        outcome_filter    = fc2.multiselect("Outcome", all_outcomes, default=all_outcomes)
        conviction_vals   = sorted(aftermath_df["Conviction"].dropna().unique().tolist())
        conviction_filter = fc3.multiselect("Conviction", conviction_vals, default=conviction_vals)
        all_statuses      = sorted(aftermath_df["Status"].dropna().unique().tolist()) if "Status" in aftermath_df.columns else []
        status_filter     = fc4.multiselect("Status", all_statuses, default=all_statuses)

        visible = visible[
            visible["Trade"].isin(trade_filter) &
            visible["Outcome"].isin(outcome_filter) &
            visible["Conviction"].isin(conviction_filter) &
            (visible["Status"].isin(status_filter) if "Status" in visible.columns and status_filter else True)
        ]

        st.dataframe(visible, use_container_width=True, height=500)

        # ── Why column in expander ────────────────────────────────────────────
        with st.expander("Show 'Why It Mattered' for all recommendations"):
            why_cols = ["Date Logged", "Stock", "Trade", "T+3 P&L", "T+5 P&L", "T+7 P&L", "T+10 P&L", "Outcome", "Why"]
            st.dataframe(
                aftermath_df[[c for c in why_cols if c in aftermath_df.columns]],
                use_container_width=True,
            )

        # ── ML Scorer ─────────────────────────────────────────────────────────
        st.markdown("---")
        st.subheader("🤖 ML Trade Scorer")
        st.caption(
            "Trains an XGBoost model on **confirmed T+3 outcomes only** (never same-day noise). "
            "Features: region, anomaly, conviction, Z-score (σ), seasonality surprise, "
            "confluence bonus, phenological multiplier, trend direction. "
            "Stored in PostgreSQL — survives deploys. Improves as your track record grows."
        )

        try:
            from ml_scorer import (
                get_labeled_data, train_model, load_model,
                predict_win_prob, model_info, MIN_SAMPLES,
            )

            labeled_df = get_labeled_data(aftermath_df)
            n_labeled  = len(labeled_df)
            info       = model_info()

            ml_c1, ml_c2, ml_c3, ml_c4, ml_c5 = st.columns(5)
            ml_c1.metric("T+3 Labeled",    n_labeled,
                         help="Trades with confirmed 3-day outcome — the training set")
            ml_c2.metric("Needed to Train", MIN_SAMPLES)
            ml_c3.metric("Model",   "✅ Ready" if info["trained"] else "⏳ Not trained")
            ml_c4.metric("Storage", "🗄️ DB" if os.environ.get("DATABASE_URL") else "📁 Local")
            if info["trained"]:
                ml_c5.metric("Trained on", f"{info['n_trained']} trades")

            if n_labeled < MIN_SAMPLES:
                st.info(
                    f"Need **{MIN_SAMPLES - n_labeled} more** trades with confirmed T+3 outcomes. "
                    "Log recommendations daily — after 3 business days each trade automatically "
                    "gets its T+3 snapshot via Yahoo Finance historical data."
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
