import glob
import json
import math
import os
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import psycopg
import xarray as xr

DATABASE_URL = os.environ["DATABASE_URL"]

GRIB_GLOB = os.environ.get("GRIB_GLOB", "/opt/render/project/src/**/*.grib2")
FORECAST_HOURS_LIMIT = int(os.environ.get("FORECAST_HOURS_LIMIT", "168"))  # 7 days

REGIONS = [
    {
        "name": "US Midwest",
        "lat_min": 36.0,
        "lat_max": 46.0,
        "lon_min": -104.0,
        "lon_max": -82.0,
        "commodities": ["Corn", "Soybeans"],
    },
    {
        "name": "US Southern Plains",
        "lat_min": 30.0,
        "lat_max": 39.0,
        "lon_min": -104.0,
        "lon_max": -94.0,
        "commodities": ["Wheat"],
    },
    {
        "name": "Brazil Center-South",
        "lat_min": -25.0,
        "lat_max": -10.0,
        "lon_min": -60.0,
        "lon_max": -40.0,
        "commodities": ["Soybeans", "Coffee", "Sugar"],
    },
    {
        "name": "Argentina Pampas",
        "lat_min": -40.0,
        "lat_max": -28.0,
        "lon_min": -67.0,
        "lon_max": -56.0,
        "commodities": ["Corn", "Soybeans", "Wheat"],
    },
    {
        "name": "Europe Gas Belt",
        "lat_min": 45.0,
        "lat_max": 56.0,
        "lon_min": -5.0,
        "lon_max": 20.0,
        "commodities": ["Natural Gas", "Power Utilities"],
    },
    {
        "name": "Black Sea",
        "lat_min": 43.0,
        "lat_max": 50.0,
        "lon_min": 27.0,
        "lon_max": 42.0,
        "commodities": ["Wheat"],
    },
    {
        "name": "India",
        "lat_min": 8.0,
        "lat_max": 30.0,
        "lon_min": 68.0,
        "lon_max": 89.0,
        "commodities": ["Sugar", "Rice"],
    },
    {
        "name": "Australia East",
        "lat_min": -38.0,
        "lat_max": -20.0,
        "lon_min": 142.0,
        "lon_max": 154.0,
        "commodities": ["Coal", "Wheat"],
    },
]

MARKET_SENSITIVITY = {
    "Corn": 4,
    "Soybeans": 4,
    "Wheat": 4,
    "Coffee": 5,
    "Sugar": 4,
    "Natural Gas": 5,
    "Power Utilities": 4,
    "Rice": 3,
    "Coal": 3,
}

RULES = {
    "heatwave": {
        "temp_c_max": 35.0,
        "severity_step_c": 2.0,
        "base_score": 3,
        "bullish_for": {"Corn", "Soybeans", "Wheat", "Coffee", "Sugar", "Natural Gas", "Power Utilities"},
    },
    "extreme_heat": {
        "temp_c_max": 40.0,
        "severity_step_c": 2.0,
        "base_score": 4,
        "bullish_for": {"Corn", "Soybeans", "Wheat", "Coffee", "Sugar", "Natural Gas", "Power Utilities"},
    },
    "frost": {
        "temp_c_min": 0.0,
        "severity_step_c": 2.0,
        "base_score": 4,
        "bullish_for": {"Coffee", "Sugar", "Wheat"},
    },
    "heavy_rain": {
        "precip_mm_7d": 100.0,
        "severity_step_mm": 25.0,
        "base_score": 3,
        "bullish_for": {"Natural Gas", "Power Utilities"},
        "bearish_for": {"Wheat", "Corn", "Soybeans", "Coffee", "Sugar"},
    },
    "drought": {
        "precip_mm_7d_max": 10.0,
        "temp_c_mean_min": 28.0,
        "severity_step_mm": 5.0,
        "base_score": 4,
        "bullish_for": {"Corn", "Soybeans", "Wheat", "Coffee", "Sugar"},
    },
    "storm_wind": {
        "wind_ms_max": 18.0,
        "severity_step_ms": 3.0,
        "base_score": 3,
        "bullish_for": {"Natural Gas", "Power Utilities", "Coal"},
    },
}

ASSET_MAP = {
    "Corn": {
        "best_vehicle": "Corn futures / CORN ETF",
        "proxy_equities": ["ADM", "BG", "CF", "MOS", "CTVA", "DE", "UNP"],
        "secondary_exposures": ["ethanol producers", "grain handlers", "rail logistics", "crop insurers", "farm equipment"],
    },
    "Soybeans": {
        "best_vehicle": "Soybean futures / SOYB ETF",
        "proxy_equities": ["ADM", "BG", "CF", "MOS", "CTVA", "DE"],
        "secondary_exposures": ["soy processors", "export terminals", "fertilizer names", "farm equipment"],
    },
    "Wheat": {
        "best_vehicle": "Wheat futures / WEAT ETF",
        "proxy_equities": ["ADM", "BG", "MOS", "CF", "DE"],
        "secondary_exposures": ["grain traders", "fertilizer names", "farm equipment", "food inflation proxies"],
    },
    "Coffee": {
        "best_vehicle": "Coffee futures / JO ETF",
        "proxy_equities": ["SBUX", "NSRGY"],
        "secondary_exposures": ["coffee roasters", "packaged beverage names", "soft commodities traders"],
    },
    "Sugar": {
        "best_vehicle": "Sugar futures / CANE ETF",
        "proxy_equities": ["CZZ", "TRRJF"],
        "secondary_exposures": ["ethanol-linked producers", "food input cost proxies", "soft commodities traders"],
    },
    "Natural Gas": {
        "best_vehicle": "Natural gas futures / UNG ETF",
        "proxy_equities": ["EQT", "CTRA", "RRC", "LNG"],
        "secondary_exposures": ["LNG exporters", "gas-sensitive utilities", "power generators", "industrial demand proxies"],
    },
    "Power Utilities": {
        "best_vehicle": "European utilities / power-sensitive names",
        "proxy_equities": ["NGG", "IBE.MC", "EOAN.DE", "ENGIY"],
        "secondary_exposures": ["power generators", "grid operators", "gas-sensitive industrials"],
    },
    "Rice": {
        "best_vehicle": "Rice futures / regional agri proxies",
        "proxy_equities": ["ADM", "BG"],
        "secondary_exposures": ["food staples", "Asian agri merchants", "supply-chain logistics"],
    },
    "Coal": {
        "best_vehicle": "Coal producers / coal-linked equities",
        "proxy_equities": ["BTU", "ARCH", "AMR"],
        "secondary_exposures": ["bulk shipping", "power generation", "rail freight"],
    },
}


def log(msg: str) -> None:
    print(f"[{datetime.now(UTC).isoformat()}] {msg}", flush=True)


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_global_shocks (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP,
                region TEXT,
                commodity TEXT,
                anomaly_type TEXT,
                anomaly_value DOUBLE PRECISION,
                persistence_score INTEGER,
                severity_score INTEGER,
                market_score INTEGER,
                signal_level INTEGER,
                signal_bucket TEXT,
                trade_bias TEXT,
                recommendation TEXT,
                affected_market TEXT,
                best_vehicle TEXT,
                proxy_equities TEXT,
                secondary_exposures TEXT,
                affected_assets_json JSONB,
                what_changed TEXT,
                why_it_matters TEXT,
                what_to_watch_next TEXT,
                source_file TEXT,
                forecast_start TIMESTAMP,
                forecast_end TIMESTAMP,
                details JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            );
            """
        )

        alter_statements = [
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS timestamp TIMESTAMP;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS region TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS commodity TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS anomaly_type TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS anomaly_value DOUBLE PRECISION;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS persistence_score INTEGER;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS severity_score INTEGER;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS market_score INTEGER;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS signal_level INTEGER;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS signal_bucket TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS trade_bias TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS recommendation TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS affected_market TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS best_vehicle TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS proxy_equities TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS secondary_exposures TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS affected_assets_json JSONB;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS what_changed TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS why_it_matters TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS what_to_watch_next TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS source_file TEXT;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS forecast_start TIMESTAMP;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS forecast_end TIMESTAMP;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS details JSONB;",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();",
        ]

        for ddl in alter_statements:
            cur.execute(ddl)

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_weather_global_shocks_lookup
            ON weather_global_shocks (region, commodity, anomaly_type, timestamp);
            """
        )
        conn.commit()


def find_latest_grib() -> str:
    files = sorted(glob.glob(GRIB_GLOB, recursive=True), key=os.path.getmtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No GRIB files found with pattern: {GRIB_GLOB}")
    return files[0]


def open_grib_dataset(path: str) -> xr.Dataset:
    backend_kwargs = {"indexpath": ""}
    return xr.open_dataset(path, engine="cfgrib", backend_kwargs=backend_kwargs)


def normalize_longitudes(ds: xr.Dataset) -> xr.Dataset:
    lon_name = get_coord_name(ds, ["longitude", "lon", "long"])
    if lon_name is None:
        return ds

    lon_vals = ds[lon_name].values
    if np.nanmax(lon_vals) > 180:
        new_lon = ((lon_vals + 180) % 360) - 180
        ds = ds.assign_coords({lon_name: new_lon}).sortby(lon_name)
    return ds


def get_coord_name(ds: xr.Dataset, options: list[str]) -> str | None:
    for name in options:
        if name in ds.coords:
            return name
    return None


def get_var_name(ds: xr.Dataset, options: list[str]) -> str | None:
    for name in options:
        if name in ds.data_vars:
            return name
    return None


def subset_region(da: xr.DataArray, region: dict) -> xr.DataArray:
    lat_name = get_coord_name(da.to_dataset(name="x"), ["latitude", "lat"])
    lon_name = get_coord_name(da.to_dataset(name="x"), ["longitude", "lon"])

    if lat_name is None or lon_name is None:
        raise ValueError("Could not find latitude/longitude coordinates in GRIB data")

    lat_vals = da[lat_name].values
    lat_slice = slice(region["lat_max"], region["lat_min"]) if lat_vals[0] > lat_vals[-1] else slice(region["lat_min"], region["lat_max"])
    lon_slice = slice(region["lon_min"], region["lon_max"])

    return da.sel({lat_name: lat_slice, lon_name: lon_slice})


def trim_forecast_horizon(da: xr.DataArray) -> xr.DataArray:
    time_name = None
    for candidate in ["valid_time", "time", "step"]:
        if candidate in da.coords:
            time_name = candidate
            break

    if time_name is None:
        return da

    if time_name == "step":
        max_step = np.timedelta64(FORECAST_HOURS_LIMIT, "h")
        return da.sel(step=da["step"] <= max_step)

    if time_name in ("valid_time", "time"):
        tvals = pd.to_datetime(da[time_name].values)
        if len(tvals) == 0:
            return da
        start = tvals.min()
        end = start + pd.Timedelta(hours=FORECAST_HOURS_LIMIT)
        return da.sel({time_name: slice(start, end)})

    return da


def extract_field_stats(ds: xr.Dataset, region: dict) -> dict:
    ds = normalize_longitudes(ds)

    t_name = get_var_name(ds, ["t2m", "2t"])
    tp_name = get_var_name(ds, ["tp", "total_precipitation"])
    wind_name = get_var_name(ds, ["si10", "wind10m", "ws10"])
    u10_name = get_var_name(ds, ["u10", "10u"])
    v10_name = get_var_name(ds, ["v10", "10v"])

    stats = {
        "temp_c_max": None,
        "temp_c_mean": None,
        "temp_c_min": None,
        "precip_mm_7d": None,
        "wind_ms_max": None,
        "forecast_start": None,
        "forecast_end": None,
    }

    if t_name:
        t = trim_forecast_horizon(subset_region(ds[t_name], region))
        if "valid_time" in t.coords:
            times = pd.to_datetime(t["valid_time"].values)
        elif "time" in t.coords:
            times = pd.to_datetime(t["time"].values)
        else:
            times = pd.to_datetime([datetime.now(UTC)])

        t_c = t - 273.15
        stats["temp_c_max"] = float(t_c.max(skipna=True).values)
        stats["temp_c_mean"] = float(t_c.mean(skipna=True).values)
        stats["temp_c_min"] = float(t_c.min(skipna=True).values)
        stats["forecast_start"] = pd.Timestamp(times.min()).to_pydatetime()
        stats["forecast_end"] = pd.Timestamp(times.max()).to_pydatetime()

    if tp_name:
        tp = trim_forecast_horizon(subset_region(ds[tp_name], region))
        precip_mm = tp * 1000.0
        stats["precip_mm_7d"] = float(precip_mm.sum(skipna=True).values)

        if stats["forecast_start"] is None:
            if "valid_time" in tp.coords:
                times = pd.to_datetime(tp["valid_time"].values)
            elif "time" in tp.coords:
                times = pd.to_datetime(tp["time"].values)
            else:
                times = pd.to_datetime([datetime.now(UTC)])
            stats["forecast_start"] = pd.Timestamp(times.min()).to_pydatetime()
            stats["forecast_end"] = pd.Timestamp(times.max()).to_pydatetime()

    if wind_name:
        wind = trim_forecast_horizon(subset_region(ds[wind_name], region))
        stats["wind_ms_max"] = float(wind.max(skipna=True).values)
    elif u10_name and v10_name:
        u = trim_forecast_horizon(subset_region(ds[u10_name], region))
        v = trim_forecast_horizon(subset_region(ds[v10_name], region))
        w = np.sqrt((u ** 2) + (v ** 2))
        stats["wind_ms_max"] = float(w.max(skipna=True).values)

    return stats


def severity_from_excess(excess: float, step: float, base: int) -> int:
    if excess <= 0:
        return 0
    return min(5, max(base, base + int(math.floor(excess / step))))


def classify_trade_bias(anomaly_type: str, commodity: str) -> str:
    rule = RULES[anomaly_type]
    if commodity in rule.get("bullish_for", set()):
        return "bullish"
    if commodity in rule.get("bearish_for", set()):
        return "bearish"
    return "watch"


def build_signals_from_stats(region: dict, stats: dict, source_file: str) -> list[dict]:
    signals = []

    temp_c_max = stats["temp_c_max"]
    temp_c_mean = stats["temp_c_mean"]
    temp_c_min = stats["temp_c_min"]
    precip_mm_7d = stats["precip_mm_7d"]
    wind_ms_max = stats["wind_ms_max"]

    for commodity in region["commodities"]:
        if temp_c_max is not None and temp_c_max >= RULES["heatwave"]["temp_c_max"]:
            excess = temp_c_max - RULES["heatwave"]["temp_c_max"]
            severity = severity_from_excess(excess, RULES["heatwave"]["severity_step_c"], RULES["heatwave"]["base_score"])
            signals.append(
                make_signal(
                    region=region["name"],
                    commodity=commodity,
                    anomaly_type="heatwave",
                    anomaly_value=temp_c_max,
                    severity_score=severity,
                    market_score=MARKET_SENSITIVITY.get(commodity, 3),
                    trade_bias=classify_trade_bias("heatwave", commodity),
                    forecast_start=stats["forecast_start"],
                    forecast_end=stats["forecast_end"],
                    source_file=source_file,
                    details=stats,
                )
            )

        if temp_c_max is not None and temp_c_max >= RULES["extreme_heat"]["temp_c_max"]:
            excess = temp_c_max - RULES["extreme_heat"]["temp_c_max"]
            severity = severity_from_excess(excess, RULES["extreme_heat"]["severity_step_c"], RULES["extreme_heat"]["base_score"])
            signals.append(
                make_signal(
                    region=region["name"],
                    commodity=commodity,
                    anomaly_type="extreme_heat",
                    anomaly_value=temp_c_max,
                    severity_score=severity,
                    market_score=MARKET_SENSITIVITY.get(commodity, 3),
                    trade_bias=classify_trade_bias("extreme_heat", commodity),
                    forecast_start=stats["forecast_start"],
                    forecast_end=stats["forecast_end"],
                    source_file=source_file,
                    details=stats,
                )
            )

        if temp_c_min is not None and temp_c_min <= RULES["frost"]["temp_c_min"]:
            excess = RULES["frost"]["temp_c_min"] - temp_c_min
            severity = severity_from_excess(excess, RULES["frost"]["severity_step_c"], RULES["frost"]["base_score"])
            signals.append(
                make_signal(
                    region=region["name"],
                    commodity=commodity,
                    anomaly_type="frost",
                    anomaly_value=temp_c_min,
                    severity_score=severity,
                    market_score=MARKET_SENSITIVITY.get(commodity, 3),
                    trade_bias=classify_trade_bias("frost", commodity),
                    forecast_start=stats["forecast_start"],
                    forecast_end=stats["forecast_end"],
                    source_file=source_file,
                    details=stats,
                )
            )

        if precip_mm_7d is not None and precip_mm_7d >= RULES["heavy_rain"]["precip_mm_7d"]:
            excess = precip_mm_7d - RULES["heavy_rain"]["precip_mm_7d"]
            severity = severity_from_excess(excess, RULES["heavy_rain"]["severity_step_mm"], RULES["heavy_rain"]["base_score"])
            signals.append(
                make_signal(
                    region=region["name"],
                    commodity=commodity,
                    anomaly_type="heavy_rain",
                    anomaly_value=precip_mm_7d,
                    severity_score=severity,
                    market_score=MARKET_SENSITIVITY.get(commodity, 3),
                    trade_bias=classify_trade_bias("heavy_rain", commodity),
                    forecast_start=stats["forecast_start"],
                    forecast_end=stats["forecast_end"],
                    source_file=source_file,
                    details=stats,
                )
            )

        if (
            precip_mm_7d is not None
            and temp_c_mean is not None
            and precip_mm_7d <= RULES["drought"]["precip_mm_7d_max"]
            and temp_c_mean >= RULES["drought"]["temp_c_mean_min"]
        ):
            excess = RULES["drought"]["precip_mm_7d_max"] - precip_mm_7d
            severity = severity_from_excess(excess, RULES["drought"]["severity_step_mm"], RULES["drought"]["base_score"])
            signals.append(
                make_signal(
                    region=region["name"],
                    commodity=commodity,
                    anomaly_type="drought",
                    anomaly_value=precip_mm_7d,
                    severity_score=severity,
                    market_score=MARKET_SENSITIVITY.get(commodity, 3),
                    trade_bias=classify_trade_bias("drought", commodity),
                    forecast_start=stats["forecast_start"],
                    forecast_end=stats["forecast_end"],
                    source_file=source_file,
                    details=stats,
                )
            )

        if wind_ms_max is not None and wind_ms_max >= RULES["storm_wind"]["wind_ms_max"]:
            excess = wind_ms_max - RULES["storm_wind"]["wind_ms_max"]
            severity = severity_from_excess(excess, RULES["storm_wind"]["severity_step_ms"], RULES["storm_wind"]["base_score"])
            signals.append(
                make_signal(
                    region=region["name"],
                    commodity=commodity,
                    anomaly_type="storm_wind",
                    anomaly_value=wind_ms_max,
                    severity_score=severity,
                    market_score=MARKET_SENSITIVITY.get(commodity, 3),
                    trade_bias=classify_trade_bias("storm_wind", commodity),
                    forecast_start=stats["forecast_start"],
                    forecast_end=stats["forecast_end"],
                    source_file=source_file,
                    details=stats,
                )
            )

    return signals


def score_bucket(score: int) -> str:
    if score >= 8:
        return "HIGH CONVICTION"
    if score >= 5:
        return "ACTIONABLE"
    return "EARLY SIGNAL"


def build_asset_payload(commodity: str, trade_bias: str) -> dict:
    base = ASSET_MAP.get(
        commodity,
        {
            "best_vehicle": commodity,
            "proxy_equities": [],
            "secondary_exposures": [],
        },
    )

    affected_assets = []
    best_vehicle = base["best_vehicle"]
    proxy_equities = base["proxy_equities"]
    secondary_exposures = base["secondary_exposures"]

    if best_vehicle:
        affected_assets.append(
            {
                "symbol": best_vehicle,
                "type": "vehicle",
                "bias": trade_bias,
                "priority": "primary",
            }
        )

    for ticker in proxy_equities:
        affected_assets.append(
            {
                "symbol": ticker,
                "type": "equity",
                "bias": trade_bias,
                "priority": "direct",
            }
        )

    for exposure in secondary_exposures:
        affected_assets.append(
            {
                "symbol": exposure,
                "type": "theme",
                "bias": trade_bias,
                "priority": "secondary",
            }
        )

    return {
        "best_vehicle": best_vehicle,
        "proxy_equities": proxy_equities,
        "secondary_exposures": secondary_exposures,
        "affected_assets": affected_assets,
    }


def build_recommendation(trade_bias: str, commodity: str, signal_level: int) -> str:
    if trade_bias == "bullish":
        if signal_level >= 8:
            return f"High-conviction bullish setup in {commodity}"
        if signal_level >= 5:
            return f"Actionable bullish setup in {commodity}"
        return f"Early bullish watch in {commodity}"

    if trade_bias == "bearish":
        if signal_level >= 8:
            return f"High-conviction bearish setup in {commodity}"
        if signal_level >= 5:
            return f"Actionable bearish setup in {commodity}"
        return f"Early bearish watch in {commodity}"

    return f"Monitor {commodity} closely"


def build_affected_market(commodity: str, payload: dict) -> str:
    parts = [commodity]
    if payload["proxy_equities"]:
        parts.append("equities: " + ", ".join(payload["proxy_equities"]))
    if payload["secondary_exposures"]:
        parts.append("secondary: " + ", ".join(payload["secondary_exposures"]))
    return " | ".join(parts)


def build_what_changed(region: str, anomaly_type: str, anomaly_value: float, details: dict, commodity: str) -> str:
    if anomaly_type in ("heatwave", "extreme_heat"):
        return f"{region} is showing {anomaly_type.replace('_', ' ')} conditions. Peak forecast temperature for the scan window reached {anomaly_value:.1f}°C, creating stress for {commodity} exposure."
    if anomaly_type == "frost":
        return f"{region} is showing frost risk. Minimum forecast temperature fell to {anomaly_value:.1f}°C, which matters for {commodity} exposure."
    if anomaly_type == "heavy_rain":
        return f"{region} is showing heavy rainfall. Forecast precipitation reached {anomaly_value:.1f} mm over the scan window, affecting {commodity} exposure."
    if anomaly_type == "drought":
        temp_mean = details.get("temp_c_mean")
        if temp_mean is not None:
            return f"{region} is showing drought conditions: only {anomaly_value:.1f} mm of rain with average temperature near {temp_mean:.1f}°C. This matters for {commodity} exposure."
        return f"{region} is showing drought conditions: only {anomaly_value:.1f} mm of rain in the scan window, affecting {commodity} exposure."
    if anomaly_type == "storm_wind":
        return f"{region} is showing storm-wind risk, with peak wind near {anomaly_value:.1f} m/s. This affects {commodity} exposure."
    return f"{region} is showing a {anomaly_type} signal affecting {commodity}."


def build_why_it_matters(anomaly_type: str, commodity: str, trade_bias: str) -> str:
    readable = anomaly_type.replace("_", " ")
    if trade_bias == "bullish":
        return f"{readable.title()} can tighten supply or raise weather-driven demand risk for {commodity}, which may support prices and related equities."
    if trade_bias == "bearish":
        return f"{readable.title()} can improve supply conditions or reduce scarcity pricing for {commodity}, which may pressure prices and related equities."
    return f"{readable.title()} may matter for {commodity}, but the market direction is not yet strong enough for a firm view."


def build_what_to_watch_next(anomaly_type: str, region: str) -> str:
    if anomaly_type in ("heatwave", "extreme_heat"):
        return f"Watch the next ECMWF runs for persistence of extreme temperatures in {region}, plus any shift in rainfall relief."
    if anomaly_type == "frost":
        return f"Watch the next ECMWF runs for minimum-temperature persistence in {region} and whether the cold pocket expands."
    if anomaly_type == "heavy_rain":
        return f"Watch whether rainfall totals keep rising in {region} and whether flooding or planting/harvest disruption risk broadens."
    if anomaly_type == "drought":
        return f"Watch whether the dry pattern in {region} persists across the next runs and whether heat intensifies."
    if anomaly_type == "storm_wind":
        return f"Watch whether wind intensity and storm track remain stable in the next ECMWF runs for {region}."
    return f"Watch the next ECMWF update for persistence in {region}."


def make_signal(
    region: str,
    commodity: str,
    anomaly_type: str,
    anomaly_value: float,
    severity_score: int,
    market_score: int,
    trade_bias: str,
    forecast_start,
    forecast_end,
    source_file: str,
    details: dict,
) -> dict:
    return {
        "timestamp": datetime.now(UTC),
        "region": region,
        "commodity": commodity,
        "anomaly_type": anomaly_type,
        "anomaly_value": float(anomaly_value),
        "severity_score": int(severity_score),
        "market_score": int(market_score),
        "persistence_score": 1,
        "signal_level": 1,
        "signal_bucket": "EARLY SIGNAL",
        "trade_bias": trade_bias,
        "recommendation": "",
        "affected_market": "",
        "best_vehicle": "",
        "proxy_equities": "",
        "secondary_exposures": "",
        "affected_assets_json": [],
        "what_changed": "",
        "why_it_matters": "",
        "what_to_watch_next": "",
        "forecast_start": forecast_start,
        "forecast_end": forecast_end,
        "source_file": os.path.basename(source_file),
        "details": details,
    }


def enrich_persistence_and_signal(conn, signals: list[dict]) -> list[dict]:
    if not signals:
        return signals

    with conn.cursor() as cur:
        for s in signals:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM weather_global_shocks
                WHERE region = %s
                  AND commodity = %s
                  AND anomaly_type = %s
                  AND timestamp >= NOW() - INTERVAL '3 days'
                """,
                (s["region"], s["commodity"], s["anomaly_type"]),
            )
            prior_hits = cur.fetchone()[0]

            if prior_hits >= 3:
                persistence = 5
            elif prior_hits == 2:
                persistence = 4
            elif prior_hits == 1:
                persistence = 3
            else:
                persistence = 1

            s["persistence_score"] = persistence

            raw = (s["severity_score"] * 0.45) + (persistence * 0.35) + (s["market_score"] * 0.20)
            signal_level = int(round(raw * 2))
            signal_level = max(1, min(10, signal_level))
            s["signal_level"] = signal_level
            s["signal_bucket"] = score_bucket(signal_level)

            asset_payload = build_asset_payload(s["commodity"], s["trade_bias"])
            s["best_vehicle"] = asset_payload["best_vehicle"]
            s["proxy_equities"] = ", ".join(asset_payload["proxy_equities"])
            s["secondary_exposures"] = ", ".join(asset_payload["secondary_exposures"])
            s["affected_assets_json"] = asset_payload["affected_assets"]

            s["recommendation"] = build_recommendation(s["trade_bias"], s["commodity"], signal_level)
            s["affected_market"] = build_affected_market(s["commodity"], asset_payload)
            s["what_changed"] = build_what_changed(
                s["region"],
                s["anomaly_type"],
                s["anomaly_value"],
                s["details"],
                s["commodity"],
            )
            s["why_it_matters"] = build_why_it_matters(s["anomaly_type"], s["commodity"], s["trade_bias"])
            s["what_to_watch_next"] = build_what_to_watch_next(s["anomaly_type"], s["region"])

    return signals


def dedupe_signals(signals: list[dict]) -> list[dict]:
    best = {}
    for s in signals:
        key = (s["region"], s["commodity"], s["anomaly_type"])
        if key not in best or s["signal_level"] > best[key]["signal_level"]:
            best[key] = s
    return list(best.values())


def insert_signals(conn, signals: list[dict]) -> None:
    if not signals:
        log("No real weather shocks detected in current forecast window.")
        return

    with conn.cursor() as cur:
        for s in signals:
            cur.execute(
                """
                INSERT INTO weather_global_shocks (
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
                    details
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s,
                    %s, %s, %s, %s::jsonb
                )
                """,
                (
                    s["timestamp"],
                    s["region"],
                    s["commodity"],
                    s["anomaly_type"],
                    s["anomaly_value"],
                    s["persistence_score"],
                    s["severity_score"],
                    s["market_score"],
                    s["signal_level"],
                    s["signal_bucket"],
                    s["trade_bias"],
                    s["recommendation"],
                    s["affected_market"],
                    s["best_vehicle"],
                    s["proxy_equities"],
                    s["secondary_exposures"],
                    json.dumps(s["affected_assets_json"]),
                    s["what_changed"],
                    s["why_it_matters"],
                    s["what_to_watch_next"],
                    s["source_file"],
                    s["forecast_start"],
                    s["forecast_end"],
                    json.dumps(s["details"]),
                ),
            )
        conn.commit()


def generate_real_shocks(conn, source_file: str) -> list[dict]:
    log(f"Opening GRIB: {source_file}")
    ds = open_grib_dataset(source_file)

    all_signals = []
    for region in REGIONS:
        try:
            stats = extract_field_stats(ds, region)
            signals = build_signals_from_stats(region, stats, source_file)
            all_signals.extend(signals)
        except Exception as e:
            log(f"Region failed: {region['name']} -> {e}")

    all_signals = enrich_persistence_and_signal(conn, all_signals)
    all_signals = dedupe_signals(all_signals)
    all_signals.sort(key=lambda x: x["signal_level"], reverse=True)
    return all_signals


def main() -> None:
    log("Starting real global shocks generator...")
    source_file = find_latest_grib()

    conn = psycopg.connect(DATABASE_URL)
    try:
        ensure_schema(conn)
        signals = generate_real_shocks(conn, source_file)
        insert_signals(conn, signals)
        log(f"Completed successfully. Inserted {len(signals)} real signals.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
