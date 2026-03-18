import glob
import json
import math
import os
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import psycopg
import xarray as xr

from weather_radar_rules import RADAR_EVENT_RULES

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
        "name": "Brazil",
        "lat_min": -25.0,
        "lat_max": -10.0,
        "lon_min": -60.0,
        "lon_max": -40.0,
        "commodities": ["Soybeans", "Coffee", "Sugar"],
    },
    {
        "name": "Mato Grosso",
        "lat_min": -17.0,
        "lat_max": -8.0,
        "lon_min": -60.0,
        "lon_max": -52.0,
        "commodities": ["Corn", "Soybeans"],
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
    {
        "name": "US Gulf",
        "lat_min": 24.0,
        "lat_max": 31.5,
        "lon_min": -98.0,
        "lon_max": -80.0,
        "commodities": ["Oil", "Natural Gas"],
    },
    {
        "name": "Southeast US",
        "lat_min": 25.0,
        "lat_max": 36.5,
        "lon_min": -91.0,
        "lon_max": -75.0,
        "commodities": ["Power Utilities"],
    },
    {
        "name": "California",
        "lat_min": 32.0,
        "lat_max": 42.0,
        "lon_min": -125.0,
        "lon_max": -114.0,
        "commodities": ["Power Utilities"],
    },
    # --- New regions ---
    {
        "name": "West Africa Cocoa Belt",
        "lat_min": 3.0,
        "lat_max": 10.0,
        "lon_min": -8.0,
        "lon_max": 2.0,
        "commodities": ["Cocoa", "Palm Oil"],
    },
    {
        "name": "Southeast Asia",
        "lat_min": 0.0,
        "lat_max": 15.0,
        "lon_min": 98.0,
        "lon_max": 120.0,
        "commodities": ["Palm Oil", "Rice"],
    },
    {
        "name": "Canadian Prairies",
        "lat_min": 48.0,
        "lat_max": 55.0,
        "lon_min": -115.0,
        "lon_max": -100.0,
        "commodities": ["Wheat", "Canola"],
    },
    {
        "name": "Middle East Gulf",
        "lat_min": 22.0,
        "lat_max": 30.0,
        "lon_min": 45.0,
        "lon_max": 60.0,
        "commodities": ["Oil", "LNG"],
    },
    {
        "name": "North Sea",
        "lat_min": 53.0,
        "lat_max": 62.0,
        "lon_min": 0.0,
        "lon_max": 8.0,
        "commodities": ["Natural Gas", "Oil"],
    },
    {
        "name": "East Africa",
        "lat_min": 0.0,
        "lat_max": 10.0,
        "lon_min": 35.0,
        "lon_max": 42.0,
        "commodities": ["Coffee"],
    },
    {
        "name": "US Pacific Northwest",
        "lat_min": 44.0,
        "lat_max": 49.0,
        "lon_min": -124.0,
        "lon_max": -120.0,
        "commodities": ["Power Utilities"],
    },
    {
        "name": "China Yangtze Basin",
        "lat_min": 28.0,
        "lat_max": 34.0,
        "lon_min": 108.0,
        "lon_max": 122.0,
        "commodities": ["Soybeans", "Rice"],
    },
    {
        "name": "Southern Europe",
        "lat_min": 35.0,
        "lat_max": 45.0,
        "lon_min": 5.0,
        "lon_max": 25.0,
        "commodities": ["Wheat", "Olive Oil"],
    },
    # --- Additional new regions ---
    {
        "name": "Ukraine Eastern Europe",
        "lat_min": 44.0,
        "lat_max": 54.0,
        "lon_min": 22.0,
        "lon_max": 40.0,
        "commodities": ["Wheat", "Sunflower Oil"],
    },
    {
        "name": "Nordic Scandinavia",
        "lat_min": 55.0,
        "lat_max": 72.0,
        "lon_min": 4.0,
        "lon_max": 30.0,
        "commodities": ["Natural Gas", "Hydropower"],
    },
    {
        "name": "Andes South America",
        "lat_min": -35.0,
        "lat_max": -15.0,
        "lon_min": -76.0,
        "lon_max": -65.0,
        "commodities": ["Copper", "Lithium"],
    },
    {
        "name": "New Zealand",
        "lat_min": -47.0,
        "lat_max": -34.0,
        "lon_min": 165.0,
        "lon_max": 178.0,
        "commodities": ["Dairy", "Natural Gas"],
    },
    {
        "name": "US Great Plains",
        "lat_min": 36.0,
        "lat_max": 48.0,
        "lon_min": -104.0,
        "lon_max": -96.0,
        "commodities": ["Wheat", "Cattle"],
    },
    {
        "name": "Central America",
        "lat_min": 8.0,
        "lat_max": 18.0,
        "lon_min": -92.0,
        "lon_max": -75.0,
        "commodities": ["Coffee", "Sugar"],
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
    "Oil": 5,
    # New commodities
    "Cocoa": 5,
    "Palm Oil": 4,
    "Canola": 4,
    "LNG": 5,
    "Olive Oil": 3,
    # Additional new commodities
    "Sunflower Oil": 4,
    "Hydropower": 4,
    "Copper": 5,
    "Lithium": 5,
    "Dairy": 3,
    "Cattle": 3,
}

RULES = {
    "heatwave": {
        "temp_c_max": 35.0,
        "severity_step_c": 2.0,
        "base_score": 3,
        "bullish_for": {"Corn", "Soybeans", "Wheat", "Coffee", "Sugar", "Natural Gas", "Power Utilities",
                        "Sunflower Oil", "Copper", "Dairy", "Cattle"},
    },
    "extreme_heat": {
        "temp_c_max": 40.0,
        "severity_step_c": 2.0,
        "base_score": 4,
        "bullish_for": {"Corn", "Soybeans", "Wheat", "Coffee", "Sugar", "Natural Gas", "Power Utilities",
                        "Sunflower Oil", "Copper", "Dairy", "Cattle"},
    },
    "frost": {
        "temp_c_min": 0.0,
        "severity_step_c": 2.0,
        "base_score": 4,
        "bullish_for": {"Coffee", "Sugar", "Wheat", "Natural Gas", "Power Utilities",
                        "Sunflower Oil", "Dairy"},
    },
    "heavy_rain": {
        "precip_mm_7d": 100.0,
        "severity_step_mm": 25.0,
        "base_score": 3,
        "bullish_for": {"Natural Gas", "Power Utilities", "Hydropower"},
        "bearish_for": {"Wheat", "Corn", "Soybeans", "Coffee", "Sugar", "Sunflower Oil"},
    },
    "drought": {
        "precip_mm_7d_max": 10.0,
        "temp_c_mean_min": 28.0,
        "severity_step_mm": 5.0,
        "base_score": 4,
        "bullish_for": {"Corn", "Soybeans", "Wheat", "Coffee", "Sugar",
                        "Sunflower Oil", "Copper", "Lithium", "Dairy", "Cattle"},
        "bearish_for": {"Hydropower"},
    },
    "storm_wind": {
        "wind_ms_max": 18.0,
        "severity_step_ms": 3.0,
        "base_score": 3,
        "bullish_for": {"Natural Gas", "Power Utilities", "Coal", "Oil"},
    },
    "flood_risk": {
        "precip_mm_7d": 140.0,
        "severity_step_mm": 30.0,
        "base_score": 4,
        "bullish_for": {"Power Utilities", "Oil", "Natural Gas", "Hydropower"},
        "bearish_for": {"Corn", "Soybeans", "Wheat", "Coffee", "Sugar", "Sunflower Oil"},
    },
    "wildfire_risk": {
        "temp_c_max": 36.0,
        "precip_mm_7d_max": 5.0,
        "wind_ms_min": 10.0,
        "severity_step_c": 2.0,
        "base_score": 4,
        "bullish_for": {"Power Utilities"},
    },
    "cold_wave": {
        "temp_c_min": -5.0,
        "severity_step_c": 3.0,
        "base_score": 4,
        "bullish_for": {"Natural Gas", "Power Utilities", "Wheat", "Dairy"},
    },
    "hurricane_risk": {
        "wind_ms_max": 25.0,
        "precip_mm_7d": 120.0,
        "severity_step_ms": 5.0,
        "base_score": 5,
        "bullish_for": {"Oil", "Natural Gas", "Power Utilities"},
    },
    # --- New anomaly types ---
    "polar_vortex": {
        "temp_c_min": -15.0,
        "severity_step_c": 3.0,
        "base_score": 5,
        "bullish_for": {"Natural Gas", "Power Utilities", "LNG", "Coal"},
        "bearish_for": set(),
    },
    "atmospheric_river": {
        "precip_mm_7d": 200.0,
        "severity_step_mm": 40.0,
        "base_score": 4,
        "bullish_for": {"Power Utilities"},
        "bearish_for": {"Wheat", "Corn", "Soybeans", "Coffee", "Sugar"},
    },
    "monsoon_failure": {
        # Triggered in build_signals_from_stats with date-aware logic
        "precip_mm_7d_max": 5.0,
        "severity_step_mm": 2.0,
        "base_score": 5,
        "bullish_for": {"Rice", "Sugar", "Palm Oil", "Coffee"},
        "bearish_for": set(),
    },
    "ice_storm": {
        # temp between -2 and +2°C AND precip >= 50mm — freezing rain proxy
        "precip_mm_7d": 50.0,
        "severity_step_mm": 15.0,
        "base_score": 4,
        "bullish_for": {"Natural Gas", "Power Utilities", "LNG"},
        "bearish_for": set(),
    },
    "extreme_wind": {
        "wind_ms_max": 30.0,
        "severity_step_ms": 5.0,
        "base_score": 4,
        "bullish_for": {"Oil", "Natural Gas", "LNG"},
        "bearish_for": set(),
    },
}

ASSET_MAP = {
    "Corn": {
        "best_vehicle": "CORN",
        "proxy_equities": ["ADM", "BG", "CF", "MOS", "CTVA", "DE", "UNP"],
        "secondary_exposures": ["ethanol", "grain handlers", "rail logistics", "crop insurers", "farm equipment"],
    },
    "Soybeans": {
        "best_vehicle": "SOYB",
        "proxy_equities": ["ADM", "BG", "CF", "MOS", "CTVA", "DE"],
        "secondary_exposures": ["soy processors", "export terminals", "fertilizer", "farm equipment"],
    },
    "Wheat": {
        "best_vehicle": "WEAT",
        "proxy_equities": ["ADM", "BG", "MOS", "CF", "DE"],
        "secondary_exposures": ["grain traders", "fertilizer", "farm equipment", "food inflation"],
    },
    "Coffee": {
        "best_vehicle": "JO",
        "proxy_equities": ["SBUX", "NSRGY"],
        "secondary_exposures": ["coffee roasters", "packaged beverages", "soft commodities"],
    },
    "Sugar": {
        "best_vehicle": "CANE",
        "proxy_equities": ["CZZ", "TRRJF"],
        "secondary_exposures": ["ethanol-linked producers", "food input costs", "soft commodities"],
    },
    "Natural Gas": {
        "best_vehicle": "UNG",
        "proxy_equities": ["EQT", "LNG", "CTRA", "RRC"],
        "secondary_exposures": ["LNG exporters", "utilities", "power generation", "industrial demand"],
    },
    "Power Utilities": {
        "best_vehicle": "XLU",
        "proxy_equities": ["NEE", "DUK", "SO", "AEP"],
        "secondary_exposures": ["power generators", "grid operators", "gas-sensitive industrials"],
    },
    "Rice": {
        "best_vehicle": "DBA",
        "proxy_equities": ["ADM", "BG"],
        "secondary_exposures": ["food staples", "Asian agri merchants", "supply-chain logistics"],
    },
    "Coal": {
        "best_vehicle": "KOL",
        "proxy_equities": ["BTU", "ARCH", "AMR"],
        "secondary_exposures": ["bulk shipping", "power generation", "rail freight"],
    },
    "Oil": {
        "best_vehicle": "USO",
        "proxy_equities": ["XOM", "CVX", "COP"],
        "secondary_exposures": ["refiners", "offshore services", "tankers"],
    },
    # New commodities
    "Cocoa": {
        "best_vehicle": "NIB",
        "proxy_equities": ["MDLZ", "HSY", "ADM"],
        "secondary_exposures": ["chocolate confectionery", "food manufacturers", "soft commodities"],
    },
    "Palm Oil": {
        "best_vehicle": "DBA",
        "proxy_equities": ["ADM", "BG"],
        "secondary_exposures": ["food manufacturers", "biodiesel", "Asian agri merchants"],
    },
    "Canola": {
        "best_vehicle": "MOO",
        "proxy_equities": ["NTR", "ADM", "BG"],
        "secondary_exposures": ["oilseed processors", "vegetable oil", "Canadian agribusiness"],
    },
    "LNG": {
        "best_vehicle": "UNG",
        "proxy_equities": ["LNG", "EQNR", "SHEL", "BP"],
        "secondary_exposures": ["LNG exporters", "regasification terminals", "gas utilities"],
    },
    "Olive Oil": {
        "best_vehicle": "DBA",
        "proxy_equities": ["ADM", "GIS"],
        "secondary_exposures": ["food manufacturers", "Mediterranean agriculture", "specialty foods"],
    },
    # Additional new commodity asset mappings
    "Sunflower Oil": {
        "best_vehicle": "DBA",
        "proxy_equities": ["ADM", "BG", "NTR"],
        "secondary_exposures": ["oilseed processors", "vegetable oil", "Eastern European agribusiness"],
    },
    "Hydropower": {
        "best_vehicle": "XLU",
        "proxy_equities": ["BEP", "AES", "NEE"],
        "secondary_exposures": ["renewable energy utilities", "power grid operators", "water management"],
    },
    "Copper": {
        "best_vehicle": "COPX",
        "proxy_equities": ["FCX", "SCCO", "BHP", "RIO"],
        "secondary_exposures": ["copper mining", "EV supply chain", "construction materials"],
    },
    "Lithium": {
        "best_vehicle": "LIT",
        "proxy_equities": ["ALB", "SQM", "LTHM"],
        "secondary_exposures": ["EV battery supply chain", "energy storage", "chemicals"],
    },
    "Dairy": {
        "best_vehicle": "DBA",
        "proxy_equities": ["SMFG", "TSN", "PPC"],
        "secondary_exposures": ["dairy processors", "food manufacturers", "agricultural inputs"],
    },
    "Cattle": {
        "best_vehicle": "COW",
        "proxy_equities": ["TSN", "PPC", "WH"],
        "secondary_exposures": ["meatpacking", "food manufacturers", "feedlots"],
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
        # Add new columns if they don't exist (idempotent migrations)
        migrations = [
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS trend_direction TEXT DEFAULT 'new'",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS media_validated BOOLEAN DEFAULT NULL",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS media_source TEXT DEFAULT NULL",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS media_headline TEXT DEFAULT NULL",
            "ALTER TABLE weather_global_shocks ADD COLUMN IF NOT EXISTS media_score FLOAT DEFAULT NULL",
        ]
        for migration in migrations:
            cur.execute(migration)
        conn.commit()


def find_latest_grib() -> str:
    files = sorted(glob.glob(GRIB_GLOB, recursive=True), key=os.path.getmtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No GRIB files found with pattern: {GRIB_GLOB}")
    return files[0]


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


def open_grib_dataset(path: str) -> xr.Dataset:
    backend_kwargs = {"indexpath": ""}
    return xr.open_dataset(path, engine="cfgrib", backend_kwargs=backend_kwargs)


def open_grib_dataset_filtered(path: str, filter_by_keys: dict) -> xr.Dataset | None:
    try:
        backend_kwargs = {"indexpath": "", "filter_by_keys": filter_by_keys}
        return xr.open_dataset(path, engine="cfgrib", backend_kwargs=backend_kwargs)
    except Exception:
        return None


def normalize_longitudes(ds: xr.Dataset) -> xr.Dataset:
    lon_name = get_coord_name(ds, ["longitude", "lon", "long"])
    if lon_name is None:
        return ds
    lon_vals = ds[lon_name].values
    if np.nanmax(lon_vals) > 180:
        new_lon = ((lon_vals + 180) % 360) - 180
        ds = ds.assign_coords({lon_name: new_lon}).sortby(lon_name)
    return ds


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


def sanitize_details(details: dict) -> dict:
    clean = {}
    for k, v in details.items():
        if isinstance(v, datetime):
            clean[k] = v.isoformat()
        elif isinstance(v, np.generic):
            clean[k] = v.item()
        else:
            clean[k] = v
    return clean


def safe_precip_value_mm(tp_region: xr.DataArray) -> float:
    """
    Safer precipitation logic:
    - ECMWF tp is often cumulative
    - use final minus initial if possible
    - then compute a regional mean, not a crazy max grid-cell spike
    - clamp impossible values
    """
    precip_mm = tp_region * 1000.0

    try:
        if "step" in precip_mm.coords and precip_mm.sizes.get("step", 0) > 1:
            first = precip_mm.isel(step=0)
            last = precip_mm.isel(step=-1)
            total_mm = last - first
        elif "valid_time" in precip_mm.coords and precip_mm.sizes.get("valid_time", 0) > 1:
            first = precip_mm.isel(valid_time=0)
            last = precip_mm.isel(valid_time=-1)
            total_mm = last - first
        elif "time" in precip_mm.coords and precip_mm.sizes.get("time", 0) > 1:
            first = precip_mm.isel(time=0)
            last = precip_mm.isel(time=-1)
            total_mm = last - first
        else:
            total_mm = precip_mm
    except Exception:
        total_mm = precip_mm

    try:
        total_mm = total_mm.where(total_mm >= 0, 0)
    except Exception:
        pass

    mean_mm = float(total_mm.mean(skipna=True).values)

    if not np.isfinite(mean_mm):
        return 0.0

    # Safety clamp for clearly broken values
    if mean_mm > 2000:
        return 2000.0

    return mean_mm


def extract_field_stats(main_ds: xr.Dataset, region: dict, source_file: str) -> dict:
    stats = {
        "temp_c_max": None,
        "temp_c_mean": None,
        "temp_c_min": None,
        "precip_mm_7d": None,
        "wind_ms_max": None,
        "forecast_start": None,
        "forecast_end": None,
    }

    ds_temp = normalize_longitudes(main_ds)
    t_name = get_var_name(ds_temp, ["t2m", "2t"])
    if t_name is None:
        fallback = open_grib_dataset_filtered(source_file, {"shortName": "2t"})
        if fallback is not None:
            ds_temp = normalize_longitudes(fallback)
            t_name = get_var_name(ds_temp, ["t2m", "2t"])

    if t_name:
        t = trim_forecast_horizon(subset_region(ds_temp[t_name], region))
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

    ds_tp = normalize_longitudes(main_ds)
    tp_name = get_var_name(ds_tp, ["tp", "total_precipitation"])
    if tp_name is None:
        fallback = open_grib_dataset_filtered(source_file, {"shortName": "tp"})
        if fallback is not None:
            ds_tp = normalize_longitudes(fallback)
            tp_name = get_var_name(ds_tp, ["tp", "total_precipitation"])

    if tp_name:
        tp = trim_forecast_horizon(subset_region(ds_tp[tp_name], region))
        stats["precip_mm_7d"] = safe_precip_value_mm(tp)

        if stats["forecast_start"] is None:
            if "valid_time" in tp.coords:
                times = pd.to_datetime(tp["valid_time"].values)
            elif "time" in tp.coords:
                times = pd.to_datetime(tp["time"].values)
            else:
                times = pd.to_datetime([datetime.now(UTC)])
            stats["forecast_start"] = pd.Timestamp(times.min()).to_pydatetime()
            stats["forecast_end"] = pd.Timestamp(times.max()).to_pydatetime()

    ds_wind = normalize_longitudes(main_ds)
    wind_name = get_var_name(ds_wind, ["si10", "wind10m", "ws10"])
    u10_name = get_var_name(ds_wind, ["u10", "10u"])
    v10_name = get_var_name(ds_wind, ["v10", "10v"])

    if wind_name:
        wind = trim_forecast_horizon(subset_region(ds_wind[wind_name], region))
        stats["wind_ms_max"] = float(wind.max(skipna=True).values)
    else:
        ds_u = None
        ds_v = None

        if u10_name is None:
            fallback_u = open_grib_dataset_filtered(
                source_file,
                {"shortName": "10u", "typeOfLevel": "heightAboveGround", "level": 10},
            )
            if fallback_u is not None:
                ds_u = normalize_longitudes(fallback_u)
                u10_name = get_var_name(ds_u, ["u10", "10u"])
        else:
            ds_u = ds_wind

        if v10_name is None:
            fallback_v = open_grib_dataset_filtered(
                source_file,
                {"shortName": "10v", "typeOfLevel": "heightAboveGround", "level": 10},
            )
            if fallback_v is not None:
                ds_v = normalize_longitudes(fallback_v)
                v10_name = get_var_name(ds_v, ["v10", "10v"])
        else:
            ds_v = ds_wind

        if u10_name and v10_name and ds_u is not None and ds_v is not None:
            u = trim_forecast_horizon(subset_region(ds_u[u10_name], region))
            v = trim_forecast_horizon(subset_region(ds_v[v10_name], region))
            w = np.sqrt((u ** 2) + (v ** 2))
            stats["wind_ms_max"] = float(w.max(skipna=True).values)

    return stats


def severity_from_excess(excess: float, step: float, base: int) -> int:
    if excess <= 0:
        return 0
    return min(10, max(base, base + int(math.floor(excess / step))))


def classify_trade_bias(anomaly_type: str, commodity: str) -> str:
    rule = RULES.get(anomaly_type, {})
    if commodity in rule.get("bullish_for", set()):
        return "bullish"
    if commodity in rule.get("bearish_for", set()):
        return "bearish"
    return "watch"


def normalize_event_key(anomaly_type: str) -> str:
    mapping = {
        "extreme_heat": "heatwave",
        "frost": "cold_wave",
        "heavy_rain": "flood",
        "flood_risk": "flood",
        "storm_wind": "storm_wind",
        "wildfire_risk": "wildfire",
        "cold_wave": "cold_wave",
        "hurricane_risk": "hurricane",
        "polar_vortex": "cold_wave",
        "atmospheric_river": "flood",
        "monsoon_failure": "drought",
        "ice_storm": "cold_wave",
        "extreme_wind": "storm_wind",
    }
    return mapping.get(anomaly_type, anomaly_type)


def build_radar_event_note(anomaly_type: str) -> str:
    norm = normalize_event_key(anomaly_type)
    rule = RADAR_EVENT_RULES.get(norm)
    if not rule:
        return anomaly_type.replace("_", " ").title()
    return rule["description"]


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
        "persistence_score": 0,
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
        "details": sanitize_details(details),
        "trend_direction": "new",  # enriched later
    }


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
            signals.append(make_signal(region["name"], commodity, "heatwave", temp_c_max, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("heatwave", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        if temp_c_max is not None and temp_c_max >= RULES["extreme_heat"]["temp_c_max"]:
            excess = temp_c_max - RULES["extreme_heat"]["temp_c_max"]
            severity = severity_from_excess(excess, RULES["extreme_heat"]["severity_step_c"], RULES["extreme_heat"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "extreme_heat", temp_c_max, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("extreme_heat", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        if temp_c_min is not None and temp_c_min <= RULES["frost"]["temp_c_min"]:
            excess = RULES["frost"]["temp_c_min"] - temp_c_min
            severity = severity_from_excess(excess, RULES["frost"]["severity_step_c"], RULES["frost"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "frost", temp_c_min, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("frost", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        if precip_mm_7d is not None and precip_mm_7d >= RULES["heavy_rain"]["precip_mm_7d"]:
            excess = precip_mm_7d - RULES["heavy_rain"]["precip_mm_7d"]
            severity = severity_from_excess(excess, RULES["heavy_rain"]["severity_step_mm"], RULES["heavy_rain"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "heavy_rain", precip_mm_7d, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("heavy_rain", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        if (
            precip_mm_7d is not None
            and temp_c_mean is not None
            and precip_mm_7d <= RULES["drought"]["precip_mm_7d_max"]
            and temp_c_mean >= RULES["drought"]["temp_c_mean_min"]
        ):
            excess = RULES["drought"]["precip_mm_7d_max"] - precip_mm_7d
            severity = severity_from_excess(excess, RULES["drought"]["severity_step_mm"], RULES["drought"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "drought", precip_mm_7d, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("drought", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        if wind_ms_max is not None and wind_ms_max >= RULES["storm_wind"]["wind_ms_max"]:
            excess = wind_ms_max - RULES["storm_wind"]["wind_ms_max"]
            severity = severity_from_excess(excess, RULES["storm_wind"]["severity_step_ms"], RULES["storm_wind"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "storm_wind", wind_ms_max, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("storm_wind", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        if precip_mm_7d is not None and precip_mm_7d >= RULES["flood_risk"]["precip_mm_7d"]:
            excess = precip_mm_7d - RULES["flood_risk"]["precip_mm_7d"]
            severity = severity_from_excess(excess, RULES["flood_risk"]["severity_step_mm"], RULES["flood_risk"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "flood_risk", precip_mm_7d, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("flood_risk", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        if (
            temp_c_max is not None
            and precip_mm_7d is not None
            and wind_ms_max is not None
            and temp_c_max >= RULES["wildfire_risk"]["temp_c_max"]
            and precip_mm_7d <= RULES["wildfire_risk"]["precip_mm_7d_max"]
            and wind_ms_max >= RULES["wildfire_risk"]["wind_ms_min"]
        ):
            excess = temp_c_max - RULES["wildfire_risk"]["temp_c_max"]
            severity = severity_from_excess(excess, RULES["wildfire_risk"]["severity_step_c"], RULES["wildfire_risk"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "wildfire_risk", temp_c_max, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("wildfire_risk", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        if temp_c_min is not None and temp_c_min <= RULES["cold_wave"]["temp_c_min"]:
            excess = RULES["cold_wave"]["temp_c_min"] - temp_c_min
            severity = severity_from_excess(excess, RULES["cold_wave"]["severity_step_c"], RULES["cold_wave"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "cold_wave", temp_c_min, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("cold_wave", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        if (
            wind_ms_max is not None
            and precip_mm_7d is not None
            and wind_ms_max >= RULES["hurricane_risk"]["wind_ms_max"]
            and precip_mm_7d >= RULES["hurricane_risk"]["precip_mm_7d"]
        ):
            excess = wind_ms_max - RULES["hurricane_risk"]["wind_ms_max"]
            severity = severity_from_excess(excess, RULES["hurricane_risk"]["severity_step_ms"], RULES["hurricane_risk"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "hurricane_risk", wind_ms_max, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("hurricane_risk", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        # --- New anomaly types ---

        # polar_vortex: temp_min <= -15°C
        if temp_c_min is not None and temp_c_min <= RULES["polar_vortex"]["temp_c_min"]:
            excess = RULES["polar_vortex"]["temp_c_min"] - temp_c_min
            severity = severity_from_excess(excess, RULES["polar_vortex"]["severity_step_c"], RULES["polar_vortex"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "polar_vortex", temp_c_min, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("polar_vortex", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        # atmospheric_river: precip >= 200mm/7d
        if precip_mm_7d is not None and precip_mm_7d >= RULES["atmospheric_river"]["precip_mm_7d"]:
            excess = precip_mm_7d - RULES["atmospheric_river"]["precip_mm_7d"]
            severity = severity_from_excess(excess, RULES["atmospheric_river"]["severity_step_mm"], RULES["atmospheric_river"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "atmospheric_river", precip_mm_7d, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("atmospheric_river", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        # monsoon_failure: precip <= 5mm in peak monsoon months (Jun-Sep) for India/SE Asia/East Africa
        monsoon_regions = {"India", "Southeast Asia", "East Africa", "West Africa Cocoa Belt"}
        current_month = datetime.now(UTC).month
        if (
            region["name"] in monsoon_regions
            and current_month in {6, 7, 8, 9}
            and precip_mm_7d is not None
            and precip_mm_7d <= RULES["monsoon_failure"]["precip_mm_7d_max"]
        ):
            excess = RULES["monsoon_failure"]["precip_mm_7d_max"] - precip_mm_7d
            severity = severity_from_excess(excess, RULES["monsoon_failure"]["severity_step_mm"], RULES["monsoon_failure"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "monsoon_failure", precip_mm_7d, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("monsoon_failure", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        # ice_storm: temp between -2°C and +2°C AND precip >= 50mm — freezing rain proxy
        if (
            temp_c_mean is not None
            and precip_mm_7d is not None
            and -2.0 <= temp_c_mean <= 2.0
            and precip_mm_7d >= RULES["ice_storm"]["precip_mm_7d"]
        ):
            excess = precip_mm_7d - RULES["ice_storm"]["precip_mm_7d"]
            severity = severity_from_excess(excess, RULES["ice_storm"]["severity_step_mm"], RULES["ice_storm"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "ice_storm", precip_mm_7d, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("ice_storm", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

        # extreme_wind: wind >= 30 m/s (higher tier than storm_wind)
        if wind_ms_max is not None and wind_ms_max >= RULES["extreme_wind"]["wind_ms_max"]:
            excess = wind_ms_max - RULES["extreme_wind"]["wind_ms_max"]
            severity = severity_from_excess(excess, RULES["extreme_wind"]["severity_step_ms"], RULES["extreme_wind"]["base_score"])
            signals.append(make_signal(region["name"], commodity, "extreme_wind", wind_ms_max, severity, MARKET_SENSITIVITY.get(commodity, 3), classify_trade_bias("extreme_wind", commodity), stats["forecast_start"], stats["forecast_end"], source_file, stats))

    return signals


def score_bucket(score: int) -> str:
    if score >= 8:
        return "HIGH"
    if score >= 5:
        return "MEDIUM"
    return "EARLY"


def build_asset_payload(commodity: str, trade_bias: str) -> dict:
    base = ASSET_MAP.get(
        commodity,
        {"best_vehicle": commodity, "proxy_equities": [], "secondary_exposures": []},
    )

    affected_assets = []
    if base["best_vehicle"]:
        affected_assets.append({"symbol": base["best_vehicle"], "type": "vehicle", "bias": trade_bias, "priority": "primary"})
    for ticker in base["proxy_equities"]:
        affected_assets.append({"symbol": ticker, "type": "equity", "bias": trade_bias, "priority": "direct"})
    for exposure in base["secondary_exposures"]:
        affected_assets.append({"symbol": exposure, "type": "theme", "bias": trade_bias, "priority": "secondary"})

    return {
        "best_vehicle": base["best_vehicle"],
        "proxy_equities": base["proxy_equities"],
        "secondary_exposures": base["secondary_exposures"],
        "affected_assets": affected_assets,
    }


def build_recommendation(trade_bias: str, commodity: str, signal_level: int) -> str:
    if trade_bias == "bullish":
        if signal_level >= 8:
            return f"High-conviction long setup in {commodity}"
        if signal_level >= 5:
            return f"Actionable long setup in {commodity}"
        return f"Early long watch in {commodity}"
    if trade_bias == "bearish":
        if signal_level >= 8:
            return f"High-conviction short setup in {commodity}"
        if signal_level >= 5:
            return f"Actionable short setup in {commodity}"
        return f"Early short watch in {commodity}"
    return f"Monitor {commodity} closely"


def build_affected_market(commodity: str, payload: dict) -> str:
    parts = [commodity]
    if payload["proxy_equities"]:
        parts.append("equities: " + ", ".join(payload["proxy_equities"]))
    if payload["secondary_exposures"]:
        parts.append("secondary: " + ", ".join(payload["secondary_exposures"]))
    return " | ".join(parts)


def build_what_changed(region: str, anomaly_type: str, anomaly_value: float, details: dict, commodity: str) -> str:
    label = build_radar_event_note(anomaly_type)
    return f"{region} is showing {label.lower()} affecting {commodity}. Measured trigger value: {anomaly_value:.1f}."


def build_why_it_matters(anomaly_type: str, commodity: str, trade_bias: str) -> str:
    label = build_radar_event_note(anomaly_type)
    if trade_bias == "bullish":
        return f"{label} can tighten supply, disrupt logistics, or raise demand risk for {commodity}, which may support prices and related stocks."
    if trade_bias == "bearish":
        return f"{label} can damage operating conditions or pressure pricing for {commodity}, which may hurt prices and related stocks."
    return f"{label} may matter for {commodity}, but the market direction is not yet strong enough for a firm view."


def build_what_to_watch_next(anomaly_type: str, region: str) -> str:
    label = build_radar_event_note(anomaly_type)
    return f"Watch the next ECMWF runs to see whether {label.lower()} persists or intensifies in {region}."


def enrich_persistence_and_signal(conn, signals: list[dict]) -> list[dict]:
    if not signals:
        return signals

    with conn.cursor() as cur:
        for s in signals:
            # Extended persistence window: 7 days
            cur.execute(
                """
                SELECT COUNT(*)
                FROM weather_global_shocks
                WHERE region = %s
                  AND commodity = %s
                  AND anomaly_type = %s
                  AND timestamp >= NOW() - INTERVAL '7 days'
                """,
                (s["region"], s["commodity"], s["anomaly_type"]),
            )
            prior_hits = cur.fetchone()[0]

            if prior_hits >= 5:
                persistence = 5
            elif prior_hits >= 4:
                persistence = 4
            elif prior_hits >= 3:
                persistence = 3
            elif prior_hits >= 2:
                persistence = 2
            elif prior_hits >= 1:
                persistence = 1
            else:
                persistence = 0  # No prior — new signal, not inflated

            s["persistence_score"] = persistence

            # Compute trend direction by comparing to most recent prior signal level
            cur.execute(
                """
                SELECT signal_level
                FROM weather_global_shocks
                WHERE region = %s
                  AND commodity = %s
                  AND anomaly_type = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (s["region"], s["commodity"], s["anomaly_type"]),
            )
            prev_row = cur.fetchone()
            if prev_row is None:
                trend_direction = "new"
            else:
                prev_level = prev_row[0] or 0
                # Compute current signal level preview (without persistence bias)
                raw_preview = (s["severity_score"] * 0.45) + (persistence * 0.35) + (s["market_score"] * 0.20)
                current_level_preview = int(round(raw_preview * 2))
                if current_level_preview > prev_level + 1:
                    trend_direction = "worsening"
                elif current_level_preview < prev_level - 1:
                    trend_direction = "recovering"
                else:
                    trend_direction = "stable"

            s["trend_direction"] = trend_direction

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
            s["what_changed"] = build_what_changed(s["region"], s["anomaly_type"], s["anomaly_value"], s["details"], s["commodity"])
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
                    details,
                    trend_direction
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s,
                    %s, %s, %s, %s::jsonb, %s
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
                    json.dumps(s["affected_assets_json"], default=str),
                    s["what_changed"],
                    s["why_it_matters"],
                    s["what_to_watch_next"],
                    s["source_file"],
                    s["forecast_start"],
                    s["forecast_end"],
                    json.dumps(s["details"], default=str),
                    s.get("trend_direction", "new"),
                ),
            )
        conn.commit()


def generate_real_shocks(conn, source_file: str) -> list[dict]:
    log(f"Opening GRIB: {source_file}")
    main_ds = open_grib_dataset(source_file)

    all_signals = []
    for region in REGIONS:
        try:
            stats = extract_field_stats(main_ds, region, source_file)
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
