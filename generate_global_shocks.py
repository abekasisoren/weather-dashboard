from ecmwf.opendata import Client
from datetime import datetime, timedelta, timezone
import xarray as xr
import pandas as pd
import numpy as np

client = Client(source="aws")


def candidate_00z_runs():
    now = datetime.now(timezone.utc)
    base = now.replace(hour=0, minute=0, second=0, microsecond=0)
    for days_back in range(0, 6):
        d = base - timedelta(days=days_back)
        yield d.strftime("%Y%m%d"), 0


def open_param_dataset(grib_file, short_name):
    return xr.open_dataset(
        grib_file,
        engine="cfgrib",
        backend_kwargs={"filter_by_keys": {"shortName": short_name}},
    )


def normalize_lon(lon):
    if lon > 180:
        return lon - 360
    return lon


def macro_region(lat, lon):
    lon = normalize_lon(lon)

    if 15 <= lat <= 75 and -170 <= lon <= -50:
        return "North America"
    if -60 <= lat < 15 and -95 <= lon <= -30:
        return "South America"
    if 35 <= lat <= 72 and -15 <= lon <= 40:
        return "Europe"
    if -35 <= lat <= 37 and -20 <= lon <= 55:
        return "Africa / Middle East"
    if 5 <= lat <= 55 and 55 <= lon <= 150:
        return "Asia"
    if -50 <= lat <= 10 and 95 <= lon <= 180:
        return "SE Asia / Australia"
    if 15 <= lat <= 60 and -100 <= lon <= 20:
        return "North Atlantic"
    if 5 <= lat <= 35 and -100 <= lon <= -70:
        return "Caribbean / Gulf"
    if 20 <= lat <= 50 and 120 <= lon <= 160:
        return "NW Pacific"
    return "Other"


def market_mapping(shock_type, lat, lon, value):
    region = macro_region(lat, lon)

    if shock_type == "storm":
        if region in ["Caribbean / Gulf", "North Atlantic", "NW Pacific"]:
            return (
                "Storm / Shipping / Insurance",
                "Offshore energy, shipping, insurance, airlines, cruises",
                "Equity / Shipping"
            )
        if region in ["Europe", "North America"]:
            return (
                "Utilities / Insurance / Logistics",
                "Utilities, insurers, logistics, ports",
                "Equity / Utility"
            )
        return (
            "Storm / Logistics",
            "Shipping, insurers, logistics",
            "Equity / Proxy"
        )

    if shock_type == "heat":
        if region in ["North America", "Europe", "Asia"]:
            return (
                "Power Demand / Gas / Utilities",
                "Utilities, power demand, natural gas, grid stress",
                "Utility / Power"
            )
        return (
            "Heat / Power",
            "Power demand, cooling load, utilities",
            "Utility / Proxy"
        )

    if shock_type == "cold":
        return (
            "Gas / Power / Heating",
            "Natural gas, power generation, heating demand",
            "Commodity / Futures"
        )

    if shock_type == "dry":
        if region in ["North America", "South America"]:
            return (
                "Agriculture / Fertilizer",
                "Grains, oilseeds, fertilizers, agribusiness",
                "Commodity / Futures"
            )
        if region in ["SE Asia / Australia", "Asia"]:
            return (
                "Agriculture / Food Inputs",
                "Palm oil, grains, food inputs",
                "Commodity / Futures"
            )
        return (
            "Agriculture",
            "Crop stress, fertilizers, agribusiness",
            "Commodity / Futures"
        )

    if shock_type == "wet":
        return (
            "Flood / Logistics / Crop Disruption",
            "Logistics, ports, crops, insurers",
            "Logistics / Proxy"
        )

    return (
        "General Weather",
        "Multi-sector weather sensitivity",
        "Mixed"
    )


found = []

for date_str, hh in candidate_00z_runs():
    filename = f"run_{date_str}_{hh:02d}.grib2"
    try:
        client.retrieve(
            date=int(date_str),
            time=hh,
            stream="oper",
            type="fc",
            step=[96],
            param=["2t", "tp", "10u", "10v", "msl"],
            target=filename,
        )
        found.append((date_str, hh, filename))
        if len(found) == 2:
            break
    except Exception:
        continue

if len(found) < 2:
    raise RuntimeError("Could not find two recent ECMWF 00Z runs.")

current_file = found[0][2]
previous_file = found[1][2]

cur_t = open_param_dataset(current_file, "2t")
cur_p = open_param_dataset(current_file, "tp")
cur_u = open_param_dataset(current_file, "10u")
cur_v = open_param_dataset(current_file, "10v")
cur_m = open_param_dataset(current_file, "msl")

prev_t = open_param_dataset(previous_file, "2t")
prev_p = open_param_dataset(previous_file, "tp")
prev_u = open_param_dataset(previous_file, "10u")
prev_v = open_param_dataset(previous_file, "10v")
prev_m = open_param_dataset(previous_file, "msl")


def maybe_step(ds, var_name):
    arr = ds[var_name]
    if "step" in arr.dims:
        return arr.isel(step=0)
    return arr


cur_t2m = maybe_step(cur_t, "t2m") - 273.15
prev_t2m = maybe_step(prev_t, "t2m") - 273.15

cur_tp = maybe_step(cur_p, "tp") * 1000.0
prev_tp = maybe_step(prev_p, "tp") * 1000.0

cur_u10 = maybe_step(cur_u, "u10")
prev_u10 = maybe_step(prev_u, "u10")

cur_v10 = maybe_step(cur_v, "v10")
prev_v10 = maybe_step(prev_v, "v10")

cur_msl = maybe_step(cur_m, "msl") / 100.0
prev_msl = maybe_step(prev_m, "msl") / 100.0

cur_wind = np.sqrt(cur_u10 ** 2 + cur_v10 ** 2)
prev_wind = np.sqrt(prev_u10 ** 2 + prev_v10 ** 2)

temp_delta = (cur_t2m - prev_t2m).values
precip_delta = (cur_tp - prev_tp).values
wind_delta = (cur_wind - prev_wind).values
msl_delta = (cur_msl - prev_msl).values

lats = cur_t2m.latitude.values
lons = cur_t2m.longitude.values

rows = []

# 1. Heat shocks
heat_mask = temp_delta >= 4.0
for i, j in np.argwhere(heat_mask):
    lat = float(lats[i])
    lon = float(lons[j])
    value = float(temp_delta[i, j])
    market, industries, vehicle = market_mapping("heat", lat, lon, value)
    rows.append({
        "type": "heat",
        "lat": round(lat, 2),
        "lon_normal": round(normalize_lon(float(lon)), 2),
        "value": round(value, 2),
        "macro_region": macro_region(lat, lon),
        "Market": market,
        "AffectedIndustries": industries,
        "BestVehicle": vehicle,
        "TradePriority": min(4, max(1, int(np.ceil(value / 2.5))))
    })

# 2. Cold shocks
cold_mask = temp_delta <= -4.0
for i, j in np.argwhere(cold_mask):
    lat = float(lats[i])
    lon = float(lons[j])
    value = float(temp_delta[i, j])
    market, industries, vehicle = market_mapping("cold", lat, lon, value)
    rows.append({
        "type": "cold",
        "lat": round(lat, 2),
        "lon_normal": round(normalize_lon(float(lon)), 2),
        "value": round(value, 2),
        "macro_region": macro_region(lat, lon),
        "Market": market,
        "AffectedIndustries": industries,
        "BestVehicle": vehicle,
        "TradePriority": min(4, max(1, int(np.ceil(abs(value) / 2.5))))
    })

# 3. Wet shocks
wet_mask = precip_delta >= 15.0
for i, j in np.argwhere(wet_mask):
    lat = float(lats[i])
    lon = float(lons[j])
    value = float(precip_delta[i, j])
    market, industries, vehicle = market_mapping("wet", lat, lon, value)
    rows.append({
        "type": "wet",
        "lat": round(lat, 2),
        "lon_normal": round(normalize_lon(float(lon)), 2),
        "value": round(value, 2),
        "macro_region": macro_region(lat, lon),
        "Market": market,
        "AffectedIndustries": industries,
        "BestVehicle": vehicle,
        "TradePriority": min(4, max(1, int(np.ceil(value / 10.0))))
    })

# 4. Dry shocks
dry_mask = precip_delta <= -15.0
for i, j in np.argwhere(dry_mask):
    lat = float(lats[i])
    lon = float(lons[j])
    value = float(precip_delta[i, j])
    market, industries, vehicle = market_mapping("dry", lat, lon, value)
    rows.append({
        "type": "dry",
        "lat": round(lat, 2),
        "lon_normal": round(normalize_lon(float(lon)), 2),
        "value": round(value, 2),
        "macro_region": macro_region(lat, lon),
        "Market": market,
        "AffectedIndustries": industries,
        "BestVehicle": vehicle,
        "TradePriority": min(4, max(1, int(np.ceil(abs(value) / 10.0))))
    })

# 5. Storm shocks
storm_score = np.maximum(wind_delta, 0) * 2.5 + np.maximum(-msl_delta, 0) * 2.0
storm_mask = storm_score >= 10.0
for i, j in np.argwhere(storm_mask):
    lat = float(lats[i])
    lon = float(lons[j])
    value = float(storm_score[i, j])
    market, industries, vehicle = market_mapping("storm", lat, lon, value)
    rows.append({
        "type": "storm",
        "lat": round(lat, 2),
        "lon_normal": round(normalize_lon(float(lon)), 2),
        "value": round(value, 2),
        "macro_region": macro_region(lat, lon),
        "Market": market,
        "AffectedIndustries": industries,
        "BestVehicle": vehicle,
        "TradePriority": min(4, max(1, int(np.ceil(value / 8.0))))
    })

df = pd.DataFrame(rows)

if df.empty:
    df = pd.DataFrame([{
        "type": "none",
        "lat": 0,
        "lon_normal": 0,
        "value": 0,
        "macro_region": "None",
        "Market": "None",
        "AffectedIndustries": "None",
        "BestVehicle": "None",
        "TradePriority": 0
    }])
else:
    df["abs_value"] = df["value"].abs()
    df = (
        df.sort_values(["TradePriority", "abs_value"], ascending=[False, False])
          .drop_duplicates(subset=["type", "macro_region"], keep="first")
          .drop(columns=["abs_value"])
          .head(25)
          .reset_index(drop=True)
    )

df.to_csv("global_shocks.csv", index=False)

print("global_shocks.csv generated")
print(df.head(15).to_string(index=False))
