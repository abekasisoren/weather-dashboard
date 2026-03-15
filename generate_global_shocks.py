import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import psycopg
import xarray as xr
from ecmwf.opendata import Client

DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set.")

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


def maybe_step(ds, var_name):
    arr = ds[var_name]
    if "step" in arr.dims:
        return arr.isel(step=0)
    return arr


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


def market_mapping(shock_type, lat, lon, magnitude):
    region = macro_region(lat, lon)

    if shock_type == "storm":
        if region in ["Caribbean / Gulf"]:
            return {
                "market": "Storm / Offshore Energy / Insurance",
                "affected_industries": "Offshore energy, LNG terminals, insurers, cruise lines, airlines",
                "best_vehicle": "Energy / Insurance Basket",
                "proxy_equities": "SLB, HAL, XOM, CVX, LNG, FLNG, TRV, ALL, HIG",
                "weather_logic": "Wind and pressure signals suggest storm risk intensification.",
                "market_logic": "Storms in the Gulf can disrupt offshore production and raise insurance/travel risk."
            }
        if region in ["North Atlantic", "NW Pacific"]:
            return {
                "market": "Storm / Shipping / Insurance",
                "affected_industries": "Shipping, marine insurers, airlines, coastal logistics",
                "best_vehicle": "Shipping / Insurance Basket",
                "proxy_equities": "STNG, TK, FLNG, ZIM, TRV, ALL, HIG",
                "weather_logic": "Storm intensity metrics are spiking across key ocean routes.",
                "market_logic": "Shipping and insurance are sensitive to rising storm disruption."
            }
        return {
            "market": "Storm / Logistics",
            "affected_industries": "Shipping, insurers, logistics",
            "best_vehicle": "Logistics / Insurance Basket",
            "proxy_equities": "STNG, TK, FLNG, TRV, ALL, HIG",
            "weather_logic": "Storm metrics are rising materially in this region.",
            "market_logic": "Logistics and insurers become sensitive when storm disruption increases."
        }

    if shock_type == "heat":
        return {
            "market": "Heat / Power Demand / Utilities",
            "affected_industries": "Utilities, power demand, natural gas, cooling load",
            "best_vehicle": "Utilities / Power",
            "proxy_equities": "VST, NRG, XLU, DUK, SO, LNG",
            "weather_logic": "Temperature forecasts are moving sharply hotter.",
            "market_logic": "Higher temperatures can lift cooling demand and strain power systems."
        }

    if shock_type == "cold":
        return {
            "market": "Cold / Gas / Power",
            "affected_industries": "Natural gas, heating demand, power generation",
            "best_vehicle": "Natural Gas / Power",
            "proxy_equities": "UNG, LNG, SHEL, BP",
            "weather_logic": "Temperature forecasts are turning sharply colder.",
            "market_logic": "Colder conditions can raise heating demand and energy sensitivity."
        }

    if shock_type == "dry":
        return {
            "market": "Dryness / Agriculture",
            "affected_industries": "Grains, oilseeds, fertilizers, agribusiness",
            "best_vehicle": "Commodity / Agriculture",
            "proxy_equities": "ADM, BG, NTR, MOS, CF",
            "weather_logic": "Rainfall forecasts are collapsing and dryness risk is increasing.",
            "market_logic": "Dryness can tighten crop supply expectations and lift agriculture sensitivity."
        }

    if shock_type == "wet":
        return {
            "market": "Flood / Logistics / Crop Disruption",
            "affected_industries": "Logistics, ports, crops, insurers",
            "best_vehicle": "Logistics / Insurance",
            "proxy_equities": "ZIM, STNG, TK, TRV, ALL, HIG",
            "weather_logic": "Rainfall forecasts are surging and flood risk is building.",
            "market_logic": "Flooding can disrupt logistics, ports, crops and insurance exposure."
        }

    return {
        "market": "General Weather",
        "affected_industries": "Multi-sector weather sensitivity",
        "best_vehicle": "Mixed",
        "proxy_equities": "Mixed",
        "weather_logic": "Material weather anomaly detected.",
        "market_logic": "Weather change could influence multiple sectors."
    }


def score_shock(shock_type, magnitude, region):
    if shock_type == "storm":
        base = min(10.0, magnitude / 12.0)
    elif shock_type in ["heat", "cold"]:
        base = min(10.0, abs(magnitude) / 1.2)
    else:
        base = min(10.0, abs(magnitude) / 5.0)

    market_bonus = 0.0
    if region in ["North America", "South America", "Europe", "North Atlantic", "Caribbean / Gulf"]:
        market_bonus = 1.0
    elif region in ["Asia", "SE Asia / Australia", "NW Pacific"]:
        market_bonus = 0.7

    return round(min(10.0, base + market_bonus), 2)


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

cur_wind = np.sqrt(cur_u10**2 + cur_v10**2)
prev_wind = np.sqrt(prev_u10**2 + prev_v10**2)

temp_delta = (cur_t2m - prev_t2m).values
precip_delta = (cur_tp - prev_tp).values
wind_delta = (cur_wind - prev_wind).values
msl_delta = (cur_msl - prev_msl).values

lats = cur_t2m.latitude.values
lons = cur_t2m.longitude.values

rows = []


def add_row(shock_type, lat, lon, magnitude):
    region = macro_region(lat, lon)
    mapping = market_mapping(shock_type, lat, lon, magnitude)
    score = score_shock(shock_type, magnitude, region)

    if score < 5:
        return

    recommendation = "TRADE" if score >= 8 else "WATCH"

    rows.append({
        "shock_type": shock_type,
        "macro_region": region,
        "lat": round(lat, 2),
        "lon_normal": round(normalize_lon(float(lon)), 2),
        "magnitude": round(float(magnitude), 2),
        "score": score,
        "recommendation": recommendation,
        "market": mapping["market"],
        "affected_industries": mapping["affected_industries"],
        "best_vehicle": mapping["best_vehicle"],
        "proxy_equities": mapping["proxy_equities"],
        "weather_logic": mapping["weather_logic"],
        "market_logic": mapping["market_logic"],
        "updated_at": datetime.now(timezone.utc),
    })


# Heat shocks
for i, j in np.argwhere(temp_delta >= 4.0):
    add_row("heat", float(lats[i]), float(lons[j]), float(temp_delta[i, j]))

# Cold shocks
for i, j in np.argwhere(temp_delta <= -4.0):
    add_row("cold", float(lats[i]), float(lons[j]), float(temp_delta[i, j]))

# Wet shocks
for i, j in np.argwhere(precip_delta >= 15.0):
    add_row("wet", float(lats[i]), float(lons[j]), float(precip_delta[i, j]))

# Dry shocks
for i, j in np.argwhere(precip_delta <= -15.0):
    add_row("dry", float(lats[i]), float(lons[j]), float(precip_delta[i, j]))

# Storm shocks
storm_score = np.maximum(wind_delta, 0) * 2.5 + np.maximum(-msl_delta, 0) * 2.0
for i, j in np.argwhere(storm_score >= 10.0):
    add_row("storm", float(lats[i]), float(lons[j]), float(storm_score[i, j]))

df = pd.DataFrame(rows)

if not df.empty:
    df["abs_mag"] = df["magnitude"].abs()
    df = (
        df.sort_values(["score", "abs_mag"], ascending=[False, False])
        .drop_duplicates(subset=["shock_type", "macro_region"], keep="first")
        .drop(columns=["abs_mag"])
        .head(10)
        .reset_index(drop=True)
    )

with psycopg.connect(DATABASE_URL) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_global_shocks (
                id BIGSERIAL PRIMARY KEY,
                shock_type TEXT NOT NULL,
                macro_region TEXT NOT NULL,
                lat DOUBLE PRECISION NOT NULL,
                lon_normal DOUBLE PRECISION NOT NULL,
                magnitude DOUBLE PRECISION NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                recommendation TEXT NOT NULL,
                market TEXT NOT NULL,
                affected_industries TEXT NOT NULL,
                best_vehicle TEXT NOT NULL,
                proxy_equities TEXT NOT NULL,
                weather_logic TEXT NOT NULL,
                market_logic TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
        """)

        cur.execute("TRUNCATE TABLE weather_global_shocks")

        if not df.empty:
            cur.executemany("""
                INSERT INTO weather_global_shocks (
                    shock_type,
                    macro_region,
                    lat,
                    lon_normal,
                    magnitude,
                    score,
                    recommendation,
                    market,
                    affected_industries,
                    best_vehicle,
                    proxy_equities,
                    weather_logic,
                    market_logic,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, [
                (
                    r["shock_type"],
                    r["macro_region"],
                    float(r["lat"]),
                    float(r["lon_normal"]),
                    float(r["magnitude"]),
                    float(r["score"]),
                    r["recommendation"],
                    r["market"],
                    r["affected_industries"],
                    r["best_vehicle"],
                    r["proxy_equities"],
                    r["weather_logic"],
                    r["market_logic"],
                    r["updated_at"],
                )
                for _, r in df.iterrows()
            ])

        conn.commit()

print("weather_global_shocks table updated in Postgres")
if df.empty:
    print("No global shocks above threshold")
else:
    print(df[["shock_type", "macro_region", "score", "recommendation"]].to_string(index=False))
