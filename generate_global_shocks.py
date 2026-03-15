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


def market_mapping(shock_type, lat, lon):
    region = macro_region(lat, lon)

    if shock_type == "storm":
        if region in ["Caribbean / Gulf"]:
            return {
                "affected_market": "Offshore energy, LNG, insurance, airlines, cruises",
                "best_vehicle": "Energy / insurance basket",
                "proxy_equities": "SLB, HAL, XOM, CVX, LNG, FLNG, TRV, ALL, HIG",
                "why_it_matters": "A strengthening storm setup here can become a mainstream energy and insurance story quickly."
            }
        if region in ["North Atlantic", "NW Pacific"]:
            return {
                "affected_market": "Shipping, marine insurers, airlines",
                "best_vehicle": "Shipping / insurance basket",
                "proxy_equities": "STNG, TK, FLNG, ZIM, TRV, ALL, HIG",
                "why_it_matters": "Storm intensification on major ocean routes can become a freight and insurance story."
            }
        return {
            "affected_market": "Logistics, shipping, insurers",
            "best_vehicle": "Logistics / insurance basket",
            "proxy_equities": "STNG, TK, TRV, ALL, HIG",
            "why_it_matters": "A stronger storm setup can disrupt logistics and raise insurance sensitivity."
        }

    if shock_type == "dry":
        return {
            "affected_market": "Agriculture, fertilizers, agribusiness",
            "best_vehicle": "Commodity / agriculture",
            "proxy_equities": "ADM, BG, NTR, MOS, CF",
            "why_it_matters": "A drying weather anomaly can become a crop-stress story if it persists."
        }

    if shock_type == "wet":
        return {
            "affected_market": "Logistics, ports, crops, insurers",
            "best_vehicle": "Logistics / insurance",
            "proxy_equities": "ZIM, STNG, TK, TRV, ALL, HIG",
            "why_it_matters": "A wet anomaly can become a flood and logistics disruption story."
        }

    if shock_type == "heat":
        return {
            "affected_market": "Utilities, power demand, natural gas",
            "best_vehicle": "Utilities / power",
            "proxy_equities": "VST, NRG, XLU, DUK, SO, LNG",
            "why_it_matters": "A hotter anomaly can become a power-demand story."
        }

    if shock_type == "cold":
        return {
            "affected_market": "Natural gas, power generation, heating demand",
            "best_vehicle": "Natural gas / power",
            "proxy_equities": "UNG, LNG, SHEL, BP",
            "why_it_matters": "A colder anomaly can become a heating-demand and energy story."
        }

    return {
        "affected_market": "General weather-sensitive sectors",
        "best_vehicle": "Mixed",
        "proxy_equities": "Mixed",
        "why_it_matters": "This weather anomaly may matter if it persists."
    }


def signal_level(change_material: bool, outcome_material: bool):
    if change_material and outcome_material:
        return "HIGH CONVICTION", "ACT"
    if change_material or outcome_material:
        if change_material:
            return "EARLY SIGNAL", "WATCH"
        return "ACTIONABLE", "PREPARE"
    return None, None


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


def add_cluster_signal(shock_type, lat, lon, magnitude, change_material, outcome_material):
    level, recommendation = signal_level(change_material, outcome_material)
    if level is None:
        return

    region = macro_region(lat, lon)
    mapping = market_mapping(shock_type, lat, lon)

    rows.append({
        "macro_region": region,
        "shock_type": shock_type.upper(),
        "signal_level": level,
        "recommendation": recommendation,
        "magnitude": round(float(magnitude), 2),
        "what_changed": f"{shock_type.upper()} anomaly detected with magnitude {float(magnitude):.2f}.",
        "why_it_matters": mapping["why_it_matters"],
        "affected_market": mapping["affected_market"],
        "best_vehicle": mapping["best_vehicle"],
        "proxy_equities": mapping["proxy_equities"],
        "updated_at": datetime.now(timezone.utc),
    })


# Representative cluster logic: one signal per shock type per macro region
seen = set()

def process_grid(shock_type, indices, magnitude_array):
    for i, j in indices:
        lat = float(lats[i])
        lon = float(lons[j])
        region = macro_region(lat, lon)
        key = (shock_type, region)

        if key in seen:
            continue

        magnitude = float(magnitude_array[i, j])

        if shock_type in ["dry", "wet"]:
            change_material = abs(magnitude) >= 15.0
            outcome_material = abs(magnitude) >= 25.0
        elif shock_type in ["heat", "cold"]:
            change_material = abs(magnitude) >= 4.0
            outcome_material = abs(magnitude) >= 6.0
        else:
            change_material = abs(magnitude) >= 10.0
            outcome_material = abs(magnitude) >= 20.0

        add_cluster_signal(shock_type, lat, lon, magnitude, change_material, outcome_material)
        seen.add(key)

# Heat
process_grid("heat", np.argwhere(temp_delta >= 4.0), temp_delta)

# Cold
process_grid("cold", np.argwhere(temp_delta <= -4.0), temp_delta)

# Wet
process_grid("wet", np.argwhere(precip_delta >= 15.0), precip_delta)

# Dry
process_grid("dry", np.argwhere(precip_delta <= -15.0), precip_delta)

# Storm
storm_score = np.maximum(wind_delta, 0) * 2.5 + np.maximum(-msl_delta, 0) * 2.0
process_grid("storm", np.argwhere(storm_score >= 10.0), storm_score)

df = pd.DataFrame(rows)

if not df.empty:
    level_rank = {
        "HIGH CONVICTION": 0,
        "ACTIONABLE": 1,
        "EARLY SIGNAL": 2,
    }
    df["level_rank"] = df["signal_level"].map(level_rank)
    df = (
        df.sort_values(by=["level_rank", "macro_region", "shock_type"])
        .drop(columns=["level_rank"])
        .head(12)
        .reset_index(drop=True)
    )

with psycopg.connect(DATABASE_URL) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_global_shocks (
                id BIGSERIAL PRIMARY KEY,
                macro_region TEXT NOT NULL,
                shock_type TEXT NOT NULL,
                signal_level TEXT NOT NULL,
                recommendation TEXT NOT NULL,
                magnitude DOUBLE PRECISION NOT NULL,
                what_changed TEXT NOT NULL,
                why_it_matters TEXT NOT NULL,
                affected_market TEXT NOT NULL,
                best_vehicle TEXT NOT NULL,
                proxy_equities TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
        """)

        cur.execute("TRUNCATE TABLE weather_global_shocks")

        if not df.empty:
            cur.executemany("""
                INSERT INTO weather_global_shocks (
                    macro_region,
                    shock_type,
                    signal_level,
                    recommendation,
                    magnitude,
                    what_changed,
                    why_it_matters,
                    affected_market,
                    best_vehicle,
                    proxy_equities,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, [
                (
                    r["macro_region"],
                    r["shock_type"],
                    r["signal_level"],
                    r["recommendation"],
                    float(r["magnitude"]),
                    r["what_changed"],
                    r["why_it_matters"],
                    r["affected_market"],
                    r["best_vehicle"],
                    r["proxy_equities"],
                    r["updated_at"],
                )
                for _, r in df.iterrows()
            ])

        conn.commit()

print("weather_global_shocks table updated in Postgres")
if df.empty:
    print("No global weather radar items this run")
else:
    print(df[[
        "macro_region",
        "shock_type",
        "signal_level",
        "recommendation"
    ]].to_string(index=False))
