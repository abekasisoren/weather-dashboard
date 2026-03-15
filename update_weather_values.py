from ecmwf.opendata import Client
from datetime import datetime, timedelta, timezone
import xarray as xr
import pandas as pd
import numpy as np

client = Client(source="aws")


def candidate_00z_runs():
    now = datetime.now(timezone.utc)
    base = now.replace(hour=0, minute=0, second=0, microsecond=0)
    for days_back in range(0, 8):
        d = base - timedelta(days=days_back)
        yield d.strftime("%Y%m%d"), 0


def open_param_dataset(grib_file, short_name):
    return xr.open_dataset(
        grib_file,
        engine="cfgrib",
        backend_kwargs={"filter_by_keys": {"shortName": short_name}},
    )


def subset_region(ds, lat_min, lat_max, lon_min, lon_max):
    ds2 = ds.sortby("longitude")
    lat_vals = ds2.latitude.values
    if lat_vals[0] > lat_vals[-1]:
        lat_slice = slice(lat_max, lat_min)
    else:
        lat_slice = slice(lat_min, lat_max)
    lon_slice = slice(min(lon_min, lon_max), max(lon_min, lon_max))
    return ds2.sel(latitude=lat_slice, longitude=lat_slice if False else slice(None)).sel(
        longitude=lon_slice
    )


def pick_step_index(ds):
    steps = pd.to_timedelta(ds.step.values)
    target_step = pd.Timedelta(hours=96)
    step = target_step if target_step in steps else steps[0]
    return list(steps).index(step)


regions = {
    "gulf_of_mexico": {"lat_min": 18, "lat_max": 31, "lon_min": -96, "lon_max": -82},
    "us_east_coast": {"lat_min": 30, "lat_max": 41, "lon_min": -82, "lon_max": -70},
    "north_sea": {"lat_min": 51, "lat_max": 61, "lon_min": -4, "lon_max": 9},
    "china_east": {"lat_min": 24, "lat_max": 37, "lon_min": 112, "lon_max": 123},
    "texas_power": {"lat_min": 26, "lat_max": 36, "lon_min": -106, "lon_max": -93},
    "california": {"lat_min": 32, "lat_max": 42, "lon_min": -125, "lon_max": -114},
    "nw_europe": {"lat_min": 45, "lat_max": 56, "lon_min": 0, "lon_max": 15},
    "corn_belt": {"lat_min": 36, "lat_max": 46, "lon_min": -100, "lon_max": -80},
    "us_wheat_plains": {"lat_min": 33, "lat_max": 49, "lon_min": -104, "lon_max": -96},
    "argentina_soy": {"lat_min": -39, "lat_max": -28, "lon_min": -66, "lon_max": -56},
    "black_sea_grain": {"lat_min": 43, "lat_max": 50, "lon_min": 28, "lon_max": 40},
    "brazil_coffee": {"lat_min": -25, "lat_max": -15, "lon_min": -50, "lon_max": -40},
    "west_africa_cocoa": {"lat_min": 4, "lat_max": 10, "lon_min": -8, "lon_max": 2},
    "india_monsoon": {"lat_min": 15, "lat_max": 28, "lon_min": 72, "lon_max": 88},

    # Phase 1 additions
    "canadian_prairies": {"lat_min": 49, "lat_max": 56, "lon_min": -114, "lon_max": -100},
    "mato_grosso": {"lat_min": -17, "lat_max": -8, "lon_min": -61, "lon_max": -51},
    "rhine_corridor": {"lat_min": 47, "lat_max": 53, "lon_min": 5, "lon_max": 10},
    "panama_canal": {"lat_min": 7, "lat_max": 11, "lon_min": -81, "lon_max": -77},
    "sea_palm_oil": {"lat_min": -1, "lat_max": 7, "lon_min": 99, "lon_max": 118},
}


def region_mean_temp_c(ds_t, region_name):
    r = regions[region_name]
    reg = subset_region(ds_t, r["lat_min"], r["lat_max"], r["lon_min"], r["lon_max"])
    idx = pick_step_index(reg)
    vals = (reg["t2m"].isel(step=idx).values - 273.15).flatten()
    vals = vals[~np.isnan(vals)]
    return round(float(np.mean(vals)) if len(vals) else 0.0, 2)


def region_mean_precip_mm(ds_p, region_name):
    r = regions[region_name]
    reg = subset_region(ds_p, r["lat_min"], r["lat_max"], r["lon_min"], r["lon_max"])
    idx = pick_step_index(reg)
    vals = (reg["tp"].isel(step=idx).values * 1000.0).flatten()
    vals = vals[~np.isnan(vals)]
    return round(float(np.mean(vals)) if len(vals) else 0.0, 2)


def region_hotdry_score(ds_t, ds_p, region_name):
    mean_t = region_mean_temp_c(ds_t, region_name)
    mean_p = region_mean_precip_mm(ds_p, region_name)
    score = mean_t - (mean_p / 3.0)
    return round(score, 2)


def region_storm_index(ds_u, ds_v, ds_msl, region_name):
    r = regions[region_name]
    reg_u = subset_region(ds_u, r["lat_min"], r["lat_max"], r["lon_min"], r["lon_max"])
    reg_v = subset_region(ds_v, r["lat_min"], r["lat_max"], r["lon_min"], r["lon_max"])
    reg_m = subset_region(ds_msl, r["lat_min"], r["lat_max"], r["lon_min"], r["lon_max"])

    idx = pick_step_index(reg_u)

    wind = np.sqrt(reg_u["u10"].isel(step=idx).values**2 + reg_v["v10"].isel(step=idx).values**2)
    msl_hpa = reg_m["msl"].isel(step=idx).values / 100.0

    wind_vals = wind.flatten()
    msl_vals = msl_hpa.flatten()

    wind_vals = wind_vals[~np.isnan(wind_vals)]
    msl_vals = msl_vals[~np.isnan(msl_vals)]

    if len(wind_vals) == 0 or len(msl_vals) == 0:
        return 0.0

    p95_wind = float(np.percentile(wind_vals, 95))
    p05_msl = float(np.percentile(msl_vals, 5))

    storm = max(0.0, (p95_wind - 12.0) * 2.5) + max(0.0, (1008.0 - p05_msl) * 2.0)
    return round(storm, 2)


found = []

for date_str, hh in candidate_00z_runs():
    filename = f"run_{date_str}_{hh:02d}.grib2"
    try:
        client.retrieve(
            date=int(date_str),
            time=hh,
            stream="oper",
            type="fc",
            step=[24, 48, 72, 96, 120],
            param=["2t", "tp", "10u", "10v", "msl"],
            target=filename,
        )
        found.append((date_str, hh, filename))
        if len(found) == 5:
            break
    except Exception:
        continue

if len(found) < 5:
    raise RuntimeError("Could not find five recent ECMWF 00Z runs.")

run_rows = []

for date_str, hh, grib_file in reversed(found):
    ds_t = open_param_dataset(grib_file, "2t")
    ds_p = open_param_dataset(grib_file, "tp")
    ds_u = open_param_dataset(grib_file, "10u")
    ds_v = open_param_dataset(grib_file, "10v")
    ds_msl = open_param_dataset(grib_file, "msl")

    row = {
        "run_date": date_str,
        "run_time": hh,

        "gulf_storm_index": region_storm_index(ds_u, ds_v, ds_msl, "gulf_of_mexico"),
        "us_east_coast_storm_index": region_storm_index(ds_u, ds_v, ds_msl, "us_east_coast"),
        "north_sea_storm_index": region_storm_index(ds_u, ds_v, ds_msl, "north_sea"),
        "china_east_storm_index": region_storm_index(ds_u, ds_v, ds_msl, "china_east"),

        "texas_mean_temp_c": region_mean_temp_c(ds_t, "texas_power"),
        "california_mean_temp_c": region_mean_temp_c(ds_t, "california"),
        "nw_europe_mean_temp_c": region_mean_temp_c(ds_t, "nw_europe"),

        "cornbelt_hotdry_score": region_hotdry_score(ds_t, ds_p, "corn_belt"),
        "us_wheat_hotdry_score": region_hotdry_score(ds_t, ds_p, "us_wheat_plains"),
        "argentina_soy_hotdry_score": region_hotdry_score(ds_t, ds_p, "argentina_soy"),
        "black_sea_hotdry_score": region_hotdry_score(ds_t, ds_p, "black_sea_grain"),

        "brazil_coffee_precip_mm": region_mean_precip_mm(ds_p, "brazil_coffee"),
        "west_africa_cocoa_precip_mm": region_mean_precip_mm(ds_p, "west_africa_cocoa"),
        "india_monsoon_precip_mm": region_mean_precip_mm(ds_p, "india_monsoon"),

        # Phase 1 additions
        "canadian_prairies_hotdry_score": region_hotdry_score(ds_t, ds_p, "canadian_prairies"),
        "mato_grosso_hotdry_score": region_hotdry_score(ds_t, ds_p, "mato_grosso"),
        "rhine_corridor_precip_mm": region_mean_precip_mm(ds_p, "rhine_corridor"),
        "rhine_corridor_storm_index": region_storm_index(ds_u, ds_v, ds_msl, "rhine_corridor"),
        "panama_canal_precip_mm": region_mean_precip_mm(ds_p, "panama_canal"),
        "sea_palm_oil_precip_mm": region_mean_precip_mm(ds_p, "sea_palm_oil"),
        "sea_palm_oil_hotdry_score": region_hotdry_score(ds_t, ds_p, "sea_palm_oil"),
    }

    run_rows.append(row)

history_df = pd.DataFrame(run_rows)
history_df.to_csv("weather_history.csv", index=False)

latest = history_df.iloc[-1].drop(labels=["run_date", "run_time"])
latest_df = pd.DataFrame({"metric": latest.index, "value": latest.values})
latest_df.to_csv("weather_values.csv", index=False)

prev = history_df.iloc[-2].drop(labels=["run_date", "run_time"])
prev_df = pd.DataFrame({"metric": prev.index, "value": prev.values})
prev_df.to_csv("weather_values_prev.csv", index=False)

print("weather_values.csv updated from ECMWF last-5-runs model")
print(latest_df)
print("\nSaved weather_history.csv with last 5 runs.")
