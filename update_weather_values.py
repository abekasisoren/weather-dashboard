from ecmwf.opendata import Client
from datetime import datetime, timedelta, timezone
import time
import random
import xarray as xr
import pandas as pd
import numpy as np

# ─── Download config ──────────────────────────────────────────────────────────
MIN_RUNS_REQUIRED    = 2          # proceed with at least this many successful downloads
TARGET_RUNS          = 5          # ideal number of historical runs
MAX_RETRIES_PER_SRC  = 2          # attempts per source before trying the next mirror
RETRY_DELAY          = 20         # seconds to wait before a same-source retry
INTER_DOWNLOAD_DELAY = (8, 15)    # random pause between successful downloads

# ECMWF open data is mirrored on 4 independent endpoints.
# Source order: prefer ECMWF's own CDN first (fewest public parallel connections),
# then Azure and Google (separate infrastructure from AWS), AWS last.
# When a source is throttled we immediately switch to the next mirror rather than
# waiting 120 s on the same throttled endpoint (the library's internal retry strategy).
_SOURCES = [
    ("ecmwf",   Client(source="ecmwf")),    # ECMWF's own data.ecmwf.int CDN
    ("azure",   Client(source="azure")),    # Azure Blob Storage mirror
    ("google",  Client(source="google")),   # Google Cloud Storage mirror
    ("aws",     Client(source="aws")),      # AWS S3 (most public traffic → most throttled)
]


def _is_throttle_error(exc: Exception) -> bool:
    """
    Detect rate-limit / service-overload errors regardless of how the HTTP
    client formats the status code.

    ecmwf.opendata (via requests) formats 503 as:
        '503 Server Error: Slow Down for url: ...'
    Raw S3/CDN XML responses contain:
        '<Code>SlowDown</Code>'
    """
    err = str(exc).lower()
    return (
        "slow down" in err               # requests format: "Slow Down"
        or "slowdown" in err             # XML format: "SlowDown"
        or "reduce your request rate" in err
        or ("503" in err and "server error" in err)
        or "429" in err                  # Too Many Requests
        or "service unavailable" in err  # generic 503 text
    )


def _retrieve_one(client_obj: "Client", date_str: str, hh: int, filename: str) -> None:
    """Single retrieve call — raises on any error."""
    client_obj.retrieve(
        date=int(date_str),
        time=hh,
        stream="oper",
        type="fc",
        step=[24, 48, 72, 96, 120],
        param=["2t", "tp", "10u", "10v", "msl"],
        target=filename,
    )


def retrieve_with_backoff(date_str: str, hh: int, filename: str) -> bool:
    """
    Download one ECMWF GRIB file, trying each mirror in turn.

    On throttle / 503: give the same source one quick retry (RETRY_DELAY s),
    then immediately move on to the next mirror.  This avoids the ecmwf
    library's built-in 120 s × 500 attempts loop on the same choked endpoint.

    On non-throttle errors (date not yet published, bad response): skip that
    source immediately and try the next — retrying won't help.

    Returns True on first successful download, False when all sources fail.
    """
    for src_name, client_obj in _SOURCES:
        for attempt in range(MAX_RETRIES_PER_SRC):
            try:
                _retrieve_one(client_obj, date_str, hh, filename)
                print(f"  ✓ {date_str}/{hh:02d}h — {src_name.upper()}")
                return True

            except Exception as exc:
                if _is_throttle_error(exc):
                    if attempt < MAX_RETRIES_PER_SRC - 1:
                        print(
                            f"  [{src_name.upper()}] Throttled {date_str}/{hh:02d}h, "
                            f"quick retry in {RETRY_DELAY}s …"
                        )
                        time.sleep(RETRY_DELAY + random.uniform(0, 5))
                    else:
                        print(
                            f"  [{src_name.upper()}] Still throttled — switching mirror"
                        )
                else:
                    # Not a throttle — date unavailable / parse error; skip this source
                    print(
                        f"  [{src_name.upper()}] {date_str}/{hh:02d}h unavailable: "
                        f"{str(exc)[:120]}"
                    )
                    break  # move to next source immediately

    print(f"  ✗ {date_str}/{hh:02d}h — all 4 mirrors failed, skipping")
    return False


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
    print(f"Fetching ECMWF run {date_str}/{hh:02d}h …")
    if retrieve_with_backoff(date_str, hh, filename):
        found.append((date_str, hh, filename))
        print(f"  ✓ {date_str}/{hh:02d}h — {len(found)}/{TARGET_RUNS} collected")
        if len(found) == TARGET_RUNS:
            break
        # Polite pause between successful downloads to stay under S3 rate limits
        if len(found) < TARGET_RUNS:
            pause = random.uniform(*INTER_DOWNLOAD_DELAY)
            print(f"  Pausing {pause:.1f}s before next download …")
            time.sleep(pause)

if len(found) < MIN_RUNS_REQUIRED:
    raise RuntimeError(
        f"Could not retrieve enough recent ECMWF 00Z runs "
        f"(got {len(found)}, need at least {MIN_RUNS_REQUIRED})."
    )

if len(found) < TARGET_RUNS:
    print(f"⚠️  Only {len(found)} of {TARGET_RUNS} runs available — proceeding with partial history.")

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

# Write previous run if we have at least 2; otherwise copy latest as fallback
if len(history_df) >= 2:
    prev = history_df.iloc[-2].drop(labels=["run_date", "run_time"])
else:
    print("⚠️  Only one run available — using latest as both current and previous.")
    prev = latest
prev_df = pd.DataFrame({"metric": prev.index, "value": prev.values})
prev_df.to_csv("weather_values_prev.csv", index=False)

print(f"weather_values.csv updated from ECMWF (last {len(history_df)} run(s))")
print(latest_df)
print(f"\nSaved weather_history.csv with {len(history_df)} run(s).")
