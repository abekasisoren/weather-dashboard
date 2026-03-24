#!/usr/bin/env bash
# Weather signal pipeline — runs every 12h via Render cron
# 1. Download latest ECMWF Open Data GRIB2 files
# 2. Generate quick regional signals from CSV
# 3. Generate full anomaly signals from GRIB
# 4. Clean up large GRIB files to save disk space
set -e
echo "[weather-pipeline] Step 1: downloading ECMWF GRIB data..."
python update_weather_values.py
echo "[weather-pipeline] Step 2: generating CSV-based signals..."
python generate_signals.py
echo "[weather-pipeline] Step 3: generating GRIB anomaly signals..."
python generate_global_shocks.py
echo "[weather-pipeline] Step 4: cleaning up GRIB files..."
rm -f *.grib2
echo "[weather-pipeline] Done."
