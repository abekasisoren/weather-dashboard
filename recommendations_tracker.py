"""
recommendations_tracker.py — Log and track Global Pulse Trader recommendations.

Stores each recommendation to the `recommendations_log` DB table with an entry
price fetched at logging time.  Performance is evaluated at T+3, T+5, T+7, and
T+10 business days (not same-day) using Yahoo Finance historical data, benchmarked
against SPY to compute market-relative alpha.

T+3/T+5 capture short-term momentum; T+7/T+10 capture the slower media-driven
weather signal that typically takes 1–2 weeks to fully price in.

Usage:
    from recommendations_tracker import ensure_schema, log_recommendations, get_aftermath_table
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd
import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL")

# ─── Position management thresholds ──────────────────────────────────────────
# These can be overridden from the dashboard sidebar via close_positions_stop_loss()
STOP_LOSS_PCT   = 5.0   # close Long if price drops 5%+ below entry; Short if rises 5%+
TAKE_PROFIT_PCT = 10.0  # close Long if price rises 10%+; Short if drops 10%+


# ─── Schema ───────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS recommendations_log (
    id              SERIAL PRIMARY KEY,
    logged_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    signal_date     TEXT,
    stock_symbol    TEXT NOT NULL,
    trade           TEXT NOT NULL,
    entry_price     DOUBLE PRECISION,
    region          TEXT,
    anomaly         TEXT,
    commodity       TEXT,
    final_trade_score DOUBLE PRECISION,
    conviction      TEXT,
    why_it_matters  TEXT
);
"""

# Schema migrations — safe to run multiple times (ADD COLUMN IF NOT EXISTS)
_MIGRATE_STMTS = [
    # T+3/T+5/T+7/T+10 snapshot columns
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS spy_entry      DOUBLE PRECISION",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS price_t3       DOUBLE PRECISION",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS price_t5       DOUBLE PRECISION",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS spy_t3         DOUBLE PRECISION",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS spy_t5         DOUBLE PRECISION",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS price_t7       DOUBLE PRECISION",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS price_t10      DOUBLE PRECISION",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS spy_t7         DOUBLE PRECISION",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS spy_t10        DOUBLE PRECISION",
    # ML feature snapshot columns — values captured at logging time
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS sigma_score     FLOAT",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS seasonality_sc  FLOAT",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS trend_dir       TEXT",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS confluence_bonus FLOAT",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS pheno_mult      FLOAT",
    # Media-triggered close columns — populated when media confirms the weather event
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS closed_at        TIMESTAMPTZ",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS exit_price        DOUBLE PRECISION",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS spy_exit_price    DOUBLE PRECISION",
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS close_reason      TEXT",
    # Signal source: 'weather' (default) or 'mining'
    "ALTER TABLE recommendations_log ADD COLUMN IF NOT EXISTS source            TEXT DEFAULT 'weather'",
]


def ensure_schema() -> None:
    """Create recommendations_log table if it does not exist, then migrate columns."""
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            for stmt in _MIGRATE_STMTS:
                cur.execute(stmt)
        conn.commit()


# ─── Live price fetching (Finnhub) ────────────────────────────────────────────

_fetch_errors: list[str] = []


def get_fetch_errors() -> list[str]:
    """Return errors captured during the last fetch_prices() call."""
    return list(_fetch_errors)


def _finnhub_price(symbol: str, api_key: str) -> Optional[float]:
    """Fetch current ~15-min-delayed price from Finnhub."""
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={api_key}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode())
    price = data.get("c")
    if price and float(price) > 0:
        return round(float(price), 4)
    return None


def fetch_prices(symbols: list[str]) -> dict[str, Optional[float]]:
    """
    Fetch latest prices for a list of symbols via Finnhub.
    Requires FINNHUB_API_KEY env var (free at finnhub.io).
    """
    global _fetch_errors
    _fetch_errors = []

    prices: dict[str, Optional[float]] = {s: None for s in symbols}
    if not symbols:
        return prices

    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        _fetch_errors.append(
            "⚠️ FINNHUB_API_KEY not set. "
            "Get a free key at https://finnhub.io and add it as a Render env var."
        )
        return prices

    _fetch_errors.append(f"✅ Finnhub key found, fetching {len(symbols)} symbols…")
    for symbol in symbols:
        try:
            price = _finnhub_price(symbol, api_key)
            if price:
                prices[symbol] = price
                _fetch_errors.append(f"✅ {symbol}: ${price}")
            else:
                _fetch_errors.append(f"⛔ {symbol}: Finnhub returned 0 / no price")
        except Exception as e:
            _fetch_errors.append(f"❌ {symbol}: {e}")

    return prices


# ─── Historical price fetching (Yahoo Finance, for T+3 / T+5 snapshots) ───────


def next_trading_day(d: date) -> date:
    """Return the next calendar day that is a weekday (Mon–Fri)."""
    d = d + timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d = d + timedelta(days=1)
    return d


def get_historical_open(symbol: str, target_date: date) -> Optional[float]:
    """
    Fetch the open price for `symbol` on `target_date` (or the nearest trading day
    within +7 days) using Finnhub /stock/candle endpoint.
    Requires FINNHUB_API_KEY env var.
    """
    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        return None
    ts_from = int(datetime.combine(target_date, datetime.min.time()).timestamp())
    ts_to   = int(datetime.combine(target_date + timedelta(days=7), datetime.min.time()).timestamp())
    url = (
        f"https://finnhub.io/api/v1/stock/candle"
        f"?symbol={symbol}&resolution=D"
        f"&from={ts_from}&to={ts_to}&token={api_key}"
    )
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        if data.get("s") == "ok" and data.get("o"):
            return round(float(data["o"][0]), 4)
    except Exception:
        pass
    return None


def _business_days_after(start, n: int) -> date:
    """Return the date that is n business days after `start` (datetime or date)."""
    d = start.date() if hasattr(start, "date") else start
    result = np.busday_offset(d.isoformat(), n, roll="forward")
    return result.astype("O")  # converts numpy datetime64 → datetime.date


def _yahoo_history_range(symbol: str, start_d: date, end_d: date) -> dict[date, float]:
    """
    Fetch daily closing prices from Yahoo Finance chart API (no library needed).
    Returns {date: close_price} dict for the requested range.
    """
    start_ts = int(datetime.combine(start_d, datetime.min.time()).timestamp())
    end_ts   = int(datetime.combine(end_d + timedelta(days=1), datetime.min.time()).timestamp())

    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        f"?interval=1d&period1={start_ts}&period2={end_ts}"
    )
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; WeatherTrader/1.0)",
        "Accept":     "application/json",
    })

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except Exception:
        return {}

    try:
        result = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        closes     = result["indicators"]["quote"][0]["close"]
    except (KeyError, IndexError, TypeError):
        return {}

    history: dict[date, float] = {}
    for ts, close in zip(timestamps, closes):
        if close is not None and float(close) > 0:
            d = datetime.fromtimestamp(ts, tz=timezone.utc).date()
            history[d] = round(float(close), 4)

    return history


def _closest_price_on_or_before(history: dict[date, float], target: date) -> Optional[float]:
    """Return the price on target date or the nearest prior trading day."""
    past = [d for d in history if d <= target]
    if not past:
        return None
    return history[max(past)]


def _maybe_persist_snapshots(df: pd.DataFrame) -> None:
    """
    For rows where T+3/T+5/T+7/T+10 dates have arrived and prices are still NULL,
    fetch historical closing prices from Yahoo Finance and persist them to DB.
    Also captures SPY prices for alpha computation.
    Mutates `df` in-place.
    """
    if not DATABASE_URL:
        return

    today = datetime.now(timezone.utc).date()

    horizons = [
        (3,  "price_t3",  "spy_t3"),
        (5,  "price_t5",  "spy_t5"),
        (7,  "price_t7",  "spy_t7"),
        (10, "price_t10", "spy_t10"),
    ]

    # Collect rows that need snapshots for each horizon
    needs: dict[int, pd.DataFrame] = {}
    for n, price_col, _ in horizons:
        if price_col not in df.columns:
            df[price_col] = None
        col_mask = df[price_col].isna() & df["logged_at"].apply(
            lambda x: _business_days_after(x, n) <= today
        )
        needs[n] = df[col_mask]

    if all(v.empty for v in needs.values()):
        return

    # ── Build per-symbol date ranges to fetch ─────────────────────────────────
    fetch_ranges: dict[str, tuple[date, date]] = {}

    def _extend(sym: str, d: date) -> None:
        lo, hi = fetch_ranges.get(sym, (d, d))
        fetch_ranges[sym] = (min(lo, d), max(hi, d))

    for n, _, _ in horizons:
        for _, row in needs[n].iterrows():
            d = _business_days_after(row["logged_at"], n)
            _extend(row["stock_symbol"], d)
            _extend("SPY", d)

    # ── One Yahoo Finance call per symbol ─────────────────────────────────────
    histories: dict[str, dict[date, float]] = {}
    for sym, (lo, hi) in fetch_ranges.items():
        histories[sym] = _yahoo_history_range(sym, lo - timedelta(days=5), hi + timedelta(days=2))

    # ── Populate df and collect DB updates ────────────────────────────────────
    db_updates: list[tuple] = []  # (col, val, id)

    for n, price_col, spy_col in horizons:
        if spy_col not in df.columns:
            df[spy_col] = None
        for idx, row in needs[n].iterrows():
            target_date = _business_days_after(row["logged_at"], n)
            price = _closest_price_on_or_before(histories.get(row["stock_symbol"], {}), target_date)
            spy_p = _closest_price_on_or_before(histories.get("SPY", {}), target_date)
            if price is not None:
                df.at[idx, price_col] = price
                db_updates.append((price_col, price, int(row["id"])))
            if spy_p is not None:
                df.at[idx, spy_col] = spy_p
                db_updates.append((spy_col, spy_p, int(row["id"])))

    # ── Persist to DB ─────────────────────────────────────────────────────────
    if db_updates:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                for col, val, row_id in db_updates:
                    cur.execute(
                        f"UPDATE recommendations_log SET {col} = %s WHERE id = %s",
                        (val, row_id),
                    )
            conn.commit()


# ─── Region + anomaly family maps for media-triggered close ──────────────────
# The DB stores events at fine-grained (region, anomaly_type) level, but
# recommendations are logged at a potentially different grain. Family maps
# let "Brazil Center-South / heavy_rain" close "Brazil / flood_risk" trades.

_CLOSE_REGION_FAMILY: dict[str, frozenset] = {
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
}

_CLOSE_ANOMALY_FAMILY: dict[str, frozenset] = {
    # Wet / flood cluster
    "heavy_rain":        frozenset({"heavy_rain", "flood_risk", "flood", "atmospheric_river"}),
    "flood_risk":        frozenset({"flood_risk", "heavy_rain", "flood", "atmospheric_river"}),
    "flood":             frozenset({"flood", "flood_risk", "heavy_rain"}),
    "atmospheric_river": frozenset({"atmospheric_river", "heavy_rain", "flood_risk"}),
    # Heat / fire cluster
    "heatwave":          frozenset({"heatwave", "extreme_heat", "wildfire_risk", "drought"}),
    "extreme_heat":      frozenset({"extreme_heat", "heatwave", "wildfire_risk"}),
    "wildfire_risk":     frozenset({"wildfire_risk", "wildfire", "heatwave", "extreme_heat"}),
    "wildfire":          frozenset({"wildfire", "wildfire_risk"}),
    # Cold cluster
    "cold_wave":         frozenset({"cold_wave", "polar_vortex", "frost", "ice_storm"}),
    "polar_vortex":      frozenset({"polar_vortex", "cold_wave", "frost", "extreme_wind"}),
    "frost":             frozenset({"frost", "cold_wave", "ice_storm"}),
    "ice_storm":         frozenset({"ice_storm", "frost", "cold_wave"}),
    # Dry cluster
    "drought":           frozenset({"drought", "heatwave", "monsoon_failure"}),
    "monsoon_failure":   frozenset({"monsoon_failure", "drought"}),
    # Wind cluster
    "storm_wind":        frozenset({"storm_wind", "hurricane_risk", "extreme_wind"}),
    "hurricane_risk":    frozenset({"hurricane_risk", "hurricane", "storm_wind"}),
    "hurricane":         frozenset({"hurricane", "hurricane_risk", "storm_wind"}),
    "extreme_wind":      frozenset({"extreme_wind", "storm_wind", "hurricane_risk"}),
}


# ─── Media-triggered position close ──────────────────────────────────────────

def close_positions_from_media() -> int:
    """
    For any open recommendation whose underlying weather event's REGION has been
    confirmed by media, mark the position as CLOSED.

    Uses REGION FAMILY matching only: any confirmed anomaly in a region family closes
    ALL recommendations for that entire region family — regardless of anomaly type.
    This mirrors the Radar filter logic exactly.

    Exit price: Yahoo Finance historical close on the media_pickup_at date (best effort).
    If the price cannot be fetched (weekend / holiday / rate-limit), the position is
    still marked Closed with exit_price = NULL — the UI will show "—" for that P&L.

    Sets: closed_at, exit_price, spy_exit_price, close_reason = 'media_confirmed'
    Returns number of positions newly closed.
    """
    if not DATABASE_URL:
        return 0

    # ── Step 1: Fetch all confirmed regions with their earliest pickup date ─────
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT region, MIN(media_pickup_at)::date AS pickup_date
                    FROM weather_global_shocks
                    WHERE media_validated = TRUE
                      AND media_pickup_at IS NOT NULL
                    GROUP BY region
                    """
                )
                confirmed_regions_raw = cur.fetchall()
    except Exception as e:
        print(f"[close_positions_from_media] DB error step 1: {e}")
        return 0

    if not confirmed_regions_raw:
        print("[close_positions_from_media] No media-confirmed regions found.")
        return 0

    # ── Step 2: Expand each region to its family, keep earliest pickup date ─────
    # confirmed_families: { frozenset(region_siblings) → earliest pickup_date }
    confirmed_families: dict[frozenset, date] = {}
    for region, pickup_date in confirmed_regions_raw:
        r_family = _CLOSE_REGION_FAMILY.get(region, frozenset({region}))
        if r_family not in confirmed_families or pickup_date < confirmed_families[r_family]:
            confirmed_families[r_family] = pickup_date

    print(
        f"[close_positions_from_media] {len(confirmed_regions_raw)} confirmed region(s) "
        f"→ {len(confirmed_families)} family group(s)"
    )

    # ── Step 3: Find open recommendations in confirmed region families ──────────
    to_close: list[tuple] = []  # (rec_id, symbol, trade, entry_p, spy_entry, pickup_date)
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                for r_family, pickup_date in confirmed_families.items():
                    region_list = sorted(r_family)
                    print(
                        f"[close_positions_from_media] Searching regions {region_list} "
                        f"logged <= {pickup_date}"
                    )
                    cur.execute(
                        """
                        SELECT id, stock_symbol, trade, entry_price, spy_entry
                        FROM recommendations_log
                        WHERE region     = ANY(%s)
                          AND closed_at  IS NULL
                          AND logged_at::date <= %s
                        """,
                        (region_list, pickup_date),
                    )
                    rows = cur.fetchall()
                    print(
                        f"[close_positions_from_media]   → {len(rows)} open position(s) found"
                    )
                    for row in rows:
                        to_close.append((*row, pickup_date))
    except Exception as e:
        print(f"[close_positions_from_media] DB error step 3: {e}")
        return 0

    if not to_close:
        print("[close_positions_from_media] Nothing to close.")
        return 0

    # ── Step 4: Fetch Yahoo Finance prices at pickup date (best effort) ─────────
    fetch_ranges: dict[str, tuple[date, date]] = {}

    def _ext(sym: str, d: date) -> None:
        lo, hi = fetch_ranges.get(sym, (d, d))
        fetch_ranges[sym] = (min(lo, d), max(hi, d))

    for rec_id, symbol, trade, entry_p, spy_entry, pickup_date in to_close:
        _ext(symbol, pickup_date)
        _ext("SPY", pickup_date)

    histories: dict[str, dict[date, float]] = {}
    for sym, (lo, hi) in fetch_ranges.items():
        histories[sym] = _yahoo_history_range(sym, lo - timedelta(days=5), hi + timedelta(days=2))

    # ── Step 5: Write close record — always, even if exit price is unavailable ──
    # The position MUST be marked closed regardless of price availability.
    # If price is None, exit_price stays NULL and P&L will display "—".
    closed_count = 0
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                for rec_id, symbol, trade, entry_p, spy_entry, pickup_date in to_close:
                    exit_p   = _closest_price_on_or_before(histories.get(symbol, {}), pickup_date)
                    spy_exit = _closest_price_on_or_before(histories.get("SPY",   {}), pickup_date)

                    closed_ts = datetime.combine(pickup_date, datetime.min.time()).replace(
                        tzinfo=timezone.utc
                    )
                    cur.execute(
                        """
                        UPDATE recommendations_log
                        SET closed_at      = %s,
                            exit_price     = %s,
                            spy_exit_price = %s,
                            close_reason   = 'media_confirmed'
                        WHERE id = %s AND closed_at IS NULL
                        """,
                        (closed_ts, exit_p, spy_exit, rec_id),
                    )
                    price_str = f"${exit_p:.2f}" if exit_p else "N/A (no price data)"
                    print(
                        f"[close_positions_from_media]   ✓ Closed rec #{rec_id} "
                        f"{symbol} exit={price_str} pickup={pickup_date}"
                    )
                    closed_count += 1
            conn.commit()
    except Exception as e:
        print(f"[close_positions_from_media] DB error step 5: {e}")

    print(f"[close_positions_from_media] Done — {closed_count} position(s) closed.")
    return closed_count


# ─── Weather-resolution close ─────────────────────────────────────────────────

def close_positions_weather_resolved(min_recovering_runs: int = 2) -> int:
    """
    Close open positions whose underlying weather event is measurably fading in
    the GRIB data — BEFORE media has a chance to report it.

    Detection: the most recent `min_recovering_runs` GRIB rows for a
    (region, anomaly_type) pair all carry trend_direction = 'recovering'.
    Two consecutive recovering runs confirms the event is genuinely retreating,
    not a single-model wobble.

    Uses REGION + ANOMALY family matching so "Brazil / heavy_rain recovering"
    closes "Brazil Center-South / flood_risk" positions.

    close_reason = 'weather_resolved'
    Returns number of positions newly closed.
    """
    if not DATABASE_URL:
        return 0

    # ── Step 1: Find events with N consecutive recovering GRIB runs ────────────
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH ranked AS (
                        SELECT
                            region,
                            anomaly_type,
                            trend_direction,
                            created_at,
                            ROW_NUMBER() OVER (
                                PARTITION BY region, anomaly_type
                                ORDER BY created_at DESC
                            ) AS rn
                        FROM weather_global_shocks
                        WHERE trend_direction IS NOT NULL
                    ),
                    recent AS (
                        SELECT
                            region,
                            anomaly_type,
                            MAX(created_at) AS latest_at,
                            COUNT(*) FILTER (WHERE trend_direction = 'recovering') AS recovering_count,
                            COUNT(*)                                               AS run_count
                        FROM ranked
                        WHERE rn <= %s
                        GROUP BY region, anomaly_type
                    )
                    SELECT region, anomaly_type, latest_at::date AS resolved_date
                    FROM recent
                    WHERE recovering_count >= %s
                      AND run_count        >= %s
                    """,
                    (min_recovering_runs, min_recovering_runs, min_recovering_runs),
                )
                resolved_events = cur.fetchall()
    except Exception as e:
        print(f"[close_positions_weather_resolved] DB error step 1: {e}")
        return 0

    if not resolved_events:
        print("[close_positions_weather_resolved] No resolving events found.")
        return 0

    print(
        f"[close_positions_weather_resolved] {len(resolved_events)} resolving event(s) detected"
    )

    # ── Step 2: Expand to region+anomaly families, find open recommendations ───
    to_close: list[tuple] = []
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                for region, anomaly_type, resolved_date in resolved_events:
                    r_family = _CLOSE_REGION_FAMILY.get(region, frozenset({region}))
                    a_family = _CLOSE_ANOMALY_FAMILY.get(anomaly_type, frozenset({anomaly_type}))
                    region_list  = sorted(r_family)
                    anomaly_list = sorted(a_family)

                    print(
                        f"[close_positions_weather_resolved] {region}/{anomaly_type} "
                        f"resolving → searching {region_list} × {anomaly_list}"
                    )

                    cur.execute(
                        """
                        SELECT id, stock_symbol, trade, entry_price, spy_entry
                        FROM recommendations_log
                        WHERE region    = ANY(%s)
                          AND anomaly   = ANY(%s)
                          AND closed_at IS NULL
                          AND logged_at::date <= %s
                        """,
                        (region_list, anomaly_list, resolved_date),
                    )
                    rows = cur.fetchall()
                    print(
                        f"[close_positions_weather_resolved]   → {len(rows)} position(s) to close"
                    )
                    for row in rows:
                        to_close.append((*row, resolved_date))
    except Exception as e:
        print(f"[close_positions_weather_resolved] DB error step 2: {e}")
        return 0

    if not to_close:
        print("[close_positions_weather_resolved] Nothing to close.")
        return 0

    # ── Step 3: Fetch Yahoo Finance prices at resolved_date (best effort) ──────
    fetch_ranges: dict[str, tuple[date, date]] = {}

    def _ext(sym: str, d: date) -> None:
        lo, hi = fetch_ranges.get(sym, (d, d))
        fetch_ranges[sym] = (min(lo, d), max(hi, d))

    for rec_id, symbol, trade, entry_p, spy_entry, resolved_date in to_close:
        _ext(symbol, resolved_date)
        _ext("SPY",  resolved_date)

    histories: dict[str, dict[date, float]] = {}
    for sym, (lo, hi) in fetch_ranges.items():
        histories[sym] = _yahoo_history_range(sym, lo - timedelta(days=5), hi + timedelta(days=2))

    # ── Step 4: Write close records (always, even if price unavailable) ─────────
    closed_count = 0
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                for rec_id, symbol, trade, entry_p, spy_entry, resolved_date in to_close:
                    exit_p   = _closest_price_on_or_before(histories.get(symbol, {}), resolved_date)
                    spy_exit = _closest_price_on_or_before(histories.get("SPY",   {}), resolved_date)

                    closed_ts = datetime.combine(resolved_date, datetime.min.time()).replace(
                        tzinfo=timezone.utc
                    )
                    cur.execute(
                        """
                        UPDATE recommendations_log
                        SET closed_at      = %s,
                            exit_price     = %s,
                            spy_exit_price = %s,
                            close_reason   = 'weather_resolved'
                        WHERE id = %s AND closed_at IS NULL
                        """,
                        (closed_ts, exit_p, spy_exit, rec_id),
                    )
                    price_str = f"${exit_p:.2f}" if exit_p else "N/A"
                    print(
                        f"[close_positions_weather_resolved]   ✓ Closed #{rec_id} "
                        f"{symbol} exit={price_str} resolved={resolved_date}"
                    )
                    closed_count += 1
            conn.commit()
    except Exception as e:
        print(f"[close_positions_weather_resolved] DB error step 4: {e}")

    print(f"[close_positions_weather_resolved] Done — {closed_count} position(s) closed.")
    return closed_count


# ─── Stop-loss / take-profit close ────────────────────────────────────────────

def close_positions_stop_loss(
    stop_loss_pct: float = STOP_LOSS_PCT,
    take_profit_pct: float = TAKE_PROFIT_PCT,
) -> int:
    """
    Close open positions that have hit a price-based threshold.

    Thresholds (configurable, see module constants):
      Long  + price falls stop_loss_pct%  below entry → close_reason = 'stop_loss'
      Short + price rises stop_loss_pct%  above entry → close_reason = 'stop_loss'
      Long  + price rises take_profit_pct% above entry → close_reason = 'take_profit'
      Short + price falls take_profit_pct% below entry → close_reason = 'take_profit'

    Uses live Finnhub prices.  Positions with no entry price or no live quote
    are skipped silently.

    Returns number of positions newly closed.
    """
    if not DATABASE_URL:
        return 0

    # ── Fetch all open positions that have an entry price ──────────────────────
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, stock_symbol, trade, entry_price, spy_entry
                    FROM recommendations_log
                    WHERE closed_at   IS NULL
                      AND entry_price IS NOT NULL
                      AND entry_price  > 0
                    """
                )
                open_positions = cur.fetchall()
    except Exception as e:
        print(f"[close_positions_stop_loss] DB error fetching open positions: {e}")
        return 0

    if not open_positions:
        return 0

    # ── Fetch live prices (Finnhub) ────────────────────────────────────────────
    symbols = list({row[1] for row in open_positions} | {"SPY"})
    live_prices = fetch_prices(symbols)
    spy_price   = live_prices.get("SPY")
    now_ts      = datetime.now(timezone.utc)

    triggers: list[tuple] = []  # (rec_id, symbol, trade, entry_p, exit_p, spy_exit, reason)

    for rec_id, symbol, trade, entry_p, spy_entry in open_positions:
        current_p = live_prices.get(symbol)
        if current_p is None or entry_p is None or entry_p == 0:
            continue

        if trade == "Long":
            pnl_pct = (current_p - entry_p) / entry_p * 100
        elif trade == "Short":
            pnl_pct = (entry_p - current_p) / entry_p * 100
        else:
            continue

        if pnl_pct <= -stop_loss_pct:
            close_reason = "stop_loss"
        elif pnl_pct >= take_profit_pct:
            close_reason = "take_profit"
        else:
            continue  # within thresholds — hold

        triggers.append((rec_id, symbol, trade, entry_p, current_p, spy_price, close_reason))
        print(
            f"[close_positions_stop_loss]   {close_reason.upper()} #{rec_id} "
            f"{symbol} {trade} entry={entry_p:.2f} now={current_p:.2f} pnl={pnl_pct:+.1f}%"
        )

    if not triggers:
        print("[close_positions_stop_loss] No stop-loss or take-profit triggers.")
        return 0

    # ── Write close records ────────────────────────────────────────────────────
    closed_count = 0
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                for rec_id, symbol, trade, entry_p, exit_p, spy_exit, close_reason in triggers:
                    cur.execute(
                        """
                        UPDATE recommendations_log
                        SET closed_at      = %s,
                            exit_price     = %s,
                            spy_exit_price = %s,
                            close_reason   = %s
                        WHERE id = %s AND closed_at IS NULL
                        """,
                        (now_ts, exit_p, spy_exit, close_reason, rec_id),
                    )
                    closed_count += 1
            conn.commit()
    except Exception as e:
        print(f"[close_positions_stop_loss] DB error writing closes: {e}")

    print(f"[close_positions_stop_loss] Done — {closed_count} position(s) closed.")
    return closed_count


# ─── Log recommendations ──────────────────────────────────────────────────────

def get_recently_recommended_combos(hours: int = 24) -> set[tuple[str, str, str]]:
    """
    Return set of (stock_symbol, region, anomaly) tuples logged in the last N hours.
    Used to enforce cooldown and suppress repeat logging.
    """
    if not DATABASE_URL:
        return set()
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT stock_symbol,
                           COALESCE(region, ''),
                           COALESCE(anomaly, '')
                    FROM recommendations_log
                    WHERE logged_at >= NOW() - INTERVAL '1 hour' * %s
                    """,
                    (hours,),
                )
                return {(r[0], r[1], r[2]) for r in cur.fetchall()}
    except Exception:
        return set()


def log_recommendations(pulse_df: pd.DataFrame, source: str = "weather") -> int:
    """
    Log current Pulse Trader recommendations to recommendations_log.
    - Fetches entry price (Finnhub) + SPY entry price for benchmark at time of logging.
    - Cooldown: skips (symbol, region, anomaly) already logged in last 24 hours.
    - source: 'weather' (default) or 'mining' — stored for Aftermath filtering.
    Returns number of rows inserted.
    """
    if pulse_df.empty:
        return 0

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    symbols = pulse_df["Stock Trade"].dropna().unique().tolist()
    symbols_with_spy = list(set(symbols) | {"SPY"})
    prices = fetch_prices(symbols_with_spy)
    spy_entry = prices.get("SPY")

    already_logged = get_recently_recommended_combos(hours=24)

    rows_to_insert = []
    for _, row in pulse_df.iterrows():
        symbol  = str(row.get("Stock Trade", "")).strip()
        trade   = str(row.get("Trade", "")).strip()
        region  = str(row.get("Region", "")).strip()
        anomaly = str(row.get("Anomaly", "")).strip()
        if not symbol or trade == "No Trade":
            continue
        if (symbol, region, anomaly) in already_logged:
            continue

        rows_to_insert.append({
            "signal_date":        row.get("Date", today),
            "stock_symbol":       symbol,
            "trade":              trade,
            "entry_price":        prices.get(symbol),
            "spy_entry":          spy_entry,
            "region":             str(row.get("Region", "")),
            "anomaly":            str(row.get("Anomaly", "")),
            "commodity":          str(row.get("Commodity", "")),
            "final_trade_score":  float(row.get("Final Trade Score", 0)),
            "conviction":         str(row.get("Conviction", "")),
            "why_it_matters":     str(row.get("Why It Matters", ""))[:500],
            "source":             source,
            # ML feature snapshot — captured at logging time
            "sigma_score":        float(row.get("Sigma Score", 1.0)),
            "seasonality_sc":     float(row.get("Seasonality", 5.0)),
            "trend_dir":          str(row.get("Trend", "new")),
            "confluence_bonus":   float(row.get("Confluence Bonus", 0.0)),
            "pheno_mult":         float(row.get("Pheno Mult", 1.0)),
        })

    if not rows_to_insert:
        return 0

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            for r in rows_to_insert:
                cur.execute(
                    """
                    INSERT INTO recommendations_log
                        (signal_date, stock_symbol, trade, entry_price, spy_entry,
                         region, anomaly, commodity, final_trade_score,
                         conviction, why_it_matters, source,
                         sigma_score, seasonality_sc, trend_dir,
                         confluence_bonus, pheno_mult)
                    VALUES (%(signal_date)s, %(stock_symbol)s, %(trade)s,
                            %(entry_price)s, %(spy_entry)s,
                            %(region)s, %(anomaly)s,
                            %(commodity)s, %(final_trade_score)s,
                            %(conviction)s, %(why_it_matters)s, %(source)s,
                            %(sigma_score)s, %(seasonality_sc)s, %(trend_dir)s,
                            %(confluence_bonus)s, %(pheno_mult)s)
                    """,
                    r,
                )
        conn.commit()

    return len(rows_to_insert)


# ─── P&L helpers ──────────────────────────────────────────────────────────────

def _compute_pnl(
    trade: str, entry: Optional[float], exit_price: Optional[float]
) -> Optional[float]:
    if entry is None or exit_price is None or entry == 0:
        return None
    if trade == "Long":
        return round((exit_price - entry) / entry * 100, 2)
    if trade == "Short":
        return round((entry - exit_price) / entry * 100, 2)
    return None


def _compute_alpha(
    trade: str,
    entry: Optional[float],
    exit_price: Optional[float],
    spy_entry: Optional[float],
    spy_exit: Optional[float],
) -> Optional[float]:
    """Stock P&L minus SPY P&L over the same period (market-relative return)."""
    stock_pnl = _compute_pnl(trade, entry, exit_price)
    spy_pnl   = _compute_pnl("Long", spy_entry, spy_exit)  # SPY is always Long
    if stock_pnl is None or spy_pnl is None:
        return None
    return round(stock_pnl - spy_pnl, 2)


def _outcome_label(pnl: Optional[float]) -> str:
    if pnl is None:
        return "⏳ Pending"
    return "✅ Win" if pnl > 0 else "❌ Loss" if pnl < 0 else "➖ Flat"


# ─── Aftermath table ──────────────────────────────────────────────────────────

def get_aftermath_table() -> pd.DataFrame:
    """
    Load all logged recommendations.
    0. Auto-close positions where media has confirmed the weather event.
    1. Persist T+3 / T+5 historical snapshots for rows that are due.
    2. Fetch live Finnhub prices for still-open positions (< T+10).
    3. Compute multi-horizon P&L and alpha vs SPY, including media-exit P&L.
    4. Return enriched display DataFrame.
    """
    # ── Step 0: Auto-close positions via all three exit triggers ─────────────
    # Priority: media > weather resolution > stop-loss/take-profit
    close_positions_from_media()
    close_positions_weather_resolved()
    close_positions_stop_loss()

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, logged_at, signal_date, stock_symbol, trade,
                       entry_price, spy_entry, region, anomaly, commodity,
                       final_trade_score, conviction, why_it_matters,
                       price_t3, price_t5, spy_t3, spy_t5,
                       price_t7, price_t10, spy_t7, spy_t10,
                       sigma_score, seasonality_sc, trend_dir,
                       confluence_bonus, pheno_mult,
                       closed_at, exit_price, spy_exit_price, close_reason
                FROM recommendations_log
                ORDER BY logged_at DESC
                """
            )
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=cols)
    df["logged_at"] = pd.to_datetime(df["logged_at"], utc=True)

    # ── Step 1: Persist T+3/T+5 snapshots where due ───────────────────────────
    _maybe_persist_snapshots(df)

    # Coerce new columns if they don't exist (pre-migration rows)
    for _col in ("closed_at", "exit_price", "spy_exit_price", "close_reason"):
        if _col not in df.columns:
            df[_col] = None

    # ── Step 2: Fetch live prices for open positions (T+10 not yet due) ─────
    today = datetime.now(timezone.utc).date()
    open_mask = (
        df["closed_at"].isna()          # not yet media-closed
        & df["price_t10"].isna()
        & df["logged_at"].apply(lambda x: _business_days_after(x, 10) > today)
    )
    open_symbols = df.loc[open_mask, "stock_symbol"].dropna().unique().tolist()
    live_symbols = list(set(open_symbols) | ({"SPY"} if open_symbols else set()))
    current_prices = fetch_prices(live_symbols) if live_symbols else {}

    df["current_price"] = df["stock_symbol"].apply(
        lambda s: current_prices.get(s)
    )
    spy_current = current_prices.get("SPY")

    # Backfill NULL entry prices with current price as proxy (persisted)
    null_entry_mask = df["entry_price"].isna()
    if null_entry_mask.any():
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                for idx, row in df[null_entry_mask].iterrows():
                    bp = current_prices.get(row["stock_symbol"])
                    if bp is not None:
                        cur.execute(
                            "UPDATE recommendations_log SET entry_price = %s WHERE id = %s",
                            (bp, int(row["id"])),
                        )
                        df.at[idx, "entry_price"] = bp
            conn.commit()

    # ── Step 3: Compute P&L at each horizon ───────────────────────────────────
    df["pnl_current"] = df.apply(
        lambda r: _compute_pnl(r["trade"], r.get("entry_price"), r.get("current_price")), axis=1
    )
    df["pnl_t3"] = df.apply(
        lambda r: _compute_pnl(r["trade"], r.get("entry_price"), r.get("price_t3")), axis=1
    )
    df["pnl_t5"] = df.apply(
        lambda r: _compute_pnl(r["trade"], r.get("entry_price"), r.get("price_t5")), axis=1
    )
    df["pnl_t7"] = df.apply(
        lambda r: _compute_pnl(r["trade"], r.get("entry_price"), r.get("price_t7")), axis=1
    )
    df["pnl_t10"] = df.apply(
        lambda r: _compute_pnl(r["trade"], r.get("entry_price"), r.get("price_t10")), axis=1
    )
    df["alpha_t3"] = df.apply(
        lambda r: _compute_alpha(
            r["trade"], r.get("entry_price"), r.get("price_t3"),
            r.get("spy_entry"), r.get("spy_t3")
        ), axis=1
    )
    df["alpha_t5"] = df.apply(
        lambda r: _compute_alpha(
            r["trade"], r.get("entry_price"), r.get("price_t5"),
            r.get("spy_entry"), r.get("spy_t5")
        ), axis=1
    )
    df["alpha_t7"] = df.apply(
        lambda r: _compute_alpha(
            r["trade"], r.get("entry_price"), r.get("price_t7"),
            r.get("spy_entry"), r.get("spy_t7")
        ), axis=1
    )
    df["alpha_t10"] = df.apply(
        lambda r: _compute_alpha(
            r["trade"], r.get("entry_price"), r.get("price_t10"),
            r.get("spy_entry"), r.get("spy_t10")
        ), axis=1
    )

    # ── Exit P&L (media-triggered close) ──────────────────────────────────────
    df["pnl_exit"] = df.apply(
        lambda r: _compute_pnl(r["trade"], r.get("entry_price"), r.get("exit_price")), axis=1
    )
    df["alpha_exit"] = df.apply(
        lambda r: _compute_alpha(
            r["trade"], r.get("entry_price"), r.get("exit_price"),
            r.get("spy_entry"), r.get("spy_exit_price"),
        ), axis=1
    )

    # Best available P&L: media exit > T+10 > T+7 > T+5 > T+3 > current
    # Media exit is the ACTUAL close, so it takes priority over theoretical horizons
    df["best_pnl"] = df.apply(
        lambda r: r["pnl_exit"]   if r["pnl_exit"]   is not None
                  else r["pnl_t10"] if r["pnl_t10"] is not None
                  else r["pnl_t7"]  if r["pnl_t7"]  is not None
                  else r["pnl_t5"]  if r["pnl_t5"]  is not None
                  else r["pnl_t3"]  if r["pnl_t3"]  is not None
                  else r["pnl_current"],
        axis=1,
    )

    # Status label — shows close reason + date
    _CLOSE_REASON_LABELS: dict[str, str] = {
        "media_confirmed":  "🚪 Media Exit",
        "weather_resolved": "🌤️ Event Resolved",
        "stop_loss":        "🛑 Stop Loss",
        "take_profit":      "🎯 Take Profit",
    }

    def _status_label(row) -> str:
        reason = row.get("close_reason")
        if reason and reason in _CLOSE_REASON_LABELS:
            closed_dt = row.get("closed_at")
            date_str = ""
            if closed_dt is not None:
                try:
                    if not pd.isna(closed_dt):
                        date_str = f" {pd.Timestamp(closed_dt).strftime('%b %d')}"
                except Exception:
                    pass
            return f"{_CLOSE_REASON_LABELS[reason]}{date_str}"
        return "⏳ Open"

    df["status"] = df.apply(_status_label, axis=1)
    df["outcome"] = df["best_pnl"].apply(_outcome_label)

    # ── Step 4: Build display DataFrame ───────────────────────────────────────
    def fmt(x, prefix="", suffix="", decimals=2, sign=False):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "—"
        fmt_str = f"{prefix}{x:+.{decimals}f}{suffix}" if sign else f"{prefix}{x:.{decimals}f}{suffix}"
        return fmt_str

    def fmt_alpha(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "—"
        arrow = "↑" if x > 0 else ("↓" if x < 0 else "→")
        return f"{arrow}{abs(x):.2f}%"

    # Source label for display (default to weather for pre-existing rows)
    _source_map = {"mining": "⛏️ Mining", "weather": "🌤️ Weather"}
    _source_col = (
        df["source"].map(_source_map).fillna("🌤️ Weather")
        if "source" in df.columns
        else pd.Series("🌤️ Weather", index=df.index)
    )

    display = pd.DataFrame({
        "Date Logged":  pd.to_datetime(df["logged_at"]).dt.strftime("%Y-%m-%d"),
        "Source":       _source_col,
        "Status":       df["status"],
        "Stock":        df["stock_symbol"],
        "Trade":        df["trade"],
        "Entry":        df["entry_price"].apply(lambda x: fmt(x, prefix="$")),
        # ── Media exit (actual close) ─────────────────────────────────────────
        "Exit P&L":     df["pnl_exit"].apply(lambda x: fmt(x, suffix="%", sign=True)),
        "Exit α SPY":   df["alpha_exit"].apply(fmt_alpha),
        # ── Theoretical horizons ──────────────────────────────────────────────
        "Day 0 P&L":    df["pnl_current"].apply(lambda x: fmt(x, suffix="%", sign=True)),
        "T+3 P&L":      df["pnl_t3"].apply(lambda x: fmt(x, suffix="%", sign=True)),
        "T+3 α SPY":    df["alpha_t3"].apply(fmt_alpha),
        "T+5 P&L":      df["pnl_t5"].apply(lambda x: fmt(x, suffix="%", sign=True)),
        "T+5 α SPY":    df["alpha_t5"].apply(fmt_alpha),
        "T+7 P&L":      df["pnl_t7"].apply(lambda x: fmt(x, suffix="%", sign=True)),
        "T+7 α SPY":    df["alpha_t7"].apply(fmt_alpha),
        "T+10 P&L":     df["pnl_t10"].apply(lambda x: fmt(x, suffix="%", sign=True)),
        "T+10 α SPY":   df["alpha_t10"].apply(fmt_alpha),
        "Outcome":      df["outcome"],
        "Score":        df["final_trade_score"].round(2),
        "Conviction":   df["conviction"],
        "Region":       df["region"],
        "Anomaly":      df["anomaly"],
        "Why":          df["why_it_matters"],
        # Hidden raw columns for stats / ML
        "_pnl_raw":       df["best_pnl"],
        "_pnl_exit_raw":  df["pnl_exit"],
        "_alpha_exit_raw":df["alpha_exit"],
        "_pnl_t3_raw":    df["pnl_t3"],
        "_pnl_t5_raw":    df["pnl_t5"],
        "_pnl_t7_raw":    df["pnl_t7"],
        "_pnl_t10_raw":   df["pnl_t10"],
        "_alpha_t3_raw":  df["alpha_t3"],
        "_alpha_t5_raw":  df["alpha_t5"],
        "_alpha_t7_raw":  df["alpha_t7"],
        "_alpha_t10_raw": df["alpha_t10"],
        "_is_closed":     df["close_reason"].eq("media_confirmed"),
        # ML feature snapshot columns
        "_sigma":      df["sigma_score"] if "sigma_score" in df.columns else pd.Series(1.0, index=df.index),
        "_seasonality": df["seasonality_sc"] if "seasonality_sc" in df.columns else pd.Series(5.0, index=df.index),
        "_trend_dir":  df["trend_dir"] if "trend_dir" in df.columns else pd.Series("new", index=df.index),
        "_confluence": df["confluence_bonus"] if "confluence_bonus" in df.columns else pd.Series(0.0, index=df.index),
        "_pheno_mult": df["pheno_mult"] if "pheno_mult" in df.columns else pd.Series(1.0, index=df.index),
    })

    return display


def get_performance_summary(aftermath_df: pd.DataFrame) -> dict:
    """
    Compute win rate, avg P&L, best/worst trade.
    Uses best available P&L (T+5 > T+3 > current) for each trade.
    """
    if aftermath_df.empty:
        return {}

    pnl_series = aftermath_df["_pnl_raw"].dropna()
    if pnl_series.empty:
        return {}

    wins   = (pnl_series > 0).sum()
    losses = (pnl_series < 0).sum()
    total  = len(pnl_series)

    best_idx  = pnl_series.idxmax()
    worst_idx = pnl_series.idxmin()

    t3_count   = aftermath_df["_pnl_t3_raw"].dropna().shape[0]   if "_pnl_t3_raw"   in aftermath_df.columns else 0
    t5_count   = aftermath_df["_pnl_t5_raw"].dropna().shape[0]   if "_pnl_t5_raw"   in aftermath_df.columns else 0
    t7_count   = aftermath_df["_pnl_t7_raw"].dropna().shape[0]   if "_pnl_t7_raw"   in aftermath_df.columns else 0
    t10_count  = aftermath_df["_pnl_t10_raw"].dropna().shape[0]  if "_pnl_t10_raw"  in aftermath_df.columns else 0
    exit_count = aftermath_df["_pnl_exit_raw"].dropna().shape[0] if "_pnl_exit_raw" in aftermath_df.columns else 0
    closed_count = int(aftermath_df["_is_closed"].sum()) if "_is_closed" in aftermath_df.columns else 0

    return {
        "total":          total,
        "wins":           int(wins),
        "losses":         int(losses),
        "win_rate":       round(wins / total * 100, 1) if total else 0,
        "avg_pnl":        round(float(pnl_series.mean()), 2),
        "best_trade":     f"{aftermath_df.loc[best_idx, 'Stock']} {pnl_series[best_idx]:+.2f}%",
        "worst_trade":    f"{aftermath_df.loc[worst_idx, 'Stock']} {pnl_series[worst_idx]:+.2f}%",
        "t3_evaluated":   t3_count,
        "t5_evaluated":   t5_count,
        "t7_evaluated":   t7_count,
        "t10_evaluated":  t10_count,
        "exit_evaluated": exit_count,
        "closed_count":   closed_count,
    }
