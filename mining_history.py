"""
mining_history.py
─────────────────
Fetches 10-year GDELT news volume timelines for mining commodities and regions.
Results are cached in PostgreSQL (24h TTL) to avoid re-querying on every load.

Entry points used by the dashboard:
  - ensure_history_schema()        — creates the cache table if missing
  - get_timeline(commodity, region) — returns pd.DataFrame with date + volume
  - get_peak_articles(commodity, region, date_str) — articles near a spike date
  - COMMODITIES, REGIONS            — dropdown option dicts
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg
import requests

DATABASE_URL = os.environ.get("DATABASE_URL")

# ── What the user can choose ───────────────────────────────────────────────────

COMMODITIES: dict[str, dict] = {
    "Copper":    {
        "queries": ["copper mine strike", "copper mine closure accident",
                    "copper mine flood protest", "copper mine explosion"],
        "tickers": ["FCX", "SCCO", "HBM", "COPX"],
        "color":   "#f97316",
    },
    "Gold":      {
        "queries": ["gold mine strike closure", "gold mine accident flood",
                    "gold mine protest shutdown"],
        "tickers": ["GDX", "NEM", "GOLD", "AEM"],
        "color":   "#eab308",
    },
    "Iron Ore":  {
        "queries": ["iron ore mine closure accident", "iron ore mine flood strike",
                    "Vale mine BHP mine Rio Tinto mine disruption"],
        "tickers": ["VALE", "BHP", "RIO", "CLF"],
        "color":   "#ef4444",
    },
    "Coal":      {
        "queries": ["coal mine explosion flood closure", "coal mine strike accident",
                    "coal mine shutdown protest"],
        "tickers": ["ARCH", "CEIX", "BTU", "SXC"],
        "color":   "#6b7280",
    },
    "Lithium":   {
        "queries": ["lithium mine closure accident", "lithium mine strike protest",
                    "lithium supply disruption"],
        "tickers": ["ALB", "SQM", "LAC", "LTHM"],
        "color":   "#8b5cf6",
    },
    "Nickel":    {
        "queries": ["nickel mine closure strike", "nickel mine accident flood",
                    "nickel supply disruption"],
        "tickers": ["VALE", "NILSY", "NORILSK"],
        "color":   "#06b6d4",
    },
    "Silver":    {
        "queries": ["silver mine closure strike", "silver mine accident flood",
                    "silver mine shutdown"],
        "tickers": ["SLV", "PAAS", "AG", "WPM"],
        "color":   "#94a3b8",
    },
    "Zinc":      {
        "queries": ["zinc mine closure strike", "zinc mine accident flood",
                    "zinc supply disruption"],
        "tickers": ["HBM", "TECK", "ZNO"],
        "color":   "#84cc16",
    },
}

REGIONS: dict[str, str] = {
    "Global":          "",
    "Chile":           "Chile",
    "Peru":            "Peru",
    "DRC / Congo":     "Congo DRC",
    "Australia":       "Australia",
    "South Africa":    "South Africa",
    "Indonesia":       "Indonesia",
    "Brazil":          "Brazil",
    "Canada":          "Canada",
    "United States":   "United States",
    "Russia":          "Russia",
    "Zambia":          "Zambia",
    "Mexico":          "Mexico",
    "Philippines":     "Philippines",
    "Papua New Guinea":"Papua New Guinea",
}

# Known major historical mining events — shown as reference markers on chart
MAJOR_EVENTS: list[dict] = [
    {"date": "2015-11-05", "label": "Samarco dam collapse (Brazil)", "commodity": "Iron Ore"},
    {"date": "2019-01-25", "label": "Vale Brumadinho dam collapse", "commodity": "Iron Ore"},
    {"date": "2015-07-01", "label": "BHP/Rio Tinto copper strike Chile", "commodity": "Copper"},
    {"date": "2017-02-01", "label": "Grasberg (Indonesia) strike", "commodity": "Copper"},
    {"date": "2021-03-01", "label": "Escondida strike talks Chile", "commodity": "Copper"},
    {"date": "2016-10-01", "label": "South Africa coal mine protests", "commodity": "Coal"},
    {"date": "2022-02-01", "label": "DRC mining disruptions (conflict)", "commodity": "Cobalt"},
    {"date": "2023-05-01", "label": "Panama Cobre closure protests", "commodity": "Copper"},
    {"date": "2023-11-01", "label": "Panama mine shut — canal drought", "commodity": "Copper"},
    {"date": "2024-01-01", "label": "Red Sea → shipping reroute impact", "commodity": "Iron Ore"},
]


# ── DB schema ──────────────────────────────────────────────────────────────────

def ensure_history_schema() -> None:
    if not DATABASE_URL:
        return
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mining_history_cache (
                cache_key   TEXT PRIMARY KEY,
                payload     TEXT NOT NULL,
                cached_at   TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()


def _cache_get(key: str) -> pd.DataFrame | None:
    """Return cached DataFrame if younger than 24h, else None."""
    if not DATABASE_URL:
        return None
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            row = conn.execute(
                "SELECT payload, cached_at FROM mining_history_cache WHERE cache_key = %s",
                (key,)
            ).fetchone()
        if not row:
            return None
        cached_at = row[1]
        if cached_at.tzinfo is None:
            cached_at = cached_at.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - cached_at
        if age > timedelta(hours=24):
            return None
        return pd.read_json(row[0], orient="records")
    except Exception:
        return None


def _cache_set(key: str, df: pd.DataFrame) -> None:
    if not DATABASE_URL:
        return
    try:
        payload = df.to_json(orient="records")
        with psycopg.connect(DATABASE_URL) as conn:
            conn.execute("""
                INSERT INTO mining_history_cache (cache_key, payload, cached_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (cache_key) DO UPDATE
                SET payload = EXCLUDED.payload, cached_at = NOW()
            """, (key, payload))
            conn.commit()
    except Exception:
        pass


# ── GDELT helpers ──────────────────────────────────────────────────────────────

def _gdelt_timeline(query: str, timespan: str = "10y") -> list[dict]:
    """
    Call GDELT DOC v2 TimelineVol API.
    Returns list of {date, value} dicts, empty on failure.
    """
    q = urllib.parse.quote(f"{query} sourcelang:english")
    url = (
        f"https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={q}&mode=TimelineVol&timespan={timespan}&format=json"
    )
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        # GDELT returns {"timeline": [{"date":"20150101000000","value":0.0}, ...]}
        # or nested under a key like "data"
        timeline = data.get("timeline") or []
        if not timeline and isinstance(data, list):
            timeline = data
        return timeline
    except Exception:
        return []


def _parse_gdelt_date(raw: str) -> str | None:
    """Convert '20150101000000' → '2015-01-01'."""
    try:
        return datetime.strptime(str(raw)[:8], "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        return None


def _gdelt_articles(query: str, timespan: str = "3m") -> list[dict]:
    """Return recent articles for a query — used for peak drill-down."""
    q = urllib.parse.quote(f"{query} sourcelang:english")
    url = (
        f"https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={q}&mode=ArtList&maxrecords=10&timespan={timespan}&format=json"
    )
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json().get("articles", [])
    except Exception:
        return []


# ── Public API ─────────────────────────────────────────────────────────────────

def get_timeline(commodity: str, region: str = "Global") -> pd.DataFrame:
    """
    Return a DataFrame with columns [date, volume, query] covering ~10 years.
    Results are aggregated across all queries for the commodity.
    Cached 24h in DB.
    """
    cache_key = f"timeline_{commodity}_{region}".replace(" ", "_").lower()
    cached = _cache_get(cache_key)
    if cached is not None and not cached.empty:
        return cached

    com_cfg  = COMMODITIES.get(commodity, {})
    queries  = com_cfg.get("queries", [f"{commodity.lower()} mine"])
    reg_term = REGIONS.get(region, region)

    # Combine volumes across all queries into one daily series
    combined: dict[str, float] = {}

    for q in queries:
        full_q = f"{q} {reg_term}".strip() if reg_term else q
        rows   = _gdelt_timeline(full_q, timespan="10y")
        for row in rows:
            d = _parse_gdelt_date(row.get("date", ""))
            if d:
                combined[d] = combined.get(d, 0.0) + float(row.get("value", 0))
        time.sleep(0.5)   # be polite to GDELT

    if not combined:
        return pd.DataFrame(columns=["date", "volume"])

    df = (
        pd.DataFrame({"date": list(combined.keys()), "volume": list(combined.values())})
        .sort_values("date")
        .reset_index(drop=True)
    )
    df["date"] = pd.to_datetime(df["date"])

    # 4-week rolling average so chart is readable
    df["volume_smooth"] = df["volume"].rolling(window=28, min_periods=1).mean()

    _cache_set(cache_key, df)
    return df


def get_peak_articles(commodity: str, region: str = "Global") -> list[dict]:
    """Return recent articles for the commodity/region (last 3 months)."""
    com_cfg  = COMMODITIES.get(commodity, {})
    queries  = com_cfg.get("queries", [f"{commodity.lower()} mine"])
    reg_term = REGIONS.get(region, region)

    q     = queries[0]
    full_q = f"{q} {reg_term}".strip() if reg_term else q
    return _gdelt_articles(full_q, timespan="3m")


def get_major_events(commodity: str) -> list[dict]:
    """Return known major events for a commodity (or all if 'All')."""
    if commodity == "All":
        return MAJOR_EVENTS
    return [e for e in MAJOR_EVENTS if e["commodity"] == commodity]
