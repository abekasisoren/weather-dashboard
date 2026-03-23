"""
mining_news_monitor.py — Mining supply-disruption radar.

Continuously scans NewsAPI, GDELT, Google News RSS, and Bing News RSS for
events that could disrupt supply at major global mining regions, then scores
and persists signals to the `mining_signals` DB table.

Entry point for the dashboard:
    signals_df = get_active_signals()   # read from DB
    pulse_df   = build_mining_pulse_table(signals_df)  # format for Pulse Trader

Standalone scan (run from CLI or a cron job):
    python mining_news_monitor.py
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import psycopg

from mining_regions import (
    MINING_REGIONS,
    EVENT_TYPES,
    COMMODITY_VEHICLES,
    SIGNAL_BUCKETS,
    supply_to_signal_level,
)

DATABASE_URL  = os.environ.get("DATABASE_URL")
NEWSAPI_KEY   = os.environ.get("NEWSAPI_KEY")

# Minimum news score to create a signal
SCORE_THRESHOLD = 4.0
# Hours before re-scanning same region/commodity
SCAN_COOLDOWN_H = 2


# ─── DB Schema ────────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS mining_signals (
    id                  SERIAL PRIMARY KEY,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    last_updated        TIMESTAMPTZ DEFAULT NOW(),
    region              TEXT NOT NULL,
    country             TEXT,
    commodity           TEXT NOT NULL,
    event_type          TEXT,
    event_summary       TEXT,
    severity_score      INTEGER DEFAULT 1,
    supply_impact_pct   FLOAT DEFAULT 0,
    signal_level        INTEGER DEFAULT 1,
    signal_bucket       TEXT DEFAULT 'WATCH',
    trade_bias          TEXT DEFAULT 'Long',
    trend_direction     TEXT DEFAULT 'new',
    affected_stocks     JSONB,
    news_headline       TEXT,
    news_source         TEXT,
    news_url            TEXT,
    news_score          FLOAT,
    news_published_at   TIMESTAMPTZ,
    is_active           BOOLEAN DEFAULT TRUE
);
"""

_MIGRATIONS = [
    "ALTER TABLE mining_signals ADD COLUMN IF NOT EXISTS news_article_url TEXT",
    "CREATE INDEX IF NOT EXISTS idx_mining_region_commodity ON mining_signals(region, commodity)",
    "CREATE INDEX IF NOT EXISTS idx_mining_active ON mining_signals(is_active, last_updated)",
]


def ensure_mining_schema() -> None:
    if not DATABASE_URL:
        return
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(_CREATE_TABLE)
            for stmt in _MIGRATIONS:
                try:
                    cur.execute(stmt)
                except Exception:
                    pass
        conn.commit()


# ─── Text scoring helpers ──────────────────────────────────────────────────────

def _score_text(text: str, keywords: list[str], boost_terms: list[str] | None = None) -> float:
    """
    Score a text against keyword lists.
    Returns 0-10 where 0 = no match, 10 = strong multi-keyword match.
    """
    if not text:
        return 0.0
    t = text.lower()

    kw_score = 0.0
    for kw in keywords:
        if kw.lower() in t:
            kw_score += 2.0
            if kw_score >= 6.0:
                break

    boost_score = 0.0
    if boost_terms:
        for bt in boost_terms:
            if bt.lower() in t:
                boost_score += 1.5
                if boost_score >= 3.0:
                    break

    return min(10.0, kw_score + boost_score)


def _detect_event_type(text: str) -> tuple[str, int]:
    """
    Identify the most prominent event type from article text.
    Returns (event_type_key, base_severity).
    Defaults to ("geopolitical", 4) if no match.
    """
    best_type = "geopolitical"
    best_score = 0
    best_severity = 4

    t = text.lower()
    for etype, meta in EVENT_TYPES.items():
        hits = sum(1 for kw in meta["keywords"] if kw in t)
        if hits > best_score:
            best_score = hits
            best_type = etype
            best_severity = meta["base_severity"]

    return best_type, best_severity


def _compute_severity(
    base_severity: int,
    supply_pct: float,
    published_at: Optional[datetime] = None,
    source_count: int = 1,
) -> int:
    """
    Combine event severity, supply impact, freshness, and coverage into 1-10 score.
    """
    # Supply impact bonus
    if supply_pct >= 30:
        supply_bonus = 3
    elif supply_pct >= 15:
        supply_bonus = 2
    elif supply_pct >= 5:
        supply_bonus = 1
    else:
        supply_bonus = 0

    # Freshness bonus
    freshness = 0
    if published_at:
        age_h = (datetime.now(timezone.utc) - published_at).total_seconds() / 3600
        if age_h < 6:
            freshness = 2
        elif age_h < 24:
            freshness = 1

    # Coverage bonus
    coverage = min(2, source_count - 1)

    raw = base_severity + supply_bonus + freshness + coverage
    return min(10, max(1, raw))


# ─── News source fetchers ──────────────────────────────────────────────────────

def _fetch_gdelt(query: str, max_records: int = 8) -> list[dict]:
    """Query GDELT v2 Doc API. Free, no key needed."""
    q = urllib.parse.quote(query)
    url = (
        f"https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={q}&mode=artlist&maxrecords={max_records}"
        f"&format=json&sort=datedesc"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        articles = data.get("articles", []) or []
        return [
            {
                "headline": a.get("title", ""),
                "source":   a.get("domain", "gdelt"),
                "url":      a.get("url", ""),
                "published_at": None,
            }
            for a in articles
        ]
    except Exception:
        return []


def _fetch_google_rss(query: str) -> list[dict]:
    """Scrape Google News RSS (no auth)."""
    q = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            xml = resp.read().decode("utf-8", errors="replace")
        root = ET.fromstring(xml)
        items = []
        for item in root.findall(".//item")[:8]:
            title = item.findtext("title") or ""
            link  = item.findtext("link") or ""
            items.append({"headline": title, "source": "google_news", "url": link, "published_at": None})
        return items
    except Exception:
        return []


def _fetch_bing_rss(query: str) -> list[dict]:
    """Bing News RSS feed."""
    q = urllib.parse.quote(query)
    url = f"https://www.bing.com/news/search?q={q}&format=rss"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; WeatherTrader/1.0)"
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            xml = resp.read().decode("utf-8", errors="replace")
        root = ET.fromstring(xml)
        items = []
        for item in root.findall(".//item")[:6]:
            title = item.findtext("title") or ""
            link  = item.findtext("link") or ""
            items.append({"headline": title, "source": "bing_news", "url": link, "published_at": None})
        return items
    except Exception:
        return []


def _fetch_newsapi(query: str) -> list[dict]:
    """NewsAPI.org (requires NEWSAPI_KEY env var)."""
    if not NEWSAPI_KEY:
        return []
    q = urllib.parse.quote(query)
    url = (
        f"https://newsapi.org/v2/everything"
        f"?q={q}&sortBy=publishedAt&pageSize=6&language=en&apiKey={NEWSAPI_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        articles = data.get("articles", []) or []
        result = []
        for a in articles:
            pub = None
            try:
                pub = datetime.fromisoformat(a.get("publishedAt", "").replace("Z", "+00:00"))
            except Exception:
                pass
            result.append({
                "headline":     (a.get("title") or ""),
                "source":       (a.get("source", {}).get("name") or "newsapi"),
                "url":          (a.get("url") or ""),
                "published_at": pub,
            })
        return result
    except Exception:
        return []


# ─── Region scanner ───────────────────────────────────────────────────────────

def scan_region_commodity(
    region: dict,
    commodity: str,
) -> Optional[dict]:
    """
    Scan news for one (region, commodity) pair.
    Returns a signal dict if a qualifying event is found, else None.
    """
    region_name  = region["region"]
    country      = region["country"]
    supply_pct   = region["global_supply_pct"].get(commodity, 0)
    region_keywords = region["keywords"]

    # Build 3 query variations for maximum coverage
    queries = [
        f"{region['keywords'][0]} {commodity} mine",
        f"{country} {commodity} mining disruption",
        f"{region['keywords'][0]} supply disruption",
    ]

    all_articles: list[dict] = []
    for q in queries[:2]:  # 2 queries to stay under rate limits
        all_articles += _fetch_gdelt(q)
        all_articles += _fetch_google_rss(q)
        all_articles += _fetch_newsapi(q)
        time.sleep(0.3)  # polite rate limiting

    if not all_articles:
        return None

    # Score each article
    best_score    = 0.0
    best_article  = None
    matching_count = 0

    for art in all_articles:
        full_text = f"{art.get('headline', '')} {art.get('source', '')}"
        # Region + commodity relevance
        region_score = _score_text(full_text, region_keywords)
        commodity_score = _score_text(full_text, [commodity, f"{commodity} mine", f"{commodity} supply"])
        total = (region_score * 0.6) + (commodity_score * 0.4)

        if total >= SCORE_THRESHOLD * 0.7:
            matching_count += 1

        if total > best_score:
            best_score   = total
            best_article = art

    if best_score < SCORE_THRESHOLD or best_article is None:
        return None

    # Detect event type from best article
    headline   = best_article.get("headline", "")
    event_type, base_severity = _detect_event_type(headline)
    event_meta = EVENT_TYPES.get(event_type, {})
    trade_bias = event_meta.get("trade_bias", "Long")
    duration   = event_meta.get("duration_est", "days_to_weeks")
    trend      = event_meta.get("trend", "new")

    severity = _compute_severity(
        base_severity,
        supply_pct,
        best_article.get("published_at"),
        source_count=matching_count,
    )
    signal_level  = supply_to_signal_level(supply_pct)
    signal_bucket = SIGNAL_BUCKETS.get(signal_level, "WATCH")

    # Stock candidates
    stocks = region["stocks"].get(commodity, {"Long": [], "Short": []})

    return {
        "region":            region_name,
        "country":           country,
        "commodity":         commodity,
        "event_type":        event_type,
        "event_summary":     event_meta.get("description", event_type),
        "severity_score":    severity,
        "supply_impact_pct": supply_pct,
        "signal_level":      signal_level,
        "signal_bucket":     signal_bucket,
        "trade_bias":        trade_bias,
        "trend_direction":   trend,
        "affected_stocks":   json.dumps(stocks),
        "news_headline":     headline[:500],
        "news_source":       best_article.get("source", ""),
        "news_url":          best_article.get("url", ""),
        "news_score":        round(best_score, 2),
        "news_published_at": best_article.get("published_at"),
        "duration_est":      duration,
    }


# ─── DB persistence ───────────────────────────────────────────────────────────

def _upsert_signal(signal: dict) -> None:
    """
    Insert a new signal or update an existing one for the same
    (region, commodity, event_type) within the last 48 hours.
    """
    if not DATABASE_URL:
        return

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # Check for recent duplicate
            cur.execute(
                """
                SELECT id FROM mining_signals
                WHERE region     = %s
                  AND commodity  = %s
                  AND event_type = %s
                  AND is_active  = TRUE
                  AND last_updated > NOW() - INTERVAL '48 hours'
                ORDER BY last_updated DESC
                LIMIT 1
                """,
                (signal["region"], signal["commodity"], signal["event_type"]),
            )
            row = cur.fetchone()

            if row:
                # Update existing
                cur.execute(
                    """
                    UPDATE mining_signals
                    SET last_updated      = NOW(),
                        severity_score    = %s,
                        signal_level      = %s,
                        signal_bucket     = %s,
                        news_headline     = %s,
                        news_source       = %s,
                        news_url          = %s,
                        news_score        = %s,
                        news_published_at = %s,
                        trend_direction   = CASE
                            WHEN severity_score < %s THEN 'escalating'
                            WHEN severity_score > %s THEN 'de-escalating'
                            ELSE 'stable'
                        END
                    WHERE id = %s
                    """,
                    (
                        signal["severity_score"], signal["signal_level"],
                        signal["signal_bucket"],  signal["news_headline"],
                        signal["news_source"],    signal["news_url"],
                        signal["news_score"],     signal["news_published_at"],
                        signal["severity_score"], signal["severity_score"],
                        row[0],
                    ),
                )
            else:
                # Insert new
                cur.execute(
                    """
                    INSERT INTO mining_signals
                        (region, country, commodity, event_type, event_summary,
                         severity_score, supply_impact_pct, signal_level, signal_bucket,
                         trade_bias, trend_direction, affected_stocks,
                         news_headline, news_source, news_url, news_score,
                         news_published_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        signal["region"],          signal["country"],
                        signal["commodity"],       signal["event_type"],
                        signal["event_summary"],   signal["severity_score"],
                        signal["supply_impact_pct"], signal["signal_level"],
                        signal["signal_bucket"],   signal["trade_bias"],
                        signal["trend_direction"], signal["affected_stocks"],
                        signal["news_headline"],   signal["news_source"],
                        signal["news_url"],        signal["news_score"],
                        signal["news_published_at"],
                    ),
                )
        conn.commit()


# ─── Main scan ────────────────────────────────────────────────────────────────

def scan_all_regions(max_regions: int = 20) -> int:
    """
    Scan all mining regions for supply disruption news.
    Writes qualifying signals to the mining_signals table.
    Returns number of new/updated signals found.
    """
    ensure_mining_schema()
    count = 0

    for region in MINING_REGIONS[:max_regions]:
        for commodity in region["commodities"][:2]:  # top 2 commodities per region
            try:
                signal = scan_region_commodity(region, commodity)
                if signal:
                    _upsert_signal(signal)
                    count += 1
                    print(
                        f"[mining] ✓ {region['region']} / {commodity} — "
                        f"{signal['event_type']} score={signal['severity_score']} "
                        f"'{signal['news_headline'][:60]}'"
                    )
                else:
                    print(f"[mining] — {region['region']} / {commodity}: no qualifying news")
            except Exception as e:
                print(f"[mining] ✗ {region['region']} / {commodity}: {e}")

    print(f"[mining] Scan complete — {count} signal(s) found/updated.")
    return count


# ─── DB reader ────────────────────────────────────────────────────────────────

def get_active_signals() -> pd.DataFrame:
    """
    Load all active mining signals from the DB (signals updated in last 7 days).
    Returns empty DataFrame if DB unavailable.
    """
    if not DATABASE_URL:
        return pd.DataFrame()

    ensure_mining_schema()

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id, created_at, last_updated,
                        region, country, commodity, event_type, event_summary,
                        severity_score, supply_impact_pct, signal_level, signal_bucket,
                        trade_bias, trend_direction, affected_stocks,
                        news_headline, news_source, news_url, news_score,
                        news_published_at, is_active
                    FROM mining_signals
                    WHERE is_active    = TRUE
                      AND last_updated > NOW() - INTERVAL '7 days'
                    ORDER BY severity_score DESC, last_updated DESC
                    """
                )
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
    except Exception as e:
        print(f"[mining] get_active_signals error: {e}")
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=cols)
    df["last_updated"] = pd.to_datetime(df["last_updated"], utc=True)
    df["created_at"]   = pd.to_datetime(df["created_at"],   utc=True)
    return df


# ─── Pulse Trader table builder ───────────────────────────────────────────────

def build_mining_pulse_table(signals_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert active mining signals into a Pulse Trader compatible DataFrame.
    One row per (stock, region, commodity) combination.
    Columns match weather pulse table for seamless combined display.
    """
    if signals_df.empty:
        return pd.DataFrame()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows: list[dict] = []

    # Conviction bucketing (mirrors weather system)
    def _conviction(score: float) -> str:
        if score >= 9:   return "PRIME"
        if score >= 7:   return "ACTIONABLE"
        if score >= 5:   return "WATCH"
        return "MARGINAL"

    # Trend badge (mirrors weather)
    _trend_badge = {
        "new":           "★ NEW",
        "escalating":    "↑ ESCALATING",
        "stable":        "→ STABLE",
        "de-escalating": "↓ DE-ESCALATING",
    }

    for _, sig in signals_df.iterrows():
        try:
            stocks_raw = sig.get("affected_stocks")
            if isinstance(stocks_raw, str):
                stocks_data = json.loads(stocks_raw)
            elif isinstance(stocks_raw, dict):
                stocks_data = stocks_raw
            else:
                stocks_data = {}
        except Exception:
            stocks_data = {}

        trade_bias = str(sig.get("trade_bias", "Long"))
        # For "Long" bias, take Long stock list; for "Short" bias, take Short list
        stock_list = stocks_data.get(trade_bias, [])
        if not stock_list:
            # Fall back: take the commodity ETF vehicle
            vehicle = COMMODITY_VEHICLES.get(str(sig.get("commodity", "")), "")
            if vehicle:
                stock_list = [vehicle]

        severity      = float(sig.get("severity_score", 1))
        supply_pct    = float(sig.get("supply_impact_pct", 0))
        # Final score: severity weighted by supply impact
        final_score   = round(min(10.0, severity * (0.7 + supply_pct / 100)), 2)
        conviction    = _conviction(final_score)
        trend_raw     = str(sig.get("trend_direction", "new")).lower()
        trend_badge   = _trend_badge.get(trend_raw, "→ STABLE")
        event_summary = str(sig.get("event_summary", sig.get("event_type", "")))
        headline      = str(sig.get("news_headline", ""))[:120]

        for symbol in stock_list[:4]:
            rows.append({
                # ── Shared columns with weather pulse ──────────────────────────
                "Source":           "⛏️ Mining",
                "Date":             today,
                "Stock Trade":      symbol,
                "Trade":            trade_bias,
                "Region":           sig.get("region", ""),
                "Country":          sig.get("country", ""),
                "Commodity":        sig.get("commodity", ""),
                "Anomaly":          sig.get("event_type", ""),  # reuse Anomaly col
                "Trend":            trend_badge,
                "Vehicle":          COMMODITY_VEHICLES.get(str(sig.get("commodity", "")), symbol),
                "Final Trade Score": final_score,
                "Conviction":       conviction,
                "Signal Bucket":    str(sig.get("signal_bucket", "WATCH")),
                "Supply Impact %":  f"{supply_pct:.0f}%",
                "Why It Matters":   (
                    f"{event_summary} at {sig.get('region')} ({sig.get('country')}). "
                    f"~{supply_pct:.0f}% of global {sig.get('commodity')} supply at risk. "
                    f"{headline}"
                )[:400],
                # ── Mining-specific ───────────────────────────────────────────
                "Event Type":       sig.get("event_type", ""),
                "Severity Score":   severity,
                "Signal ID":        int(sig.get("id", 0)),
            })

    if not rows:
        return pd.DataFrame()

    pulse = pd.DataFrame(rows)

    # Deduplicate: keep highest-scoring row per stock symbol
    pulse = (
        pulse.sort_values("Final Trade Score", ascending=False)
             .drop_duplicates(subset=["Stock Trade"])
             .reset_index(drop=True)
    )

    return pulse


# ─── Deactivate stale signals ─────────────────────────────────────────────────

def deactivate_stale_signals(days: int = 7) -> int:
    """Mark signals older than `days` as inactive."""
    if not DATABASE_URL:
        return 0
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE mining_signals
                    SET is_active = FALSE
                    WHERE is_active = TRUE
                      AND last_updated < NOW() - INTERVAL '1 day' * %s
                    """,
                    (days,),
                )
                n = cur.rowcount
            conn.commit()
        return n
    except Exception:
        return 0


# ─── CLI entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Mining News Monitor — full scan ===")
    n = scan_all_regions()
    print(f"=== Done: {n} signal(s) ===")
