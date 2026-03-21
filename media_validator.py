"""
media_validator.py — Weather signal media validation + 4x-daily auto-monitor.

Validates weather trading signals against published news headlines (NewsAPI,
Google News RSS) and official government weather alerts (NOAA/NWS).

When media picks up a weather event, it signals the EXIT window for the trade
("buy the rumor, sell the news"). The monitor runs 4x daily via a Render cron
job and marks confirmed events in the DB with media_pickup_at + article URL.

Configuration via environment variables:
  NEWSAPI_KEY   — newsapi.org API key (optional — Google RSS used as fallback)
  DATABASE_URL  — PostgreSQL connection string (required for scheduled monitor)

Usage:
  # Run the scheduled monitor (called by Render cron job):
  python media_validator.py

  # Import in dashboard:
  from media_validator import MediaValidator, write_validation_to_db
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Optional


# ─── Result types ─────────────────────────────────────────────────────────────

@dataclass
class MediaResult:
    validated: bool
    source: str
    headline: str
    score: float          # 0-10: confidence of match
    url: str = ""
    published_at: Optional[datetime] = None


@dataclass
class ValidationSummary:
    signal_id: Optional[int]
    region: str
    anomaly: str
    commodity: str
    results: list[MediaResult] = field(default_factory=list)

    @property
    def is_confirmed(self) -> bool:
        return any(r.validated for r in self.results)

    @property
    def best_result(self) -> Optional[MediaResult]:
        confirmed = [r for r in self.results if r.validated]
        if not confirmed:
            return None
        return max(confirmed, key=lambda r: r.score)


# ─── Keyword mapping ──────────────────────────────────────────────────────────

ANOMALY_KEYWORDS: dict[str, list[str]] = {
    "heatwave":          ["heatwave", "heat wave", "extreme heat", "record temperature"],
    "extreme_heat":      ["extreme heat", "record heat", "dangerous heat", "heat emergency"],
    "frost":             ["frost", "freeze", "freezing temperatures", "crop freeze"],
    "cold_wave":         ["cold wave", "cold snap", "arctic blast", "deep freeze"],
    "polar_vortex":      ["polar vortex", "arctic vortex", "extreme cold", "polar blast"],
    "drought":           ["drought", "dry conditions", "water shortage", "crop stress"],
    "heavy_rain":        ["heavy rain", "heavy rainfall", "flooding", "severe weather"],
    "flood_risk":        ["flood risk", "flood warning", "flooding", "flash flood"],
    "flood":             ["flooding", "flood", "inundation", "storm flooding"],
    "atmospheric_river": ["atmospheric river", "bomb cyclone", "historic rainfall", "extreme precipitation"],
    "monsoon_failure":   ["monsoon failure", "monsoon delay", "below-normal rain", "drought monsoon"],
    "storm_wind":        ["severe storm", "storm damage", "high winds", "wind damage"],
    "hurricane_risk":    ["hurricane", "tropical storm", "cyclone", "hurricane warning"],
    "hurricane":         ["hurricane", "major hurricane", "cyclone", "tropical storm"],
    "wildfire_risk":     ["wildfire risk", "fire danger", "red flag warning", "fire weather"],
    "wildfire":          ["wildfire", "forest fire", "fire evacuation", "wildfire spread"],
    "tornado":           ["tornado", "tornado warning", "severe thunderstorm", "tornado outbreak"],
    "ice_storm":         ["ice storm", "freezing rain", "winter storm", "ice accumulation"],
    "extreme_wind":      ["extreme wind", "gale force", "wind storm", "offshore wind disruption"],
}

REGION_CONTEXT: dict[str, list[str]] = {
    # Original 21 regions
    "US Midwest":               ["midwest", "corn belt", "iowa", "illinois", "indiana"],
    "US Southern Plains":       ["southern plains", "texas", "oklahoma", "kansas", "wheat belt"],
    "Brazil":                   ["brazil", "mato grosso", "cerrado", "Brazilian"],
    "Mato Grosso":              ["mato grosso", "Brazilian soy", "Brazil corn"],
    "Argentina Pampas":         ["argentina", "pampas", "buenos aires", "Argentine"],
    "Europe Gas Belt":          ["europe", "european gas", "germany", "france", "UK energy"],
    "Black Sea":                ["black sea", "ukraine wheat", "russia grain", "black sea grain"],
    "India":                    ["india", "monsoon india", "indian subcontinent"],
    "Australia East":           ["australia", "queensland", "new south wales", "Australian"],
    "US Gulf":                  ["gulf of mexico", "gulf coast", "houston", "louisiana"],
    "Southeast US":             ["southeast US", "florida", "georgia", "carolinas"],
    "California":               ["california", "pacific coast", "bay area", "los angeles"],
    "West Africa Cocoa Belt":   ["ivory coast", "ghana", "cocoa belt", "west africa"],
    "Southeast Asia":           ["southeast asia", "malaysia", "indonesia", "palm oil"],
    "Canadian Prairies":        ["canadian prairies", "alberta", "saskatchewan", "canada wheat"],
    "Middle East Gulf":         ["middle east", "saudi arabia", "gulf states", "persian gulf"],
    "North Sea":                ["north sea", "norway", "uk offshore", "equinor"],
    "East Africa":              ["ethiopia", "kenya", "east africa", "african coffee"],
    "US Pacific Northwest":     ["pacific northwest", "oregon", "washington state", "columbia river"],
    "China Yangtze Basin":      ["china", "yangtze", "chinese floods", "china flooding"],
    "Southern Europe":          ["mediterranean", "spain", "italy", "greek", "southern europe"],
    # 12 new regions
    "Japan + Korean Peninsula": ["japan", "korea", "japanese typhoon", "korean peninsula"],
    "Taiwan + Western Pacific": ["taiwan", "typhoon taiwan", "western pacific", "tsmc"],
    "Northeast China":          ["northeast china", "manchuria", "heilongjiang", "china soybean"],
    "Indonesia":                ["indonesia", "jakarta", "palm oil indonesia", "kalimantan"],
    "Bangladesh + Bay of Bengal": ["bangladesh", "bay of bengal", "cyclone bangladesh", "dhaka"],
    "Kazakhstan + Central Asia":["kazakhstan", "central asia", "kazakh wheat", "steppe"],
    "Western Australia":        ["western australia", "pilbara", "port hedland", "iron ore"],
    "Panama Canal Watershed":   ["panama canal", "gatun lake", "canal drought", "shipping canal"],
    "Morocco + Northwest Africa":["morocco", "northwest africa", "phosphate morocco", "maghreb"],
    "Caribbean Basin":          ["caribbean", "cuba", "dominican republic", "caribbean oil"],
    "Texas + Permian Basin":    ["texas", "permian basin", "west texas", "midland odessa"],
    "South Africa":             ["south africa", "johannesburg", "platinum south africa", "rand"],
}


# ─── Validator class ─────────────────────────────────────────────────────────

class MediaValidator:
    """
    Validates weather events against news sources and official alerts.

    Sources (in order of preference):
    1. NewsAPI — requires NEWSAPI_KEY env var
    2. NOAA/NWS Active Alerts — free, US regions only
    3. Google News RSS — free, global, used when NewsAPI unavailable
    """

    def __init__(self):
        self.newsapi_key: Optional[str] = os.environ.get("NEWSAPI_KEY")
        self._newsapi_available = self.newsapi_key is not None

    @property
    def is_configured(self) -> bool:
        return True  # Google RSS + NOAA always work; NewsAPI is optional

    # ── NewsAPI ───────────────────────────────────────────────────────────────

    def check_newsapi(
        self,
        region: str,
        anomaly: str,
        commodity: str,
        lookback_hours: int = 168,  # 7 days for scheduled monitor
    ) -> Optional[MediaResult]:
        """Query NewsAPI for headlines matching the weather event."""
        if not self._newsapi_available:
            return None

        keywords    = ANOMALY_KEYWORDS.get(anomaly, [anomaly.replace("_", " ")])
        region_terms = REGION_CONTEXT.get(region, [region])
        query = f"({keywords[0]}) AND ({region_terms[0]})"

        try:
            from_date = (datetime.now(UTC) - timedelta(hours=lookback_hours)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            params = urllib.parse.urlencode({
                "q":        query,
                "from":     from_date,
                "sortBy":   "relevancy",
                "language": "en",
                "pageSize": 5,
                "apiKey":   self.newsapi_key,
            })
            url = f"https://newsapi.org/v2/everything?{params}"

            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())

            articles = data.get("articles", [])
            if not articles:
                return None

            best   = articles[0]
            title  = best.get("title", "")
            body   = (best.get("description", "") or "").lower()
            text   = (title + " " + body).lower()

            score = 0.0
            for kw in keywords:
                if kw.lower() in text:
                    score += 2.5
            for term in region_terms:
                if term.lower() in text:
                    score += 2.5
            score = min(10.0, score)

            if score >= 5.0:
                raw_dt    = best.get("publishedAt")
                published = (
                    datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
                    if raw_dt else None
                )
                return MediaResult(
                    validated=True,
                    source="NewsAPI",
                    headline=title,
                    score=score,
                    url=best.get("url", ""),
                    published_at=published,
                )

        except Exception:
            pass

        return None

    # ── NOAA/NWS Alerts ───────────────────────────────────────────────────────

    def check_noaa_alerts(self, region: str) -> list[MediaResult]:
        """Check NOAA/NWS active alerts API. Free, US regions only."""
        region_terms = REGION_CONTEXT.get(region, [])
        if not region_terms:
            return []

        results: list[MediaResult] = []
        try:
            url     = "https://api.weather.gov/alerts/active?status=actual&message_type=alert&limit=50"
            headers = {
                "User-Agent": "WeatherRadar/1.0 (weather-trading-radar)",
                "Accept":     "application/geo+json",
            }
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())

            for feature in data.get("features", []):
                props     = feature.get("properties", {})
                headline  = props.get("headline", "") or props.get("description", "")[:200]
                area_desc = (props.get("areaDesc", "") or "").lower()

                if any(term.lower() in area_desc for term in region_terms) and headline:
                    results.append(MediaResult(
                        validated=True,
                        source="NOAA/NWS",
                        headline=headline[:300],
                        score=8.0,
                        url=props.get("id", ""),
                        published_at=None,
                    ))

            return results[:3]

        except Exception:
            return []

    # ── Google News RSS (free global fallback) ────────────────────────────────

    def check_google_news_rss(
        self,
        region: str,
        anomaly: str,
        commodity: str,
        lookback_hours: int = 168,  # 7 days
    ) -> Optional[MediaResult]:
        """
        Search Google News RSS — free, no API key, global coverage.
        Used as fallback when NewsAPI is unavailable.
        """
        keywords     = ANOMALY_KEYWORDS.get(anomaly, [anomaly.replace("_", " ")])
        region_terms = REGION_CONTEXT.get(region, [region])

        # Build a focused query: top anomaly keyword + region term + commodity
        query = f"{keywords[0]} {region_terms[0]} {commodity}".strip()
        encoded = urllib.parse.quote(query)
        url = (
            f"https://news.google.com/rss/search"
            f"?q={encoded}&hl=en-US&gl=US&ceid=US:en"
        )

        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; WeatherTrader/1.0)",
                "Accept":     "application/rss+xml, application/xml, text/xml",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read().decode("utf-8", errors="replace")

            root  = ET.fromstring(raw)
            items = root.findall(".//item")
            if not items:
                return None

            cutoff = datetime.now(UTC) - timedelta(hours=lookback_hours)

            for item in items[:10]:
                title  = (item.findtext("title") or "").strip()
                link   = (item.findtext("link") or "").strip()
                pubraw = (item.findtext("pubDate") or "").strip()

                # Parse published date
                published: Optional[datetime] = None
                if pubraw:
                    try:
                        from email.utils import parsedate_to_datetime
                        published = parsedate_to_datetime(pubraw)
                        if published.tzinfo is None:
                            published = published.replace(tzinfo=UTC)
                        if published < cutoff:
                            continue
                    except Exception:
                        pass

                text  = title.lower()
                score = 0.0
                for kw in keywords:
                    if kw.lower() in text:
                        score += 2.0
                for term in region_terms:
                    if term.lower() in text:
                        score += 2.0
                if commodity.lower() in text:
                    score += 1.0
                score = min(10.0, score)

                if score >= 4.0:
                    return MediaResult(
                        validated=True,
                        source="Google News",
                        headline=title,
                        score=score,
                        url=link,
                        published_at=published,
                    )

        except Exception:
            pass

        return None

    # ── Unified validate_signal ───────────────────────────────────────────────

    def validate_signal(
        self,
        signal_id: Optional[int],
        region: str,
        anomaly: str,
        commodity: str,
    ) -> ValidationSummary:
        """
        Run all available sources for a single signal.
        Order: NewsAPI → NOAA/NWS → Google News RSS
        """
        summary = ValidationSummary(
            signal_id=signal_id,
            region=region,
            anomaly=anomaly,
            commodity=commodity,
        )

        # 1. NewsAPI (if key available)
        news_result = self.check_newsapi(region, anomaly, commodity)
        if news_result:
            summary.results.append(news_result)

        # 2. NOAA/NWS alerts (US regions only, free)
        US_REGIONS = {
            "US Midwest", "US Southern Plains", "US Gulf",
            "Southeast US", "California", "US Pacific Northwest",
            "Texas + Permian Basin",
        }
        if region in US_REGIONS:
            summary.results.extend(self.check_noaa_alerts(region))

        # 3. Google News RSS — free global fallback (runs even if NewsAPI found something)
        if not summary.is_confirmed or not self._newsapi_available:
            rss_result = self.check_google_news_rss(region, anomaly, commodity)
            if rss_result:
                summary.results.append(rss_result)

        return summary

    def validate_batch(
        self,
        signals: list[dict],
    ) -> dict[int, ValidationSummary]:
        """Validate a list of signal dicts (id, region, anomaly_type, commodity)."""
        results = {}
        for signal in signals:
            sid = signal.get("id")
            summary = self.validate_signal(
                signal_id=sid,
                region=signal.get("region", ""),
                anomaly=signal.get("anomaly_type", ""),
                commodity=signal.get("commodity", ""),
            )
            if sid is not None:
                results[sid] = summary
        return results


# ─── DB write helper ─────────────────────────────────────────────────────────

def write_validation_to_db(conn, signal_id: int, summary: ValidationSummary) -> None:
    """
    Persist media validation to weather_global_shocks.

    Updates ALL rows for the same (region, anomaly_type) in the last 14 days —
    not just the single row by ID. This ensures that new rows inserted by
    subsequent GRIB runs (which get fresh IDs) also carry the media confirmation,
    so the Radar card always shows the EXIT SIGNAL regardless of which row is
    selected as best_row.

    - media_pickup_at is set only once (COALESCE preserves original timestamp)
    - media_article_url is set only once (COALESCE keeps the first URL found)
    """
    best = summary.best_result
    if best is None:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE weather_global_shocks
            SET
                media_validated   = TRUE,
                media_source      = %s,
                media_headline    = %s,
                media_score       = %s,
                media_pickup_at   = COALESCE(media_pickup_at, NOW()),
                media_article_url = COALESCE(media_article_url, %s)
            WHERE
                region       = %s
                AND anomaly_type = %s
                AND created_at  >= NOW() - INTERVAL '14 days'
            """,
            (
                best.source,
                best.headline[:500] if best.headline else None,
                best.score,
                best.url or None,
                summary.region,
                summary.anomaly,
            ),
        )
        conn.commit()


# ─── Scheduled monitor (4x daily cron job) ───────────────────────────────────

def run_scheduled_monitor() -> None:
    """
    Fetch all active, unconfirmed weather events from the last 14 days.
    For each unique (region, anomaly_type) pair, run media validation.
    Persist confirmations to DB with pickup timestamp + article URL.

    Called by the Render cron job: python media_validator.py
    Runs 4x daily at 00:00, 06:00, 12:00, 18:00 UTC.
    """
    import psycopg

    DATABASE_URL = os.environ.get("DATABASE_URL")
    if not DATABASE_URL:
        print("❌ DATABASE_URL not set — skipping monitor")
        return

    print(f"🔍 Media monitor started at {datetime.now(UTC).strftime('%Y-%m-%d %H:%M')} UTC")

    # ── Step 1: Fetch active, unconfirmed events ───────────────────────────────
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (region, anomaly_type)
                        id, region, anomaly_type, commodity
                    FROM weather_global_shocks
                    WHERE
                        media_validated IS NOT TRUE
                        AND created_at >= NOW() - INTERVAL '14 days'
                        AND signal_level >= 2
                    ORDER BY region, anomaly_type, signal_level DESC
                    """
                )
                events = [
                    {"id": r[0], "region": r[1], "anomaly_type": r[2], "commodity": r[3]}
                    for r in cur.fetchall()
                ]
    except Exception as e:
        print(f"❌ DB query failed: {e}")
        return

    if not events:
        print("✅ No unconfirmed events to check")
        return

    print(f"📋 Checking {len(events)} unique (region, anomaly) pairs …")

    # ── Step 2: Validate each event ────────────────────────────────────────────
    validator   = MediaValidator()
    confirmed   = 0
    not_found   = 0

    for ev in events:
        region  = ev["region"]
        anomaly = ev["anomaly_type"]
        commodity = ev.get("commodity") or ""

        summary = validator.validate_signal(
            signal_id=ev["id"],
            region=region,
            anomaly=anomaly,
            commodity=commodity,
        )

        if summary.is_confirmed:
            best = summary.best_result
            try:
                with psycopg.connect(DATABASE_URL) as conn:
                    write_validation_to_db(conn, ev["id"], summary)
                print(
                    f"  ✅ CONFIRMED: {region} / {anomaly}"
                    f" — {best.source}: {best.headline[:80]} (score {best.score:.1f})"
                    f" — {best.url[:60] if best.url else 'no url'}"
                )
                confirmed += 1
            except Exception as e:
                print(f"  ❌ DB write failed for {region}/{anomaly}: {e}")
        else:
            print(f"  — No coverage: {region} / {anomaly}")
            not_found += 1

    # ── Step 3: Summary ────────────────────────────────────────────────────────
    print(
        f"\n📊 Done — {confirmed} confirmed, {not_found} not yet in media"
        f" ({len(events)} checked)"
    )


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # Quick sanity test (no DB required)
        validator = MediaValidator()
        print(f"NewsAPI: {'yes' if validator._newsapi_available else 'no (set NEWSAPI_KEY)'}")
        print(f"NOAA:    always available (free public API)")
        print(f"Google:  always available (free RSS)")

        test = validator.validate_signal(
            signal_id=None,
            region="US Midwest",
            anomaly="drought",
            commodity="Corn",
        )
        print(f"\nTest — US Midwest drought:")
        print(f"  Confirmed: {test.is_confirmed}")
        if test.best_result:
            r = test.best_result
            print(f"  Source:    {r.source}")
            print(f"  Headline:  {r.headline}")
            print(f"  Score:     {r.score:.1f}")
            print(f"  URL:       {r.url}")
        else:
            print("  No confirmation found.")
    else:
        # Default: run the scheduled monitor
        run_scheduled_monitor()
