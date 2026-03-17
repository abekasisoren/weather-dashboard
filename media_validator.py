"""
media_validator.py — Weather signal media validation stub.

Validates weather trading signals against published news headlines (NewsAPI)
and official government weather alerts (NOAA/NWS).

Configuration via environment variables:
  NEWSAPI_KEY   — newsapi.org API key
  NOAA_API_KEY  — NOAA/NWS API key (free, register at api.weather.gov)

When keys are absent, all methods return None (graceful no-op).
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Optional


# ─── Result types ─────────────────────────────────────────────────────────────

@dataclass
class MediaResult:
    validated: bool
    source: str
    headline: str
    score: float  # 0-10: confidence of match
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

# Maps anomaly types to search query keywords for news matching
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

# Maps regions to NOAA-style zone identifiers or geographic context terms
REGION_CONTEXT: dict[str, list[str]] = {
    "US Midwest":            ["midwest", "corn belt", "iowa", "illinois", "indiana"],
    "US Southern Plains":    ["southern plains", "texas", "oklahoma", "kansas", "wheat belt"],
    "Brazil":                ["brazil", "mato grosso", "cerrado", "Brazilian"],
    "Mato Grosso":           ["mato grosso", "Brazilian soy", "Brazil corn"],
    "Argentina Pampas":      ["argentina", "pampas", "buenos aires", "Argentine"],
    "Europe Gas Belt":       ["europe", "european gas", "germany", "france", "UK energy"],
    "Black Sea":             ["black sea", "ukraine wheat", "russia grain", "black sea grain"],
    "India":                 ["india", "monsoon india", "indian subcontinent"],
    "Australia East":        ["australia", "queensland", "new south wales", "Australian"],
    "US Gulf":               ["gulf of mexico", "gulf coast", "houston", "louisiana"],
    "Southeast US":          ["southeast US", "florida", "georgia", "carolinas"],
    "California":            ["california", "pacific coast", "bay area", "los angeles"],
    "West Africa Cocoa Belt":["ivory coast", "ghana", "cocoa belt", "west africa"],
    "Southeast Asia":        ["southeast asia", "malaysia", "indonesia", "palm oil"],
    "Canadian Prairies":     ["canadian prairies", "alberta", "saskatchewan", "canada wheat"],
    "Middle East Gulf":      ["middle east", "saudi arabia", "gulf states", "persian gulf"],
    "North Sea":             ["north sea", "norway", "uk offshore", "equinor"],
    "East Africa":           ["ethiopia", "kenya", "east africa", "african coffee"],
    "US Pacific Northwest":  ["pacific northwest", "oregon", "washington state", "columbia river"],
    "China Yangtze Basin":   ["china", "yangtze", "chinese floods", "china flooding"],
    "Southern Europe":       ["mediterranean", "spain", "italy", "greek", "southern europe"],
}


# ─── Validator class ─────────────────────────────────────────────────────────

class MediaValidator:
    """
    Interface for media validation of weather trade signals.

    Supports:
    - NewsAPI: checks for recent news headlines matching anomaly + region
    - NOAA/NWS: checks for official active weather alerts in region
    """

    def __init__(self):
        self.newsapi_key: Optional[str] = os.environ.get("NEWSAPI_KEY")
        self._newsapi_available = self.newsapi_key is not None
        # NOAA/NWS alerts API is free and public — no key required
        self._noaa_available = True

    @property
    def is_configured(self) -> bool:
        return True  # NOAA always works; NewsAPI is optional bonus

    def check_newsapi(
        self,
        region: str,
        anomaly: str,
        commodity: str,
        lookback_hours: int = 48,
    ) -> Optional[MediaResult]:
        """
        Query NewsAPI for headlines matching the weather event.
        Returns MediaResult if match found, None if unavailable or no match.
        """
        if not self._newsapi_available:
            return None

        # Build search query
        keywords = ANOMALY_KEYWORDS.get(anomaly, [anomaly.replace("_", " ")])
        region_terms = REGION_CONTEXT.get(region, [region])
        query = f"({keywords[0]}) AND ({region_terms[0]})"

        try:
            from_date = (datetime.now(UTC) - timedelta(hours=lookback_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
            params = urllib.parse.urlencode({
                "q": query,
                "from": from_date,
                "sortBy": "relevancy",
                "language": "en",
                "pageSize": 5,
                "apiKey": self.newsapi_key,
            })
            url = f"https://newsapi.org/v2/everything?{params}"

            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read().decode())

            articles = data.get("articles", [])
            if not articles:
                return None

            best = articles[0]
            title = best.get("title", "")
            headline_lower = title.lower()

            # Score based on keyword matches
            score = 0.0
            for kw in keywords:
                if kw.lower() in headline_lower:
                    score += 2.5
            for term in region_terms:
                if term.lower() in headline_lower:
                    score += 2.5
            score = min(10.0, score)

            if score >= 5.0:
                raw_dt = best.get("publishedAt")
                published = datetime.fromisoformat(raw_dt.replace("Z", "+00:00")) if raw_dt else None
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

    def check_noaa_alerts(
        self,
        region: str,
    ) -> list[MediaResult]:
        """
        Check NOAA/NWS active alerts API for the given region.
        Returns list of MediaResult for any active alerts.
        """
        region_terms = REGION_CONTEXT.get(region, [])
        if not region_terms:
            return []

        results = []
        try:
            url = "https://api.weather.gov/alerts/active?status=actual&message_type=alert&limit=50"
            headers = {
                "User-Agent": "WeatherRadar/1.0 (weather-trading-radar)",
                "Accept": "application/geo+json",
            }
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())

            features = data.get("features", [])
            for feature in features:
                props = feature.get("properties", {})
                headline = props.get("headline", "") or props.get("description", "")[:200]
                area_desc = (props.get("areaDesc", "") or "").lower()

                # Match region context terms to alert area
                matched = any(term.lower() in area_desc for term in region_terms)
                if matched and headline:
                    results.append(MediaResult(
                        validated=True,
                        source="NOAA/NWS",
                        headline=headline[:300],
                        score=8.0,  # Official alerts are high confidence
                        url=props.get("id", ""),
                        published_at=None,
                    ))

            return results[:3]  # Return top 3

        except Exception:
            return []

    def validate_signal(
        self,
        signal_id: Optional[int],
        region: str,
        anomaly: str,
        commodity: str,
    ) -> ValidationSummary:
        """
        Run all available validation sources against a single signal.
        Returns a ValidationSummary with all results combined.
        """
        summary = ValidationSummary(
            signal_id=signal_id,
            region=region,
            anomaly=anomaly,
            commodity=commodity,
        )

        # NewsAPI check
        news_result = self.check_newsapi(region, anomaly, commodity)
        if news_result:
            summary.results.append(news_result)

        # NOAA/NWS check — free public API, always available for US regions
        US_REGIONS = {
            "US Midwest", "US Southern Plains", "US Gulf",
            "Southeast US", "California", "US Pacific Northwest",
        }
        if region in US_REGIONS:
            noaa_results = self.check_noaa_alerts(region)
            summary.results.extend(noaa_results)

        return summary

    def validate_batch(
        self,
        signals: list[dict],
    ) -> dict[int, ValidationSummary]:
        """
        Validate a list of signal dicts (each must have: id, region, anomaly_type, commodity).
        Returns dict mapping signal id → ValidationSummary.
        """

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
    Write media validation result back to weather_global_shocks table.
    Call this after validate_signal() to persist the result.
    """
    best = summary.best_result
    if best is None:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE weather_global_shocks
            SET
                media_validated = %s,
                media_source     = %s,
                media_headline   = %s,
                media_score      = %s
            WHERE id = %s
            """,
            (
                True,
                best.source,
                best.headline[:500] if best.headline else None,
                best.score,
                signal_id,
            ),
        )
        conn.commit()


# ─── Standalone test ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    validator = MediaValidator()
    print(f"  NewsAPI: {'yes' if validator._newsapi_available else 'no (set NEWSAPI_KEY)'}")
    print(f"  NOAA:    always available (free public API)")

    test = validator.validate_signal(
        signal_id=None,
        region="US Midwest",
        anomaly="drought",
        commodity="Corn",
    )
    print(f"\nTest validation — US Midwest drought:")
    print(f"  Confirmed: {test.is_confirmed}")
    if test.best_result:
        print(f"  Source: {test.best_result.source}")
        print(f"  Headline: {test.best_result.headline}")
        print(f"  Score: {test.best_result.score:.1f}")
    else:
        print("  No confirmation found.")
