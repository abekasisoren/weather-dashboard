"""
alt_data_monitor.py — Alternative Data Signal Scanner

Monitors six free public data sources for market-moving events BEFORE
financial media prices them in:

  Source              | Latency  | Auth          | What it detects
  --------------------|----------|---------------|-------------------------------
  SEC EDGAR 8-K       | ~4h      | None          | Company material disclosures
  NASA FIRMS          | 3h       | None          | Wildfire near commodity zones
  GDELT Labor (14xx)  | 15min    | None          | Strikes in mining/energy regions
  NRC Incidents       | ~24h     | None          | US pipeline/refinery accidents
  LME Warehouse       | daily    | NASDAQ key*   | Metal inventory drawdowns
  USDA Crop Progress  | weekly   | USDA key*     | Crop condition deterioration

  * = free API key; source is silently skipped when key is missing

Standalone:
    python alt_data_monitor.py       # run all scanners
"""

from __future__ import annotations

import csv
import io
import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import psycopg

DATABASE_URL         = os.environ.get("DATABASE_URL")
NASDAQ_DATA_LINK_KEY = os.environ.get("NASDAQ_DATA_LINK_KEY")
USDA_NASS_KEY        = os.environ.get("USDA_NASS_KEY")

# ─── DB Schema ────────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS alt_data_signals (
    id               SERIAL PRIMARY KEY,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    last_updated     TIMESTAMPTZ DEFAULT NOW(),
    source           TEXT NOT NULL,
    signal_type      TEXT,
    title            TEXT,
    summary          TEXT,
    url              TEXT,
    region           TEXT,
    commodity        TEXT,
    severity_score   INTEGER DEFAULT 1,
    signal_level     INTEGER DEFAULT 1,
    signal_bucket    TEXT DEFAULT 'WATCH',
    trade_bias       TEXT DEFAULT 'long',
    affected_tickers JSONB DEFAULT '[]',
    raw_data         JSONB DEFAULT '{}',
    is_active        BOOLEAN DEFAULT TRUE,
    dedup_key        TEXT UNIQUE
)
"""

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_alt_source   ON alt_data_signals(source)",
    "CREATE INDEX IF NOT EXISTS idx_alt_active   ON alt_data_signals(is_active, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_alt_dedup    ON alt_data_signals(dedup_key)",
]

_SOURCE_LABELS = {
    "sec_8k":        "SEC 8-K",
    "nasa_firms":    "NASA FIRMS",
    "gdelt_labor":   "GDELT Labor",
    "nrc_incident":  "NRC Report",
    "lme_warehouse": "LME Warehouse",
    "usda_crops":    "USDA Crops",
}

def ensure_alt_schema():
    if not DATABASE_URL:
        return
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(_CREATE_TABLE)
        for idx in _INDEXES:
            conn.execute(idx)
        conn.commit()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _severity_to_level(score: int) -> tuple[int, str]:
    if score >= 8: return 4, "EXTREME"
    if score >= 6: return 3, "STRONG"
    if score >= 4: return 2, "MODERATE"
    return 1, "WATCH"


def _upsert(signal: dict) -> bool:
    """Persist signal; upsert on dedup_key. Returns True if written."""
    if not DATABASE_URL:
        return False
    lvl, bucket = _severity_to_level(signal.get("severity_score", 1))
    signal["signal_level"]  = lvl
    signal["signal_bucket"] = bucket
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("""
            INSERT INTO alt_data_signals
              (source, signal_type, title, summary, url, region, commodity,
               severity_score, signal_level, signal_bucket, trade_bias,
               affected_tickers, raw_data, dedup_key)
            VALUES
              (%(source)s, %(signal_type)s, %(title)s, %(summary)s, %(url)s,
               %(region)s, %(commodity)s, %(severity_score)s, %(signal_level)s,
               %(signal_bucket)s, %(trade_bias)s,
               %(affected_tickers)s::jsonb, %(raw_data)s::jsonb, %(dedup_key)s)
            ON CONFLICT (dedup_key) DO UPDATE SET
              last_updated   = NOW(),
              severity_score = EXCLUDED.severity_score,
              signal_level   = EXCLUDED.signal_level,
              signal_bucket  = EXCLUDED.signal_bucket,
              title          = EXCLUDED.title,
              summary        = EXCLUDED.summary,
              is_active      = TRUE
        """, {
            **signal,
            "affected_tickers": json.dumps(signal.get("affected_tickers", [])),
            "raw_data":         json.dumps(signal.get("raw_data", {})),
        })
        conn.commit()
    return True


def _fetch_json(url: str, timeout: int = 15) -> Optional[dict]:
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "weather-trading-radar contact@example.com"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  [fetch] {url[:60]}… → {e}")
        return None


def _today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ─── Stock / Commodity Maps ───────────────────────────────────────────────────

COMMODITY_STOCKS: dict[str, dict] = {
    # Metals
    "copper":      {"long": ["FCX", "SCCO", "HBM", "COPX"],          "short": []},
    "aluminum":    {"long": ["AA", "CENX"],                            "short": []},
    "zinc":        {"long": ["HBM", "TRQ"],                            "short": []},
    "nickel":      {"long": ["VALE", "NILSY"],                         "short": []},
    "iron_ore":    {"long": ["VALE", "BHP", "RIO", "CLF"],            "short": []},
    "gold":        {"long": ["GLD", "GDX", "NEM", "GOLD"],            "short": []},
    "silver":      {"long": ["SLV", "PAAS", "WPM"],                   "short": []},
    "lithium":     {"long": ["LTHM", "ALB", "SQM", "PLL"],            "short": []},
    # Energy
    "crude_oil":   {"long": [],                                        "short": ["XOM", "CVX", "WMB"]},
    "natural_gas": {"long": ["UNG", "WMB", "KMI"],                    "short": []},
    "coal":        {"long": ["ARCH", "CEIX", "BTU"],                   "short": []},
    # Agriculture
    "soybeans":    {"long": ["SOYB", "ADM", "BG"],                    "short": []},
    "corn":        {"long": ["CORN", "DE", "NTR", "MOS", "CF"],       "short": []},
    "wheat":       {"long": ["WEAT", "ADM"],                           "short": []},
    "cotton":      {"long": ["BAL", "MOO"],                            "short": []},
    "fertilizer":  {"long": ["NTR", "MOS", "CF", "SMG"],              "short": []},
    "palm_oil":    {"long": ["DBA", "MOO"],                            "short": []},
    # Other
    "timber":      {"long": ["WY", "RYN", "PCH"],                     "short": []},
    "power":       {"long": [],                                        "short": ["PCG", "EIX", "SRE"]},
    "reinsurance": {"long": [],                                        "short": ["RNR", "AXS", "MKL"]},
    "shipping":    {"long": ["ZIM", "GOGL", "FRO"],                   "short": []},
    "chemical":    {"long": [],                                        "short": ["DOW", "LYB"]},
}


def _tickers(commodity: str, bias: str = "long") -> list[str]:
    entry = COMMODITY_STOCKS.get(commodity, {})
    picks = entry.get(bias, [])
    return picks if picks else entry.get("long", [])


# ─── 1. SEC EDGAR 8-K ─────────────────────────────────────────────────────────

_SEC_KW_WEIGHT: dict[str, int] = {
    "mine closure": 3,        "force majeure": 3,     "tailings dam": 3,
    "pipeline rupture": 3,    "production halt": 2,   "temporary suspension": 2,
    "supply disruption": 2,   "facility damage": 2,   "environmental shutdown": 2,
    "seismic event": 2,       "earthquake damage": 2, "mine fire": 2,
    "plant closure": 2,       "labor dispute": 1,     "adverse weather": 1,
    "flooding at": 1,         "underground event": 1,
}


def _sec_score(text: str) -> tuple[int, str]:
    tl = text.lower()
    score, hits = 0, []
    for kw, w in _SEC_KW_WEIGHT.items():
        if kw in tl:
            score += w
            hits.append(kw)
    return score, ", ".join(hits)


def _sec_commodity(text: str) -> str:
    t = text.lower()
    if any(w in t for w in ["copper", "cuprum"]):            return "copper"
    if any(w in t for w in ["lithium", "spodumene"]):        return "lithium"
    if any(w in t for w in ["gold mine", "gold ounce"]):     return "gold"
    if any(w in t for w in ["iron ore", "iron pellet"]):     return "iron_ore"
    if any(w in t for w in ["nickel sulphide", "nickel"]):   return "nickel"
    if any(w in t for w in ["aluminium", "aluminum", "bauxite"]): return "aluminum"
    if any(w in t for w in ["zinc mine", "zinc"]):           return "zinc"
    if any(w in t for w in ["pipeline", "crude oil", "barrel", "petroleum"]): return "crude_oil"
    if any(w in t for w in ["natural gas", "lng", "methane"]): return "natural_gas"
    if any(w in t for w in ["coal mine", "coking coal"]):    return "coal"
    if any(w in t for w in ["fertilizer", "ammonia", "potash"]): return "fertilizer"
    if any(w in t for w in ["timber", "lumber", "sawmill"]): return "timber"
    return "unknown"


def scan_sec_edgar(lookback_days: int = 3) -> int:
    """
    Scan SEC EDGAR full-text search for 8-K filings containing supply-
    disruption keywords.  No API key required.
    """
    n = 0
    start = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end   = _today_str()

    for kw in [
        "mine closure", "force majeure", "supply disruption",
        "pipeline rupture", "tailings dam", "production halt",
    ]:
        q   = urllib.parse.quote(f'"{kw}"')
        url = (f"https://efts.sec.gov/LATEST/search-index?q={q}"
               f"&dateRange=custom&startdt={start}&enddt={end}&forms=8-K")
        data = _fetch_json(url)
        if not data:
            continue

        for hit in data.get("hits", {}).get("hits", [])[:10]:
            src     = hit.get("_source", {})
            text    = " ".join(filter(None, [
                src.get("period_of_report", ""),
                src.get("display_names", ""),
                src.get("biz_descriptions") or "",
            ]))
            score, matched = _sec_score(kw + " " + text)
            if score < 2:
                continue

            cik       = src.get("entity_id", "unknown")
            filed_at  = src.get("file_date", end)
            commodity = _sec_commodity(text)
            ticker    = src.get("ticker", "").upper() or None
            tix       = _tickers(commodity)
            if ticker:
                tix = list({ticker} | set(tix))

            dedup = f"sec_{cik}_{filed_at}_{kw[:12].replace(' ', '_')}"
            signal = {
                "source":           "sec_8k",
                "signal_type":      "material_event",
                "title":            f"📋 {src.get('display_names', 'Company')} — 8-K: {matched[:60]}",
                "summary":          f"Filed {filed_at}. Keywords matched: {matched}.",
                "url":              (f"https://www.sec.gov/cgi-bin/browse-edgar"
                                     f"?action=getcompany&CIK={cik}&type=8-K&dateb=&owner=include&count=5"),
                "region":           "United States",
                "commodity":        commodity,
                "severity_score":   min(10, 3 + score),
                "trade_bias":       "long",
                "affected_tickers": tix,
                "raw_data":         {"cik": cik, "filed": filed_at, "matched": matched},
                "dedup_key":        dedup,
            }
            if _upsert(signal):
                n += 1

        time.sleep(0.4)   # respect EDGAR rate limits

    return n


# ─── 2. NASA FIRMS Active Fire ────────────────────────────────────────────────

FIRMS_ZONES = [
    # name, (lon_min, lat_min, lon_max, lat_max), commodity, trade_bias, tickers
    ("Pacific Northwest",    (-125, 45, -115, 50),  "timber",      "short", ["WY", "RYN", "PCH"]),
    ("Northern California",  (-124, 37, -119, 42),  "power",       "short", ["PCG", "EIX"]),
    ("Southern California",  (-121, 32, -115, 37),  "reinsurance", "short", ["RNR", "AXS", "MKL", "SRE"]),
    ("Rocky Mountains",      (-115, 37, -100, 48),  "coal",        "long",  ["ARCH", "CEIX", "BTU"]),
    ("Australian Pilbara",   (115,  -26, 122, -20), "iron_ore",    "long",  ["BHP", "RIO", "VALE"]),
    ("SE Australia NSW",     (143,  -38, 153, -28), "coal",        "long",  ["WHC.AX", "BTU"]),
    ("Indonesian Sumatra",   (95,   -6,  108,   6), "palm_oil",    "long",  ["DBA", "MOO"]),
    ("Brazilian Cerrado",    (-55,  -20, -38,   -5),"soybeans",    "long",  ["ADM", "BG", "AGRO3.SA"]),
    ("Chilean Norte Chico",  (-72,  -32, -67,  -20),"copper",      "long",  ["FCX", "SCCO", "COPX"]),
    ("Canadian Boreal",      (-130,  50, -60,   65),"timber",      "short", ["WY", "IFP.TO"]),
    ("South Africa Highveld",(24,   -28,  32,  -22),"gold",        "long",  ["GFI", "GOLD", "GDX"]),
    ("West Africa Sahel",    (-18,    5,  15,   20),"gold",        "long",  ["GDX", "SSEZY"]),
    ("Siberia East",         (100,   50, 140,   70),"natural_gas", "long",  ["UNG", "WMB"]),
]


def scan_nasa_firms() -> int:
    """
    Download VIIRS 7-day global fire CSV (no auth) and count high-confidence
    fire pixels inside each commodity zone.
    """
    url = ("https://firms.modaps.eosdis.nasa.gov/data/active_fire"
           "/viirs-i/csv/VIIRS_I_Global_7d.csv")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "weather-trading-radar"})
        with urllib.request.urlopen(req, timeout=90) as r:
            raw = r.read().decode("utf-8")
    except Exception as e:
        print(f"  [FIRMS] download error: {e}")
        return 0

    rows = list(csv.DictReader(io.StringIO(raw)))
    if not rows:
        return 0

    n, today = 0, _today_str()

    for name, (lon0, lat0, lon1, lat1), commodity, bias, tickers in FIRMS_ZONES:
        zone_fires = []
        for r in rows:
            try:
                lat = float(r.get("latitude",  0))
                lon = float(r.get("longitude", 0))
                conf = r.get("confidence", "").lower()
            except ValueError:
                continue
            if lon0 <= lon <= lon1 and lat0 <= lat <= lat1 and conf in ("high", "h", "nominal", "n"):
                zone_fires.append(r)

        if not zone_fires:
            continue

        count   = len(zone_fires)
        frp_sum = sum(float(r.get("frp", 0) or 0) for r in zone_fires)

        sev = min(10, 3
                  + int(math.log10(max(count, 1)) * 2)
                  + (2 if frp_sum > 500 else 1 if frp_sum > 100 else 0))
        if sev < 4:
            continue

        dedup = f"firms_{name.replace(' ', '_')}_{today}"
        signal = {
            "source":           "nasa_firms",
            "signal_type":      "wildfire",
            "title":            f"🔥 Active Fire: {name} — {count} detections",
            "summary":          (f"{count} VIIRS high-confidence fire pixels in {name}. "
                                 f"FRP total: {frp_sum:.0f} MW. "
                                 f"Commodity exposure: {commodity}."),
            "url":              "https://firms.modaps.eosdis.nasa.gov/map/",
            "region":           name,
            "commodity":        commodity,
            "severity_score":   sev,
            "trade_bias":       bias,
            "affected_tickers": tickers,
            "raw_data":         {"pixel_count": count, "frp_mw": frp_sum},
            "dedup_key":        dedup,
        }
        if _upsert(signal):
            n += 1

    return n


# ─── 3. GDELT Labor Unrest (event codes 14xx) ─────────────────────────────────

_LABOR_REGIONS: dict[str, dict] = {
    "CL": {"name": "Chile",        "commodity": "copper",   "tickers": ["FCX", "SCCO", "COPX"]},
    "ZM": {"name": "Zambia",       "commodity": "copper",   "tickers": ["FCX", "COPX"]},
    "CD": {"name": "DRC",          "commodity": "copper",   "tickers": ["CMCL", "COPX"]},
    "ZA": {"name": "South Africa", "commodity": "gold",     "tickers": ["GFI", "GOLD", "GDX"]},
    "AU": {"name": "Australia",    "commodity": "iron_ore", "tickers": ["BHP", "RIO", "VALE"]},
    "PE": {"name": "Peru",         "commodity": "copper",   "tickers": ["SCCO", "FCX", "COPX"]},
    "ID": {"name": "Indonesia",    "commodity": "nickel",   "tickers": ["VALE", "NILSY"]},
    "PH": {"name": "Philippines",  "commodity": "nickel",   "tickers": ["NILSY"]},
    "KZ": {"name": "Kazakhstan",   "commodity": "copper",   "tickers": ["ERG.L", "COPX"]},
    "RU": {"name": "Russia",       "commodity": "nickel",   "tickers": ["NILSY", "VALE"]},
    "CA": {"name": "Canada",       "commodity": "gold",     "tickers": ["NEM", "GOLD", "GDX"]},
    "MX": {"name": "Mexico",       "commodity": "silver",   "tickers": ["PAAS", "WPM", "SLV"]},
    "GN": {"name": "Guinea",       "commodity": "aluminum", "tickers": ["AA", "CENX"]},
}


def scan_gdelt_labor() -> int:
    """
    Query GDELT DOC API for labor unrest / strike articles in key
    mining/energy producing countries.
    """
    n, today = 0, _today_str()

    for cc, meta in _LABOR_REGIONS.items():
        q = urllib.parse.quote(
            f'"{meta["name"]}" (strike OR "labor dispute" OR "mine closure" '
            f'OR protest OR blockade) mining OR mine'
        )
        url = (f"https://api.gdeltproject.org/api/v2/doc/doc"
               f"?query={q}&mode=artlist&maxrecords=10&timespan=3d&format=json")
        data = _fetch_json(url)
        if not data:
            continue

        arts = data.get("articles", [])
        if not arts:
            continue

        count   = len(arts)
        top_art = arts[0]
        sev     = min(10, 3 + count)
        if sev < 4:
            continue

        dedup = f"gdelt_labor_{cc}_{today}"
        signal = {
            "source":           "gdelt_labor",
            "signal_type":      "labor_unrest",
            "title":            f"⚒️ Labor Unrest: {meta['name']} — {count} articles (3d)",
            "summary":          top_art.get("title", "Labor unrest detected in mining region."),
            "url":              top_art.get("url", "https://gdeltproject.org"),
            "region":           meta["name"],
            "commodity":        meta["commodity"],
            "severity_score":   sev,
            "trade_bias":       "long",
            "affected_tickers": meta["tickers"],
            "raw_data":         {"country": cc, "article_count": count,
                                 "top_headline": top_art.get("title", "")},
            "dedup_key":        dedup,
        }
        if _upsert(signal):
            n += 1

        time.sleep(0.3)

    return n


# ─── 4. NRC Industrial Incident Reports ───────────────────────────────────────

_NRC_MAP = [
    (["crude oil", "petroleum", "oil spill"],          "crude_oil",   ["XOM", "CVX", "WMB"],   "short", "Oil Spill"),
    (["natural gas", "gas leak", "gas pipeline"],      "natural_gas", ["WMB", "KMI", "UNG"],   "long",  "Gas Incident"),
    (["anhydrous ammonia", "ammonia"],                 "fertilizer",  ["NTR", "MOS", "CF"],    "long",  "Ammonia Release"),
    (["coal slurry", "coal ash"],                      "coal",        ["ARCH", "CEIX", "BTU"], "long",  "Coal Incident"),
    (["chlorine", "chemical spill", "toxic release"],  "chemical",    ["DOW", "LYB"],          "short", "Chemical Spill"),
    (["pipeline rupture", "pipeline failure"],         "crude_oil",   ["KMI", "WMB", "ENB"],  "short", "Pipeline Failure"),
    (["diesel", "fuel oil", "bunker fuel"],            "crude_oil",   ["VLO", "MPC", "PSX"],  "short", "Fuel Spill"),
]


def _scan_nrc_api() -> int:
    """Try NRC REST API; returns signal count or -1 on failure."""
    today    = datetime.now(timezone.utc)
    week_ago = today - timedelta(days=7)
    url = (f"https://nrc.uscg.mil/api/v1/incidents"
           f"?fromDate={urllib.parse.quote(week_ago.strftime('%m/%d/%Y'))}"
           f"&toDate={urllib.parse.quote(today.strftime('%m/%d/%Y'))}"
           f"&pageSize=100")
    data = _fetch_json(url, timeout=20)
    if data is None:
        return -1

    items = data if isinstance(data, list) else data.get("incidents", data.get("data", []))
    if not isinstance(items, list):
        return -1

    n, today_str = 0, _today_str()
    for inc in items[:100]:
        mat   = (inc.get("materialName") or inc.get("material", "") or "").lower()
        loc   = inc.get("locationDesc") or inc.get("location") or ""
        qty   = inc.get("quantity") or 0
        desc  = (inc.get("description") or inc.get("callType", "") or "").lower()
        inc_no = str(inc.get("incidentNumber") or inc.get("id", "x"))

        full = f"{mat} {desc}"
        matched = next((m for kws, *_ , _ in _NRC_MAP for kw in kws if kw in full
                        for m in [next((x for x in _NRC_MAP if kw in x[0]), None)] if m), None)
        if matched is None:
            for row in _NRC_MAP:
                if any(kw in full for kw in row[0]):
                    matched = row
                    break
        if not matched:
            continue

        sev = min(10, 4 + (2 if qty > 10000 else 1 if qty > 1000 else 0)
                       + (1 if "pipeline" in full else 0)
                       + (1 if any(w in full for w in ["offshore", "gulf"]) else 0))
        dedup = f"nrc_{inc_no}"
        signal = {
            "source":           "nrc_incident",
            "signal_type":      matched[4].lower().replace(" ", "_"),
            "title":            f"⚠️ NRC: {matched[4]} — {loc or 'United States'}",
            "summary":          f"Incident #{inc_no}: {mat or desc}. Quantity: {qty or '?'}.",
            "url":              "https://nrc.uscg.mil/",
            "region":           loc or "United States",
            "commodity":        matched[1],
            "severity_score":   sev,
            "trade_bias":       matched[3],
            "affected_tickers": matched[2],
            "raw_data":         {"inc_no": inc_no, "material": mat, "qty": qty, "loc": loc},
            "dedup_key":        dedup,
        }
        if _upsert(signal):
            n += 1

    return n


def _scan_nrc_via_gdelt() -> int:
    """Fallback: find US industrial incidents via GDELT 24h scan."""
    n, today = 0, _today_str()
    queries = [
        ("pipeline spill rupture United States",          "crude_oil",   ["KMI", "WMB"]),
        ("refinery fire explosion United States",         "crude_oil",   ["VLO", "MPC", "PSX"]),
        ("chemical plant explosion release United States","chemical",    ["DOW", "LYB"]),
        ("ammonia release plant United States",           "fertilizer",  ["NTR", "MOS", "CF"]),
    ]
    for q_text, commodity, tickers in queries:
        q   = urllib.parse.quote(q_text)
        url = (f"https://api.gdeltproject.org/api/v2/doc/doc"
               f"?query={q}&mode=artlist&maxrecords=5&timespan=24h&format=json")
        data = _fetch_json(url)
        if not data:
            continue
        for art in data.get("articles", [])[:2]:
            title = art.get("title", "")
            dedup = f"nrc_gdelt_{abs(hash(title)) % 999983}_{today}"
            signal = {
                "source":           "nrc_incident",
                "signal_type":      "industrial_incident",
                "title":            f"⚠️ {title[:90]}",
                "summary":          title,
                "url":              art.get("url", ""),
                "region":           "United States",
                "commodity":        commodity,
                "severity_score":   5,
                "trade_bias":       "short",
                "affected_tickers": tickers,
                "raw_data":         {"headline": title},
                "dedup_key":        dedup,
            }
            if _upsert(signal):
                n += 1
        time.sleep(0.3)
    return n


def scan_nrc_reports() -> int:
    result = _scan_nrc_api()
    if result < 0:
        print("  [NRC] API unavailable — using GDELT fallback")
        return _scan_nrc_via_gdelt()
    return result


# ─── 5. LME Warehouse Inventory ───────────────────────────────────────────────

_LME_METALS = {
    "copper":   {"ndl": "LME/PR_CU", "tickers": ["FCX", "SCCO", "HBM", "COPX"]},
    "aluminum": {"ndl": "LME/PR_AH", "tickers": ["AA", "CENX"]},
    "zinc":     {"ndl": "LME/PR_ZS", "tickers": ["HBM", "TRQ"]},
    "nickel":   {"ndl": "LME/PR_NI", "tickers": ["VALE", "NILSY"]},
    "lead":     {"ndl": "LME/PR_PB", "tickers": ["HBM"]},
}


def scan_lme_warehouse() -> int:
    """
    Fetch LME warehouse stock data via NASDAQ Data Link (free API key).
    Signals on weekly drawdowns > 3%.
    Skipped silently if NASDAQ_DATA_LINK_KEY is not set.
    """
    if not NASDAQ_DATA_LINK_KEY:
        print("  [LME] NASDAQ_DATA_LINK_KEY not set — skipping")
        return 0

    n     = 0
    today = _today_str()
    ago_7 = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

    for metal, meta in _LME_METALS.items():
        url = (f"https://data.nasdaq.com/api/v3/datasets/{meta['ndl']}.json"
               f"?start_date={ago_7}&end_date={today}&api_key={NASDAQ_DATA_LINK_KEY}")
        data = _fetch_json(url)
        if not data:
            continue

        ds   = data.get("dataset", {})
        rows = ds.get("data", [])
        cols = [c.lower() for c in ds.get("column_names", [])]

        # Find the warehouse stock column
        stock_idx = next(
            (i for i, c in enumerate(cols) if "stock" in c or "warrant" in c),
            None,
        )
        if stock_idx is None or len(rows) < 2:
            continue

        try:
            latest = float(rows[0][stock_idx])
            oldest = float(rows[-1][stock_idx])
        except (TypeError, ValueError, IndexError):
            continue

        if oldest == 0:
            continue
        chg_pct = (latest - oldest) / oldest * 100
        if chg_pct > -3:        # only signal significant drawdown
            continue

        sev   = min(10, int(abs(chg_pct) / 2) + 3)
        dedup = f"lme_{metal}_{today}"
        signal = {
            "source":           "lme_warehouse",
            "signal_type":      "inventory_drawdown",
            "title":            f"📦 LME {metal.title()} Stocks: {chg_pct:+.1f}% (7d)",
            "summary":          (f"LME {metal} warehouse stocks fell {abs(chg_pct):.1f}% "
                                 f"over 7 days ({oldest:,.0f}t → {latest:,.0f}t). "
                                 f"Supply tightness signal."),
            "url":              "https://www.lme.com/en/market-data/reports-and-data",
            "region":           "Global",
            "commodity":        metal,
            "severity_score":   sev,
            "trade_bias":       "long",
            "affected_tickers": meta["tickers"],
            "raw_data":         {"metal": metal, "latest_t": latest,
                                 "oldest_t": oldest, "chg_pct": chg_pct},
            "dedup_key":        dedup,
        }
        if _upsert(signal):
            n += 1

    return n


# ─── 6. USDA Crop Progress ────────────────────────────────────────────────────

_USDA_CROPS = {
    "CORN":     {"commodity": "corn",     "tickers": ["CORN", "DE", "NTR", "MOS", "CF"]},
    "SOYBEANS": {"commodity": "soybeans", "tickers": ["SOYB", "ADM", "BG"]},
    "WHEAT":    {"commodity": "wheat",    "tickers": ["WEAT", "ADM"]},
    "COTTON":   {"commodity": "cotton",   "tickers": ["BAL", "MOO"]},
}


def scan_usda_crops() -> int:
    """
    Query USDA NASS API for crop Good/Excellent percentage week-over-week.
    Signals on drops >= 3pp.
    Skipped silently if USDA_NASS_KEY is not set.
    """
    if not USDA_NASS_KEY:
        print("  [USDA] USDA_NASS_KEY not set — skipping")
        return 0

    n       = 0
    cur_yr  = datetime.now(timezone.utc).year

    for crop, meta in _USDA_CROPS.items():
        params = urllib.parse.urlencode({
            "key":               USDA_NASS_KEY,
            "commodity_desc":    crop,
            "statisticcat_desc": "CONDITION",
            "class_desc":        "GOOD",
            "agg_level_desc":    "NATIONAL",
            "year":              cur_yr,
            "format":            "json",
        })
        data = _fetch_json(f"https://quickstats.nass.usda.gov/api/api_GET/?{params}")
        if not data:
            continue

        items = data.get("data", [])
        if len(items) < 2:
            continue

        items = sorted(items, key=lambda x: x.get("week_ending", ""), reverse=True)
        try:
            latest = float(items[0].get("Value", "0").replace(",", "") or 0)
            prev   = float(items[1].get("Value", "0").replace(",", "") or 0)
        except ValueError:
            continue

        change = latest - prev
        if change > -3:
            continue

        sev      = min(10, int(abs(change)) + 3)
        week_end = items[0].get("week_ending", "unknown")
        dedup    = f"usda_{crop}_{week_end}"
        signal = {
            "source":           "usda_crops",
            "signal_type":      "crop_stress",
            "title":            f"🌾 USDA {crop.title()} Good/Excellent: {change:+.1f}pp",
            "summary":          (f"USDA NASS {crop.title()}: {prev:.1f}% → {latest:.1f}% "
                                 f"Good/Excellent (week ending {week_end}). Crop stress."),
            "url":              "https://www.nass.usda.gov/Charts_and_Maps/Crop_Progress_&_Condition/",
            "region":           "US National",
            "commodity":        meta["commodity"],
            "severity_score":   sev,
            "trade_bias":       "long",
            "affected_tickers": meta["tickers"],
            "raw_data":         {"crop": crop, "latest_pct": latest,
                                 "prev_pct": prev, "change_pp": change},
            "dedup_key":        dedup,
        }
        if _upsert(signal):
            n += 1

    return n


# ─── DB Read & Pulse Builder ──────────────────────────────────────────────────

def get_active_alt_signals(hours: int = 168) -> pd.DataFrame:
    """Return active alt-data signals from last N hours, newest first."""
    if not DATABASE_URL:
        return pd.DataFrame()
    with psycopg.connect(DATABASE_URL) as conn:
        df = pd.read_sql(
            f"""
            SELECT * FROM alt_data_signals
            WHERE is_active = TRUE
              AND created_at >= NOW() - INTERVAL '{hours} hours'
            ORDER BY severity_score DESC, created_at DESC
            """,
            conn,
        )
    return df


def deactivate_stale_alt_signals(days: int = 7):
    if not DATABASE_URL:
        return
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(
            "UPDATE alt_data_signals SET is_active = FALSE "
            "WHERE created_at < NOW() - INTERVAL %s AND is_active = TRUE",
            (f"{days} days",),
        )
        conn.commit()


def build_alt_pulse_table(signals_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert alt-data signals into a Pulse Trader-compatible DataFrame
    with the same column structure as weather and mining pulses.
    """
    if signals_df.empty:
        return pd.DataFrame()

    rows = []
    for _, sig in signals_df.iterrows():
        sev   = int(sig.get("severity_score", 1))
        bias  = str(sig.get("trade_bias", "long")).lower()
        bucket = str(sig.get("signal_bucket", "WATCH"))
        source_lbl = _SOURCE_LABELS.get(str(sig.get("source", "")), "Alt Data")

        # Trend badge from severity
        if sev >= 8:
            trend = "↑ ESCALATING"
        elif sev >= 6:
            trend = "★ NEW"
        elif sev >= 4:
            trend = "→ STABLE"
        else:
            trend = "↓ RECOVERING"

        # Tickers
        try:
            tix = sig.get("affected_tickers")
            if isinstance(tix, str):
                tix = json.loads(tix)
            tix = tix or []
        except Exception:
            tix = []

        trade_dir = "Long" if bias == "long" else "Short"
        score = round(min(10.0, sev * 0.8 + len(tix) * 0.1), 1)

        for ticker in (tix[:5] if tix else ["—"]):
            rows.append({
                "Source":           f"🛰️ {source_lbl}",
                "Stock Trade":      ticker,   # matches Pulse Trader column name
                "Region":           str(sig.get("region", "")),
                "Anomaly":          str(sig.get("signal_type", "")),
                "Commodity":        str(sig.get("commodity", "")).title(),
                "Trade":            trade_dir,
                "Trend":            trend,
                "Signal Level":     bucket,
                "Final Trade Score": score,
                "Entry Gate":       ("🟢 Enter — Escalating" if sev >= 7
                                     else "🟡 Monitor" if sev >= 5
                                     else "🔴 Avoid — Fading"),
                "Why It Matters":   str(sig.get("title", ""))[:100],  # matches Pulse Trader column
            })

    if not rows:
        return pd.DataFrame()

    return (pd.DataFrame(rows)
              .drop_duplicates(subset=["Source", "Stock Trade", "Anomaly"])
              .reset_index(drop=True))


# ─── Master Scanner ───────────────────────────────────────────────────────────

def scan_all_alt_data() -> dict[str, int]:
    """Run every scanner. Returns {source_name: signal_count}."""
    ensure_alt_schema()
    results: dict[str, int] = {}

    print("[ALT DATA] Scanning SEC EDGAR 8-K filings…")
    results["sec_8k"] = scan_sec_edgar()

    print("[ALT DATA] Scanning NASA FIRMS active fires…")
    results["nasa_firms"] = scan_nasa_firms()

    print("[ALT DATA] Scanning GDELT labor unrest…")
    results["gdelt_labor"] = scan_gdelt_labor()

    print("[ALT DATA] Scanning NRC industrial incidents…")
    results["nrc_incident"] = scan_nrc_reports()

    print("[ALT DATA] Scanning LME warehouse inventories…")
    results["lme_warehouse"] = scan_lme_warehouse()

    print("[ALT DATA] Scanning USDA crop progress…")
    results["usda_crops"] = scan_usda_crops()

    deactivate_stale_alt_signals(days=7)

    total = sum(results.values())
    print(f"[ALT DATA] Complete — {total} signals across "
          f"{sum(1 for v in results.values() if v > 0)} active sources")
    return results


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    results = scan_all_alt_data()
    print("\nSummary:")
    for src, n in results.items():
        label = _SOURCE_LABELS.get(src, src)
        print(f"  {label:<20} {n} signal(s)")
