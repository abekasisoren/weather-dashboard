"""
policy_monitor.py — Finance Minister & Central Bank Statement Monitor

Scans GDELT every 15 minutes for market-moving statements by finance ministers,
central bank governors, and senior economic officials across all major economies.

Detects and classifies:
  hawkish       — rate hike signals → long banks, short TLT/utilities/REITs
  dovish        — rate cut signals  → long TLT/gold/utilities, short banks
  trade_war     — tariff/trade escalation → short tech/semis, long gold
  stimulus      — fiscal spending   → long cyclicals/industrials
  stability_risk— banking system stress → long gold, short financials
  currency_move — intervention/devaluation → commodities rally
  sanctions     — new restrictions  → long energy, short EM

Why this beats the market:
  GDELT indexes Reuters/FT/Bloomberg articles within 15 minutes of publication.
  A finance minister press conference at 10:00am is indexed by 10:15am — before
  most retail traders or slower systematic strategies react to the full context.

Cron: every 15 minutes  →  python policy_monitor.py
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL")

# ─── DB Schema ────────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS policy_signals (
    id               SERIAL PRIMARY KEY,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    last_updated     TIMESTAMPTZ DEFAULT NOW(),
    official_name    TEXT,
    official_title   TEXT,
    country          TEXT,
    official_type    TEXT,          -- central_bank | treasury | executive | multilateral
    signal_type      TEXT,          -- hawkish | dovish | trade_war | stimulus | stability_risk | currency_move | sanctions
    sentiment_score  INTEGER,       -- 1-10, higher = stronger signal
    headline         TEXT,
    summary          TEXT,
    url              TEXT,
    published_at     TEXT,
    keywords_matched TEXT,          -- comma-separated matched keywords
    long_tickers     JSONB DEFAULT '[]',
    short_tickers    JSONB DEFAULT '[]',
    trade_rationale  TEXT,
    is_active        BOOLEAN DEFAULT TRUE,
    dedup_key        TEXT UNIQUE
)
"""

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_policy_active ON policy_signals(is_active, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_policy_type   ON policy_signals(signal_type)",
    "CREATE INDEX IF NOT EXISTS idx_policy_dedup  ON policy_signals(dedup_key)",
]


def ensure_policy_schema():
    if not DATABASE_URL:
        return
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(_CREATE_TABLE)
        for idx in _INDEXES:
            conn.execute(idx)
        conn.commit()


# ─── Officials Registry ───────────────────────────────────────────────────────
# (name, title, country_code, official_type, weight)
# weight 3 = global market mover (single statement can move indices)
# weight 2 = significant (moves sector/country ETF reliably)
# weight 1 = regional (moves single-country or EM assets)

OFFICIALS = [
    # ══ United States ══════════════════════════════════════════════════════════
    # President & cabinet — each tweet/statement can move SPY ±1%
    ("Trump",           "US President",                "US", "executive",     3),
    ("Powell",          "Fed Chair",                   "US", "central_bank",  3),
    ("Bessent",         "US Treasury Secretary",       "US", "treasury",      3),
    ("Lutnick",         "US Commerce Secretary",       "US", "executive",     2),  # tariff authority
    ("Navarro",         "US Trade Advisor",            "US", "executive",     2),  # trade war architect
    ("Goolsbee",        "Chicago Fed President",       "US", "central_bank",  2),
    ("Waller",          "Fed Governor",                "US", "central_bank",  2),
    ("Jefferson",       "Fed Vice Chair",              "US", "central_bank",  2),
    ("Williams",        "NY Fed President",            "US", "central_bank",  2),
    ("Daly",            "SF Fed President",            "US", "central_bank",  2),
    ("Musalem",         "St Louis Fed President",      "US", "central_bank",  2),
    ("Schmid",          "Kansas City Fed President",   "US", "central_bank",  2),
    ("Kugler",          "Fed Governor",                "US", "central_bank",  2),

    # ══ Euro Zone ══════════════════════════════════════════════════════════════
    ("Lagarde",         "ECB President",               "EU", "central_bank",  3),
    ("von der Leyen",   "EU Commission President",     "EU", "executive",     2),  # fiscal/trade policy
    ("de Guindos",      "ECB Vice President",          "EU", "central_bank",  2),
    ("Schnabel",        "ECB Board",                   "EU", "central_bank",  2),
    ("Villeroy",        "Bank of France Governor",     "FR", "central_bank",  2),
    ("Knot",            "Dutch Central Bank Governor", "NL", "central_bank",  2),
    ("Nagel",           "Bundesbank President",        "DE", "central_bank",  2),
    ("Rehn",            "Bank of Finland Governor",    "FI", "central_bank",  1),
    ("Wunsch",          "National Bank Belgium",       "BE", "central_bank",  1),
    ("Kukies",          "Germany FinMin",              "DE", "treasury",      2),
    ("Merz",            "German Chancellor",           "DE", "executive",     2),
    ("Macron",          "France President",            "FR", "executive",     2),

    # ══ United Kingdom ═════════════════════════════════════════════════════════
    ("Bailey",          "BoE Governor",                "UK", "central_bank",  3),
    ("Reeves",          "UK Chancellor",               "UK", "treasury",      2),
    ("Mann",            "BoE MPC Member",              "UK", "central_bank",  1),
    ("Dhingra",         "BoE MPC Member",              "UK", "central_bank",  1),

    # ══ Japan ══════════════════════════════════════════════════════════════════
    ("Ueda",            "BoJ Governor",                "JP", "central_bank",  3),
    ("Kato",            "Japan Finance Minister",      "JP", "treasury",      2),
    ("Suzuki",          "Japan Finance Minister",      "JP", "treasury",      2),  # name may rotate
    ("Kanda",           "Japan VP Finance for FX",     "JP", "treasury",      3),  # calls yen intervention

    # ══ China ══════════════════════════════════════════════════════════════════
    ("Xi",              "China President",             "CN", "executive",     3),
    ("Pan Gongsheng",   "PBOC Governor",               "CN", "central_bank",  3),
    ("Lan Fo'an",       "China Finance Minister",      "CN", "treasury",      2),
    ("Li Qiang",        "China Premier",               "CN", "executive",     2),
    ("He Lifeng",       "China Economic Tsar",         "CN", "executive",     2),

    # ══ Canada ═════════════════════════════════════════════════════════════════
    ("Macklem",         "Bank of Canada Governor",     "CA", "central_bank",  2),
    ("Carney",          "Canada PM",                   "CA", "executive",     2),
    ("Freeland",        "Canada Deputy PM",            "CA", "treasury",      2),

    # ══ Australia ══════════════════════════════════════════════════════════════
    ("Bullock",         "RBA Governor",                "AU", "central_bank",  2),
    ("Chalmers",        "Australia Treasurer",         "AU", "treasury",      2),

    # ══ India ══════════════════════════════════════════════════════════════════
    ("Malhotra",        "RBI Governor",                "IN", "central_bank",  2),
    ("Das",             "Former RBI Governor",         "IN", "central_bank",  1),
    ("Sitharaman",      "India Finance Minister",      "IN", "treasury",      2),
    ("Modi",            "India Prime Minister",        "IN", "executive",     2),

    # ══ Brazil ═════════════════════════════════════════════════════════════════
    ("Galipolo",        "BCB Governor",                "BR", "central_bank",  2),
    ("Haddad",          "Brazil Finance Minister",     "BR", "treasury",      2),
    ("Lula",            "Brazil President",            "BR", "executive",     2),

    # ══ Saudi Arabia / Gulf ════════════════════════════════════════════════════
    ("bin Salman",      "Saudi Crown Prince",          "SA", "executive",     3),  # OPEC+ & oil price
    ("Al-Jadaan",       "Saudi Finance Minister",      "SA", "treasury",      2),
    ("Al-Kholifey",     "SAMA Governor",               "SA", "central_bank",  2),

    # ══ South Korea ════════════════════════════════════════════════════════════
    ("Choi Sang-mok",   "Korea Finance Minister",      "KR", "treasury",      2),
    ("Rhee Chang-yong", "Bank of Korea Governor",      "KR", "central_bank",  2),

    # ══ Turkey ═════════════════════════════════════════════════════════════════
    ("Erdogan",         "Turkey President",            "TR", "executive",     2),  # overrides CBbank
    ("Simsek",          "Turkey Finance Minister",     "TR", "treasury",      2),
    ("Karahan",         "TCMB Governor",               "TR", "central_bank",  2),

    # ══ Mexico ═════════════════════════════════════════════════════════════════
    ("Sheinbaum",       "Mexico President",            "MX", "executive",     2),
    ("Alcocer",         "Banxico Governor",            "MX", "central_bank",  2),
    ("Heath",           "Banxico Deputy Governor",     "MX", "central_bank",  1),

    # ══ Argentina ══════════════════════════════════════════════════════════════
    ("Milei",           "Argentina President",         "AR", "executive",     2),  # peso/IMF watch
    ("Caputo",          "Argentina Finance Minister",  "AR", "treasury",      2),

    # ══ South Africa ═══════════════════════════════════════════════════════════
    ("Kganyago",        "SARB Governor",               "ZA", "central_bank",  2),
    ("Godongwana",      "South Africa FinMin",         "ZA", "treasury",      1),

    # ══ Russia ═════════════════════════════════════════════════════════════════
    ("Nabiullina",      "Russia CBR Governor",         "RU", "central_bank",  2),
    ("Siluanov",        "Russia Finance Minister",     "RU", "treasury",      2),

    # ══ Middle East / Geopolitical ═════════════════════════════════════════════
    ("Netanyahu",       "Israel Prime Minister",       "IL", "executive",     2),  # oil risk premium
    ("Khamenei",        "Iran Supreme Leader",         "IR", "executive",     2),  # oil sanctions
    ("Pezeshkian",      "Iran President",              "IR", "executive",     1),

    # ══ Nigeria / Africa ═══════════════════════════════════════════════════════
    ("Edun",            "Nigeria Finance Minister",    "NG", "treasury",      1),
    ("Cardoso",         "CBN Governor",                "NG", "central_bank",  1),

    # ══ Indonesia ══════════════════════════════════════════════════════════════
    ("Indrawati",       "Indonesia Finance Minister",  "ID", "treasury",      2),
    ("Warjiyo",         "Bank Indonesia Governor",     "ID", "central_bank",  1),

    # ══ IMF / World Bank / BIS ═════════════════════════════════════════════════
    ("Georgieva",       "IMF Managing Director",       "INT","multilateral",  2),
    ("Banga",           "World Bank President",        "INT","multilateral",  2),
    ("Carstens",        "BIS General Manager",         "INT","multilateral",  2),  # central bank of CBs

    # ══ OPEC / Energy ══════════════════════════════════════════════════════════
    ("Al Ghais",        "OPEC Secretary General",      "INT","multilateral",  2),

    # ══ Market Voices (not officials, but statements move markets) ═════════════
    ("Dimon",           "JPMorgan CEO",                "US", "market_voice",  2),
    ("Fink",            "BlackRock CEO",               "US", "market_voice",  2),
    ("Buffett",         "Berkshire Hathaway CEO",      "US", "market_voice",  2),
    ("Dalio",           "Bridgewater Founder",         "US", "market_voice",  1),
]

# Country-level ETF proxies for localised signals
COUNTRY_ETFS: dict[str, list[str]] = {
    "US": ["SPY", "QQQ"],
    "EU": ["EZU", "FEZ"],
    "UK": ["EWU"],
    "JP": ["EWJ", "DXJ"],
    "CN": ["MCHI", "FXI"],
    "DE": ["EWG"],
    "FR": ["EWQ"],
    "IN": ["INDA", "INDY"],
    "BR": ["EWZ"],
    "CA": ["EWC"],
    "AU": ["EWA"],
    "SA": ["KSA"],
    "TR": ["TUR"],
    "KR": ["EWY"],
    "MX": ["EWW"],
    "ZA": ["EZA"],
    "RU": ["RSX"],
    "NL": ["EWN"],      # Netherlands (Knot)
    "AR": ["ARGT"],     # Argentina (Milei, Caputo)
    "IL": ["EIS"],      # Israel (Netanyahu)
    "IR": [],           # Iran — no liquid ETF
    "NG": ["NGE"],      # Nigeria (Edun, Cardoso)
    "ID": ["EIDO"],     # Indonesia (Indrawati, Warjiyo)
}

# ─── Sentiment Keyword Dictionaries ──────────────────────────────────────────

HAWKISH: dict[str, int] = {
    "rate hike": 3,         "hike rates": 3,        "raise rates": 3,
    "higher for longer": 3, "not in a hurry": 2,    "premature to cut": 3,
    "tighten": 2,           "restrictive policy": 2,"above neutral": 2,
    "inflation concern": 2, "inflation risk": 2,    "overshoot": 2,
    "overheating": 2,       "fight inflation": 2,   "price stability": 1,
    "not considering cuts": 3, "pause": 1,          "vigilant": 1,
    "upside risk": 2,       "persistent inflation": 2,
}

DOVISH: dict[str, int] = {
    "rate cut": 3,          "cut rates": 3,         "lower rates": 3,
    "pivot": 2,             "easing": 2,            "accommodation": 2,
    "support growth": 2,    "stimulus": 2,          "quantitative easing": 3,
    "asset purchases": 2,   "below target": 2,      "growth concerns": 1,
    "recession risk": 2,    "downside risk": 2,     "data dependent": 1,
    "considering cuts": 3,  "appropriate to cut": 3,"gradual easing": 2,
    "insurance cut": 2,     "pre-emptive": 2,       "soft landing": 1,
}

TRADE_WAR: dict[str, int] = {
    "tariff": 3,            "trade war": 3,         "retaliatory tariff": 3,
    "import duty": 2,       "trade restriction": 2, "export control": 2,
    "trade deficit": 1,     "decoupling": 2,        "protectionist": 2,
    "reciprocal tariff": 3, "trade barrier": 2,     "sanctions": 2,
    "trade friction": 2,    "trade tension": 2,     "tariff retaliation": 3,
}

STIMULUS: dict[str, int] = {
    "fiscal stimulus": 3,   "spending package": 2,  "infrastructure": 1,
    "tax cut": 2,           "budget deficit": 1,    "government spending": 2,
    "economic package": 2,  "relief package": 2,    "investment plan": 1,
    "fiscal expansion": 3,  "helicopter money": 3,  "direct payment": 2,
}

STABILITY_RISK: dict[str, int] = {
    "financial stability": 2, "systemic risk": 3,   "bank stress": 3,
    "capital flight": 3,      "currency crisis": 3, "debt crisis": 3,
    "banking sector concern": 3, "credit crunch": 3,"liquidity crisis": 3,
    "contagion": 3,           "bank run": 3,        "solvency": 2,
    "too big to fail": 2,     "bailout": 2,         "financial stress": 2,
}

CURRENCY_MOVE: dict[str, int] = {
    "intervention": 3,      "currency weakness": 2, "devaluation": 3,
    "fx intervention": 3,   "currency manipulation": 3, "managed float": 2,
    "weak dollar": 2,       "strong dollar": 2,     "currency floor": 2,
    "verbal intervention": 2, "disorderly": 2,      "excessive volatility": 2,
}

SANCTIONS: dict[str, int] = {
    "new sanctions": 3,     "impose sanctions": 3,  "economic sanction": 3,
    "asset freeze": 3,      "export ban": 2,        "oil embargo": 3,
    "financial sanction": 3,"tech restriction": 2,  "entity list": 2,
}

ALL_SENTIMENT_DICTS = [
    ("hawkish",        HAWKISH,       "Rate hike signal"),
    ("dovish",         DOVISH,        "Rate cut signal"),
    ("trade_war",      TRADE_WAR,     "Trade war escalation"),
    ("stimulus",       STIMULUS,      "Fiscal stimulus"),
    ("stability_risk", STABILITY_RISK,"Financial stability concern"),
    ("currency_move",  CURRENCY_MOVE, "Currency intervention"),
    ("sanctions",      SANCTIONS,     "New sanctions"),
]

# ─── Signal → Stock Mapping ───────────────────────────────────────────────────

SIGNAL_TRADE_MAP: dict[str, dict] = {
    "hawkish": {
        "long":        ["XLF", "JPM", "BAC", "GS", "C"],
        "short":       ["TLT", "XLU", "VNQ", "ARKK", "IEF"],
        "rationale":   "Rates rising — financials benefit from wider spreads; "
                       "bond proxies (utilities, REITs, long-duration bonds) sold off.",
    },
    "dovish": {
        "long":        ["TLT", "XLU", "VNQ", "GLD", "IEF"],
        "short":       ["XLF", "JPM", "BAC"],
        "rationale":   "Rates falling — bond proxies rally; banks face margin compression.",
    },
    "trade_war": {
        "long":        ["GLD", "SLV", "USO", "LIT"],
        "short":       ["AAPL", "NVDA", "SMH", "TSM", "SOXX"],
        "rationale":   "Tariff escalation — tech/semis exposed to supply chain disruption; "
                       "safe havens and domestic commodity plays bid.",
    },
    "stimulus": {
        "long":        ["XLI", "XLB", "XLE", "IYT", "CAT", "DE"],
        "short":       [],
        "rationale":   "Fiscal spending — cyclicals, industrials, materials benefit "
                       "from government contract flows.",
    },
    "stability_risk": {
        "long":        ["GLD", "TLT", "SHV"],
        "short":       ["XLF", "KRE", "IAT"],
        "rationale":   "Banking system stress — safe-haven rotation out of financials "
                       "into gold and short-duration Treasuries.",
    },
    "currency_move": {
        "long":        ["GLD", "SLV", "FCX", "USO"],
        "short":       ["EFA", "EEM"],
        "rationale":   "Currency weakness — dollar-denominated commodities rally "
                       "in local-currency terms; EM equities pressured.",
    },
    "sanctions": {
        "long":        ["USO", "UNG", "GLD", "LMT", "RTX"],
        "short":       ["EEM", "RSX", "ERUS"],
        "rationale":   "New sanctions — energy supply risk; defense names bid; "
                       "EM and target-country equities sold.",
    },
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _fetch_json(url: str, timeout: int = 8) -> Optional[dict]:
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "policy-monitor contact@example.com"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  [fetch] {url[:55]}… → {e}")
        return None


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _classify_sentiment(text: str) -> tuple[str, int, str]:
    """
    Score text against all sentiment dictionaries.
    Returns (signal_type, score, matched_keywords).
    """
    tl = text.lower()
    best_type, best_score, best_matched = "unknown", 0, ""

    for sig_type, kw_dict, _ in ALL_SENTIMENT_DICTS:
        score, hits = 0, []
        for kw, weight in kw_dict.items():
            if kw in tl:
                score += weight
                hits.append(kw)
        if score > best_score:
            best_type    = sig_type
            best_score   = score
            best_matched = ", ".join(hits)

    return best_type, best_score, best_matched


def _build_tickers(signal_type: str, country: str) -> tuple[list[str], list[str]]:
    """Merge generic signal tickers with country-specific ETFs."""
    base   = SIGNAL_TRADE_MAP.get(signal_type, {})
    long_t = list(base.get("long",  []))
    short_t = list(base.get("short", []))
    # Add country ETF to the directional side (long for growth signals, short for risk)
    etfs   = COUNTRY_ETFS.get(country, [])
    if signal_type in ("hawkish", "stimulus"):
        long_t = list({*long_t, *etfs})
    elif signal_type in ("stability_risk", "sanctions"):
        short_t = list({*short_t, *etfs})
    return long_t[:8], short_t[:6]   # cap list lengths


def _upsert(signal: dict) -> bool:
    if not DATABASE_URL:
        return False
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("""
            INSERT INTO policy_signals
              (official_name, official_title, country, official_type,
               signal_type, sentiment_score, headline, summary, url,
               published_at, keywords_matched, long_tickers, short_tickers,
               trade_rationale, dedup_key)
            VALUES
              (%(official_name)s, %(official_title)s, %(country)s, %(official_type)s,
               %(signal_type)s, %(sentiment_score)s, %(headline)s, %(summary)s,
               %(url)s, %(published_at)s, %(keywords_matched)s,
               %(long_tickers)s::jsonb, %(short_tickers)s::jsonb,
               %(trade_rationale)s, %(dedup_key)s)
            ON CONFLICT (dedup_key) DO UPDATE SET
               last_updated    = NOW(),
               sentiment_score = EXCLUDED.sentiment_score,
               signal_type     = EXCLUDED.signal_type,
               keywords_matched = EXCLUDED.keywords_matched,
               is_active       = TRUE
        """, {
            **signal,
            "long_tickers":  json.dumps(signal.get("long_tickers",  [])),
            "short_tickers": json.dumps(signal.get("short_tickers", [])),
        })
        conn.commit()
    return True


# ─── GDELT Scanner ────────────────────────────────────────────────────────────

def _scan_official(
    name: str,
    title: str,
    country: str,
    official_type: str,
    weight: int,
    lookback_minutes: int = 60,
) -> int:
    """
    Query GDELT for recent articles mentioning this official with
    market-relevant language. Returns number of signals stored.
    """
    # Build query: official name + economic context terms
    q = urllib.parse.quote(
        f'"{name}" '
        f'(rate OR inflation OR tariff OR stimulus OR "interest rate" '
        f'OR deficit OR "monetary policy" OR "fiscal policy" '
        f'OR currency OR devaluation OR sanctions OR "financial stability")'
    )
    timespan = f"{lookback_minutes}min"
    url = (f"https://api.gdeltproject.org/api/v2/doc/doc"
           f"?query={q}&mode=artlist&maxrecords=10"
           f"&timespan={timespan}&format=json")

    data = _fetch_json(url, timeout=8)
    if not data:
        return 0

    articles = data.get("articles", [])
    if not articles:
        return 0

    n = 0
    today = _today()

    for art in articles[:5]:    # top 5 per official per scan
        headline = art.get("title", "")
        url_art  = art.get("url", "")
        pub_date = art.get("seendate", today)

        if not headline:
            continue

        # Classify the article text
        sig_type, score, matched = _classify_sentiment(headline)
        if sig_type == "unknown" or score < 2:
            continue    # no clear market-relevant signal

        # Weight by official importance
        weighted_score = min(10, score + weight - 1)

        long_t, short_t = _build_tickers(sig_type, country)
        trade_info = SIGNAL_TRADE_MAP.get(sig_type, {})

        # Dedup key: official + article URL hash to avoid re-inserting same article
        dedup = f"policy_{name.replace(' ', '_')}_{abs(hash(url_art)) % 999983}_{today}"

        signal = {
            "official_name":   name,
            "official_title":  title,
            "country":         country,
            "official_type":   official_type,
            "signal_type":     sig_type,
            "sentiment_score": weighted_score,
            "headline":        headline[:200],
            "summary":         f"{title} ({country}): {matched}",
            "url":             url_art,
            "published_at":    pub_date,
            "keywords_matched":matched,
            "long_tickers":    long_t,
            "short_tickers":   short_t,
            "trade_rationale": trade_info.get("rationale", ""),
            "dedup_key":       dedup,
        }
        if _upsert(signal):
            n += 1

    return n


# ─── Main Scanner ─────────────────────────────────────────────────────────────

def scan_policy_statements(lookback_minutes: int = 60) -> int:
    """
    Scan all registered officials for recent market-moving statements.
    lookback_minutes: how far back to query GDELT (default: last 60 min so
    no overlap between 15-min cron runs with some buffer).
    """
    ensure_policy_schema()
    total = 0

    # Prioritise high-weight officials first so if we hit rate limits the
    # most important ones are already captured.
    sorted_officials = sorted(OFFICIALS, key=lambda x: x[4], reverse=True)

    for name, title, country, off_type, weight in sorted_officials:
        n = _scan_official(name, title, country, off_type, weight, lookback_minutes)
        if n:
            print(f"  [{country}] {name} ({title}): {n} signal(s)")
        total += n
        time.sleep(0.2)   # gentle rate-limiting

    return total


def get_recent_policy_signals(hours: int = 24) -> pd.DataFrame:
    """Return active policy signals from the last N hours."""
    if not DATABASE_URL:
        return pd.DataFrame()
    with psycopg.connect(DATABASE_URL) as conn:
        df = pd.read_sql(
            f"""
            SELECT * FROM policy_signals
            WHERE is_active = TRUE
              AND created_at >= NOW() - INTERVAL '{hours} hours'
            ORDER BY sentiment_score DESC, created_at DESC
            """,
            conn,
        )
    return df


def deactivate_old_policy_signals(hours: int = 48):
    """Mark signals older than N hours as inactive."""
    if not DATABASE_URL:
        return
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(
            "UPDATE policy_signals SET is_active = FALSE "
            "WHERE created_at < NOW() - INTERVAL %s AND is_active = TRUE",
            (f"{hours} hours",),
        )
        conn.commit()


def build_policy_pulse_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert policy signals to Pulse Trader-compatible DataFrame.
    Each row = one ticker (long or short) from the signal.
    """
    if df.empty:
        return pd.DataFrame()

    rows = []
    for _, sig in df.iterrows():
        sev     = int(sig.get("sentiment_score", 1))
        stype   = str(sig.get("signal_type", ""))
        name    = str(sig.get("official_name", ""))
        country = str(sig.get("country", ""))
        title   = str(sig.get("official_title", ""))
        rationale = str(sig.get("trade_rationale", ""))
        headline  = str(sig.get("headline", ""))[:90]
        url       = str(sig.get("url", ""))

        try:
            long_t  = json.loads(sig.get("long_tickers",  "[]") or "[]")
            short_t = json.loads(sig.get("short_tickers", "[]") or "[]")
        except Exception:
            long_t, short_t = [], []

        trend = "↑ ESCALATING" if sev >= 8 else "★ NEW" if sev >= 6 else "→ STABLE"

        _sig_icons = {
            "hawkish":        "🦅",
            "dovish":         "🕊️",
            "trade_war":      "⚔️",
            "stimulus":       "💰",
            "stability_risk": "🚨",
            "currency_move":  "💱",
            "sanctions":      "🔒",
        }
        icon = _sig_icons.get(stype, "🏛️")

        for ticker in (long_t[:4] if long_t else []):
            rows.append({
                "Source":            f"🏛️ Policy",
                "Stock Trade":       ticker,
                "Region":            f"{country} — {name}",
                "Anomaly":           f"{icon} {stype}",
                "Commodity":         title,
                "Trade":             "Long",
                "Trend":             trend,
                "Signal Level":      "STRONG" if sev >= 7 else "MODERATE",
                "Final Trade Score": round(sev * 0.85, 1),
                "Entry Gate":        "🟢 Enter — Escalating" if sev >= 7 else "🟡 Monitor",
                "Why It Matters":    headline,
            })
        for ticker in (short_t[:3] if short_t else []):
            rows.append({
                "Source":            f"🏛️ Policy",
                "Stock Trade":       ticker,
                "Region":            f"{country} — {name}",
                "Anomaly":           f"{icon} {stype}",
                "Commodity":         title,
                "Trade":             "Short",
                "Trend":             trend,
                "Signal Level":      "STRONG" if sev >= 7 else "MODERATE",
                "Final Trade Score": round(sev * 0.85, 1),
                "Entry Gate":        "🟢 Enter — Escalating" if sev >= 7 else "🟡 Monitor",
                "Why It Matters":    headline,
            })

    if not rows:
        return pd.DataFrame()
    return (pd.DataFrame(rows)
              .drop_duplicates(subset=["Source", "Stock Trade", "Anomaly"])
              .reset_index(drop=True))


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Policy Monitor — scanning last 60 min ===")
    n = scan_policy_statements(lookback_minutes=60)
    deactivate_old_policy_signals(hours=48)
    print(f"=== Done: {n} new signal(s) ===")
