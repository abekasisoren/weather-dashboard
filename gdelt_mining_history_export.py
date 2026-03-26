#!/usr/bin/env python3
"""
gdelt_mining_history_export.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Scans GDELT for the past 10 years of mining-related events and exports
everything to an Excel file.  No stock recommendations — raw events only.

Usage:
    python3 gdelt_mining_history_export.py

Output:
    mining_events_10y_YYYYMMDD.xlsx   (same directory as this script)

Runtime: ~5-10 minutes (rate-limited to avoid GDELT 429s)
"""

import datetime
import time

import requests
import pandas as pd

# ─── GDELT endpoint ──────────────────────────────────────────────────────────
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# ─── Commodities: {display_name: gdelt_search_terms} ─────────────────────────
# Keep queries SHORT (2 phrases max) so GDELT URLs stay within limits.
# Event-type filtering is done in Python after fetching — see is_relevant_event().
COMMODITIES: dict[str, str] = {
    "Copper":    '"copper mine" OR "copper mining"',
    "Gold":      '"gold mine" OR "gold mining"',
    "Iron Ore":  '"iron ore mine" OR "iron ore mining"',
    "Coal":      '"coal mine" OR "coal mining"',
    "Lithium":   '"lithium mine" OR "lithium mining"',
    "Nickel":    '"nickel mine" OR "nickel mining"',
    "Cobalt":    '"cobalt mine" OR "cobalt mining"',
    "Silver":    '"silver mine" OR "silver mining"',
    "Zinc":      '"zinc mine" OR "zinc mining"',
    "Uranium":   '"uranium mine" OR "uranium mining"',
    "Tin":       '"tin mine" OR "tin mining"',
    "Manganese": '"manganese mine" OR "manganese mining"',
}

# ─── Event-type keywords for classification (checked against title text) ──────
EVENT_TYPE_KEYWORDS: dict[str, list[str]] = {
    "Strike / Labor Dispute":    ["strike", "walkout", "stoppage", "labor dispute",
                                   "labour dispute", "union dispute", "workers protest",
                                   "miner protest", "work stoppage", "industrial action"],
    "Mine Closure / Shutdown":   ["closure", "closed", "shutdown", "suspended", "halted",
                                   "idle", "curtailed", "suspend operation", "cease operation",
                                   "put on care", "care and maintenance", "temporary halt"],
    "Accident / Collapse":       ["explosion", "collapse", "cave-in", "cave in",
                                   "disaster", "accident", "death", "fatal", "killed",
                                   "trapped", "rescue", "injury", "casualty"],
    "Tailings / Dam Failure":    ["tailings", "tailings dam", "tailings pond",
                                   "dam collapse", "dam breach", "dam failure",
                                   "slurry", "mine waste"],
    "Spill / Contamination":     ["spill", "leak", "toxic", "acid mine drainage",
                                   "contamination", "pollut", "cyanide", "heavy metal",
                                   "effluent", "water contamination"],
    "Flooding":                  ["flood", "flooding", "inundated", "inundation",
                                   "water ingress", "pumping", "dewatering"],
    "Fire":                      ["fire", "blaze", "flames", "burning mine", "mine fire",
                                   "underground fire", "spontaneous combustion"],
    "Nationalization / Seizure": ["nationaliz", "expropriat", "seized", "seizure",
                                   "takeover", "government takeover", "state control",
                                   "revoke license", "license revoked", "mining rights"],
    "Geopolitical / Sanctions":  ["sanction", "conflict", "armed group", "rebel",
                                   "militia", "coup", "geopolit", "trade restriction",
                                   "export ban", "ban on export"],
    "Production Cut / Disruption":["production cut", "output cut", "reduce output",
                                    "curtail output", "production disruption",
                                    "supply disruption", "output disruption",
                                    "below forecast", "output miss"],
    "Environmental / Regulatory":["environmental violation", "environmental fine",
                                   "regulatory", "permit suspended", "permit revoked",
                                   "epa", "environmental agency", "cleanup order"],
    "Earthquake / Natural Disaster":["earthquake", "seismic", "tremor",
                                      "landslide", "mudslide", "cyclone", "typhoon",
                                      "hurricane damage", "natural disaster"],
}

# ─── Region detection: keyword → region label ────────────────────────────────
REGION_KEYWORDS: dict[str, list[str]] = {
    "Chile":            ["chile", "chilean", "atacama", "antofagasta", "escondida"],
    "Peru":             ["peru", "peruvian", "cusco", "junin", "cerro de pasco"],
    "Congo DRC":        ["congo", "drc", "democratic republic", "katanga", "kolwezi",
                         "tenke", "kibali"],
    "Zambia":           ["zambia", "zambian", "copper belt", "copperbelt", "kitwe",
                         "ndola", "lusaka"],
    "Zimbabwe":         ["zimbabwe", "zimbabwean"],
    "South Africa":     ["south africa", "south african", "rustenburg", "witwatersrand",
                         "marikana", "limpopo", "mpumalanga"],
    "Australia":        ["australia", "australian", "queensland", "western australia",
                         "pilbara", "kimberley", "broken hill", "kalgoorlie"],
    "Brazil":           ["brazil", "brazilian", "minas gerais", "pará", "para ",
                         "brumadinho", "mariana", "carajás", "samarco"],
    "Indonesia":        ["indonesia", "indonesian", "borneo", "sulawesi", "kalimantan",
                         "papua indonesia", "grasberg"],
    "Philippines":      ["philippines", "philippine", "manila", "mindanao"],
    "China":            ["china", "chinese", "inner mongolia", "yunnan", "xinjiang",
                         "sichuan"],
    "Russia":           ["russia", "russian", "siberia", "norilsk", "ural", "krasnoyarsk"],
    "Canada":           ["canada", "canadian", "ontario", "british columbia",
                         "quebec", "alberta", "yukon", "northwest territories"],
    "Mexico":           ["mexico", "mexican", "sonora", "zacatecas", "guerrero",
                         "chihuahua"],
    "Mongolia":         ["mongolia", "mongolian", "oyu tolgoi", "ulaanbaatar"],
    "Kazakhstan":       ["kazakhstan", "kazakh", "astana", "almaty", "balkhash"],
    "Papua New Guinea": ["papua new guinea", "png", "bougainville", "porgera",
                         "ok tedi"],
    "Ghana":            ["ghana", "ghanaian", "obuasi", "accra"],
    "Mali":             ["mali", "malian", "bamako", "syama"],
    "Burkina Faso":     ["burkina faso", "burkinabe", "ouagadougou"],
    "Bolivia":          ["bolivia", "bolivian", "potosi", "uyuni"],
    "Ecuador":          ["ecuador", "ecuadorian"],
    "Argentina":        ["argentina", "argentinian", "patagonia", "neuquen"],
    "Laos":             ["laos", "lao pdr"],
    "Myanmar":          ["myanmar", "burma", "burmese"],
    "Tanzania":         ["tanzania", "tanzanian"],
    "Mozambique":       ["mozambique", "mozambican"],
    "Guinea":           ["guinea", "conakry"],
    "Senegal":          ["senegal"],
    "Niger":            ["niger", "arlit"],
    "Namibia":          ["namibia", "namibian", "rössing"],
    "USA":              ["united states", "usa", "nevada", "alaska", "arizona",
                         "wyoming", "appalachia"],
    "United Kingdom":   ["united kingdom", "uk", "wales", "scotland", "cornwall"],
    "Germany":          ["germany", "german", "ruhr"],
    "Poland":           ["poland", "polish", "silesia"],
    "Colombia":         ["colombia", "colombian"],
    "Venezuela":        ["venezuela", "venezuelan"],
}

COUNTRY_CODE_MAP: dict[str, str] = {
    "CL": "Chile", "PE": "Peru", "CD": "Congo DRC", "ZM": "Zambia",
    "ZW": "Zimbabwe", "ZA": "South Africa", "AU": "Australia",
    "BR": "Brazil", "ID": "Indonesia", "PH": "Philippines",
    "CN": "China", "RU": "Russia", "CA": "Canada", "MX": "Mexico",
    "MN": "Mongolia", "KZ": "Kazakhstan", "PG": "Papua New Guinea",
    "GH": "Ghana", "ML": "Mali", "BF": "Burkina Faso",
    "BO": "Bolivia", "EC": "Ecuador", "AR": "Argentina",
    "LA": "Laos", "MM": "Myanmar", "TZ": "Tanzania",
    "MZ": "Mozambique", "GN": "Guinea", "SN": "Senegal",
    "NE": "Niger", "NA": "Namibia", "US": "USA",
    "GB": "United Kingdom", "DE": "Germany", "PL": "Poland",
    "CO": "Colombia", "VE": "Venezuela",
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def classify_event_type(title: str) -> str:
    t = title.lower()
    for etype, kws in EVENT_TYPE_KEYWORDS.items():
        if any(kw in t for kw in kws):
            return etype
    return "General Mining Event"


def is_relevant_event(title: str) -> bool:
    """Return True if the title matches at least one event-type keyword."""
    t = title.lower()
    for kws in EVENT_TYPE_KEYWORDS.values():
        if any(kw in t for kw in kws):
            return True
    return False


def extract_region(title: str, sourcecountry: str) -> str:
    t = title.lower()
    for region, kws in REGION_KEYWORDS.items():
        if any(kw in t for kw in kws):
            return region
    return COUNTRY_CODE_MAP.get(str(sourcecountry).strip().upper(), "Global / Other")


def parse_gdelt_date(seendate: str) -> tuple[str, str]:
    """Return (YYYY-MM-DD string, YYYY year string) from GDELT seendate."""
    try:
        s = seendate.replace("T", "").replace("Z", "")[:14]
        dt = datetime.datetime.strptime(s[:8], "%Y%m%d")
        return dt.strftime("%Y-%m-%d"), str(dt.year)
    except Exception:
        return seendate[:10] if seendate else "", seendate[:4] if seendate else ""


def query_gdelt_artlist(query: str, start_dt: str, end_dt: str,
                        retries: int = 3) -> list[dict]:
    """
    Call GDELT DOC 2.0 ArtList API for a specific date range.
    start_dt / end_dt format: YYYYMMDDHHMMSS
    Returns list of article dicts (may be empty on error).
    """
    params = {
        "query":         query,
        "mode":          "ArtList",
        "maxrecords":    250,
        "startdatetime": start_dt,
        "enddatetime":   end_dt,
        "format":        "json",
        "sort":          "DateDesc",
    }
    for attempt in range(retries):
        try:
            r = requests.get(GDELT_URL, params=params, timeout=60)
            if r.status_code == 429:
                print(" [rate-limited — waiting 10s]", end="", flush=True)
                time.sleep(10)
                continue
            r.raise_for_status()
            data = r.json()
            return data.get("articles") or []
        except Exception as exc:
            if attempt < retries - 1:
                time.sleep(3)
            else:
                print(f" [error: {exc}]", end="", flush=True)
    return []


# ─── Time windows: one per calendar year for the past 10 years ────────────────

def yearly_windows(years_back: int = 10) -> list[tuple[str, str, str]]:
    """Return list of (start_dt, end_dt, label) for each year."""
    current_year = datetime.datetime.now().year
    windows = []
    for y in range(current_year - years_back, current_year + 1):
        start = f"{y}0101000000"
        end   = f"{y}1231235959"
        windows.append((start, end, str(y)))
    return windows


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 70)
    print("  GDELT Mining Event Scanner — 10-Year Historical Export")
    print("=" * 70)

    windows = yearly_windows(years_back=10)
    total_queries = len(COMMODITIES) * len(windows)
    print(f"\n  Commodities : {len(COMMODITIES)}")
    print(f"  Years       : {windows[0][2]} – {windows[-1][2]}")
    print(f"  Total queries: {total_queries}")
    print(f"  Max articles : {total_queries * 250:,}  (before deduplication)")
    print("\nStarting scan…\n")

    all_rows: list[dict] = []
    query_count = 0

    for commodity, base_query in COMMODITIES.items():
        # Keep query short: commodity phrase + language filter only.
        # Event-type relevance is filtered in Python after fetching.
        full_query = f"{base_query} sourcelang:english"

        for start_dt, end_dt, year_label in windows:
            query_count += 1
            pct = int(query_count / total_queries * 100)
            print(f"  [{pct:3d}%] {commodity:12s}  {year_label} … ", end="", flush=True)

            articles = query_gdelt_artlist(full_query, start_dt, end_dt)

            n = 0
            for art in articles:
                title        = (art.get("title") or "").strip()
                seendate     = (art.get("seendate") or "")
                domain       = (art.get("domain") or "")
                sourcecountry= (art.get("sourcecountry") or "")
                url          = (art.get("url") or "")
                tone         = art.get("tone")          # negative = bad news

                if not title:
                    continue

                # Post-filter: skip generic/off-topic articles
                if not is_relevant_event(title):
                    continue

                date_str, year_str = parse_gdelt_date(seendate)
                event_type = classify_event_type(title)
                region     = extract_region(title, sourcecountry)

                try:
                    tone_val = round(float(tone), 2) if tone is not None else None
                except Exception:
                    tone_val = None

                all_rows.append({
                    "Date":          date_str,
                    "Year":          year_str,
                    "Commodity":     commodity,
                    "Event Type":    event_type,
                    "Region":        region,
                    "Headline":      title,
                    "Tone Score":    tone_val,
                    "Domain":        domain,
                    "Country Code":  sourcecountry,
                    "URL":           url,
                })
                n += 1

            print(f"{n} articles")
            time.sleep(1.2)   # ~50 req/min — well within GDELT limits

    # ── Deduplication ─────────────────────────────────────────────────────────
    print(f"\nRaw articles collected : {len(all_rows):,}")
    df = pd.DataFrame(all_rows)

    if df.empty:
        print("No results — check network / GDELT availability.")
        return

    before = len(df)
    df = df.drop_duplicates(subset=["URL"])
    df = df.drop_duplicates(subset=["Headline", "Date", "Commodity"])
    df = df.sort_values(["Date", "Commodity"], ascending=[False, True]).reset_index(drop=True)
    print(f"After deduplication    : {len(df):,}  (removed {before - len(df):,} duplicates)")

    # ── Build summary tables ───────────────────────────────────────────────────
    summary_year = (
        df.groupby(["Year", "Commodity", "Event Type"])
        .size().reset_index(name="Count")
        .sort_values(["Year", "Count"], ascending=[False, False])
    )

    summary_type = (
        df.groupby(["Event Type", "Commodity"])
        .size().reset_index(name="Count")
        .sort_values("Count", ascending=False)
    )

    summary_region = (
        df.groupby(["Region", "Commodity", "Event Type"])
        .size().reset_index(name="Count")
        .sort_values("Count", ascending=False)
    )

    summary_commodity = (
        df.groupby(["Commodity", "Event Type"])
        .size().reset_index(name="Count")
        .sort_values(["Commodity", "Count"], ascending=[True, False])
    )

    # ── Excel export ──────────────────────────────────────────────────────────
    today = datetime.datetime.now().strftime("%Y%m%d")
    filename = f"mining_events_10y_{today}.xlsx"

    print(f"\nWriting Excel → {filename} …")

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        # Sheet 1: all events
        df.to_excel(writer, sheet_name="All Events", index=False)

        # Sheet 2: summary by year + commodity + event type
        summary_year.to_excel(writer, sheet_name="By Year", index=False)

        # Sheet 3: summary by event type
        summary_type.to_excel(writer, sheet_name="By Event Type", index=False)

        # Sheet 4: summary by region
        summary_region.to_excel(writer, sheet_name="By Region", index=False)

        # Sheet 5: summary by commodity
        summary_commodity.to_excel(writer, sheet_name="By Commodity", index=False)

        # Sheets 6+: one per event type (most impactful first)
        top_types = (
            df.groupby("Event Type").size().sort_values(ascending=False).head(8).index.tolist()
        )
        for etype in top_types:
            sheet = etype[:31]  # Excel tab name limit
            sub = df[df["Event Type"] == etype].copy()
            sub.to_excel(writer, sheet_name=sheet, index=False)

        # Auto-width columns on the "All Events" sheet
        try:
            from openpyxl.utils import get_column_letter
            ws = writer.sheets["All Events"]
            col_widths = {
                "A": 12,   # Date
                "B": 6,    # Year
                "C": 12,   # Commodity
                "D": 28,   # Event Type
                "E": 22,   # Region
                "F": 80,   # Headline
                "G": 10,   # Tone Score
                "H": 25,   # Domain
                "I": 6,    # Country Code
                "J": 60,   # URL
            }
            for col_letter, width in col_widths.items():
                ws.column_dimensions[col_letter].width = width
        except Exception:
            pass

    print(f"\n✓ Done!  {len(df):,} events saved to: {filename}")
    print()
    print("Top event types:")
    for _, row in summary_type.head(10).iterrows():
        print(f"  {row['Event Type']:35s} {row['Commodity']:12s}  {row['Count']:4d}")


if __name__ == "__main__":
    main()
