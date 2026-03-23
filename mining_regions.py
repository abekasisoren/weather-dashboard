"""
mining_regions.py — Global mining regions registry for the Mining Radar.

Maps each mining region to:
  - Commodities produced
  - Keywords for news detection (region names, mine names, operators)
  - Affected stocks (Long / Short by commodity)
  - Global supply percentage (drives severity scoring)
  - Typical disruption event types

This file is pure data — no DB, no network calls.
"""

from __future__ import annotations

# ─── Mining Region Registry ────────────────────────────────────────────────────
MINING_REGIONS: list[dict] = [

    # ── COPPER ─────────────────────────────────────────────────────────────────
    {
        "region": "Atacama Desert",
        "country": "Chile",
        "commodities": ["copper", "lithium"],
        "keywords": [
            "Atacama", "Codelco", "Escondida", "Antofagasta mine",
            "SQM lithium", "lithium brine Chile", "Spence mine",
            "copper mine Chile", "Chuquicamata",
        ],
        "global_supply_pct": {"copper": 27, "lithium": 25},
        "stocks": {
            "copper":  {"Long": ["FCX", "SCCO", "COPX", "CPER"], "Short": ["SCCO", "FCX"]},
            "lithium": {"Long": ["ALB", "SQM", "LTHM", "LAC"],  "Short": []},
        },
        "typical_events": ["strike", "drought", "government_seizure", "accident"],
    },
    {
        "region": "Peruvian Andes",
        "country": "Peru",
        "commodities": ["copper", "zinc", "silver", "gold"],
        "keywords": [
            "Peru mine", "Cerro Verde", "Las Bambas", "Antamina",
            "Toquepala", "copper Peru", "Peruvian mining", "MMG Peru",
            "Glencore Peru", "road blockade Peru",
        ],
        "global_supply_pct": {"copper": 10, "zinc": 5, "silver": 15},
        "stocks": {
            "copper": {"Long": ["FCX", "SCCO", "COPX"], "Short": []},
            "zinc":   {"Long": ["TECK", "HBM"],         "Short": []},
            "silver": {"Long": ["SLV", "AG", "PAAS"],   "Short": []},
        },
        "typical_events": ["strike", "protest", "road_blockade", "government_seizure"],
    },
    {
        "region": "DRC Copperbelt",
        "country": "Democratic Republic of Congo",
        "commodities": ["copper", "cobalt"],
        "keywords": [
            "DRC mining", "Congo copper", "Katanga mine", "Kolwezi",
            "Tenke Fungurume", "Kamoa-Kakula", "Glencore DRC",
            "Ivanhoe Mines", "cobalt Congo",
        ],
        "global_supply_pct": {"copper": 10, "cobalt": 70},
        "stocks": {
            "copper": {"Long": ["FCX", "COPX"],            "Short": []},
            "cobalt": {"Long": ["FCX", "VALE", "GLNCY"],   "Short": []},
        },
        "typical_events": ["government_seizure", "geopolitical", "accident", "strike"],
    },
    {
        "region": "Zambian Copperbelt",
        "country": "Zambia",
        "commodities": ["copper", "cobalt"],
        "keywords": [
            "Zambia copper", "Zambian Copperbelt", "First Quantum Zambia",
            "Konkola mine", "Mopani mine", "Nchanga", "royalty Zambia",
            "Kitwe", "Ndola",
        ],
        "global_supply_pct": {"copper": 5},
        "stocks": {
            "copper": {"Long": ["COPX", "FCX", "SCCO"], "Short": []},
        },
        "typical_events": ["government_seizure", "royalty_change", "strike", "flood_mine"],
    },

    # ── IRON ORE ────────────────────────────────────────────────────────────────
    {
        "region": "Pilbara",
        "country": "Australia",
        "commodities": ["iron ore"],
        "keywords": [
            "Pilbara", "BHP iron ore", "Rio Tinto iron ore",
            "Fortescue Metals", "Port Hedland", "Hamersley",
            "iron ore Australia", "Newman mine",
        ],
        "global_supply_pct": {"iron ore": 38},
        "stocks": {
            "iron ore": {"Long": ["BHP", "RIO", "VALE", "CLF"], "Short": ["BHP", "RIO"]},
        },
        "typical_events": ["cyclone", "flood_mine", "strike", "accident"],
    },
    {
        "region": "Carajás",
        "country": "Brazil",
        "commodities": ["iron ore", "copper", "nickel"],
        "keywords": [
            "Carajás", "Vale iron ore", "Para mine Brazil",
            "Serra dos Carajás", "Brumadinho", "Mariana mine",
            "tailings dam Brazil", "Brazilian iron ore",
        ],
        "global_supply_pct": {"iron ore": 20},
        "stocks": {
            "iron ore": {"Long": ["VALE", "CLF"], "Short": []},
        },
        "typical_events": ["tailings_dam", "flood_mine", "strike", "accident"],
    },

    # ── GOLD ────────────────────────────────────────────────────────────────────
    {
        "region": "Nevada Gold Triangle",
        "country": "USA",
        "commodities": ["gold", "silver"],
        "keywords": [
            "Nevada gold mine", "Carlin Trend", "Newmont Nevada",
            "Barrick Goldstrike", "Cortez mine", "Elko Nevada mining",
            "Battle Mountain gold",
        ],
        "global_supply_pct": {"gold": 6},
        "stocks": {
            "gold":   {"Long": ["NEM", "GOLD", "GDX", "GLD"], "Short": []},
            "silver": {"Long": ["SLV", "AG"],                  "Short": []},
        },
        "typical_events": ["strike", "accident", "environmental_shutdown", "drought"],
    },
    {
        "region": "Witwatersrand",
        "country": "South Africa",
        "commodities": ["gold", "platinum", "palladium"],
        "keywords": [
            "Witwatersrand", "South Africa gold mine", "AngloGold Ashanti",
            "Gold Fields", "Sibanye Stillwater", "Harmony Gold",
            "loadshedding mine South Africa", "Rustenburg platinum",
            "South Africa platinum", "Impala Platinum",
        ],
        "global_supply_pct": {"gold": 8, "platinum": 70, "palladium": 35},
        "stocks": {
            "gold":      {"Long": ["GFI", "AU", "HMY", "GDX"],  "Short": []},
            "platinum":  {"Long": ["SBSW", "PPLT", "ANGPY"],     "Short": []},
            "palladium": {"Long": ["PALL", "SBSW"],               "Short": []},
        },
        "typical_events": ["strike", "power_disruption", "accident", "government_seizure"],
    },
    {
        "region": "Abitibi Greenstone Belt",
        "country": "Canada",
        "commodities": ["gold", "silver"],
        "keywords": [
            "Abitibi gold", "Quebec gold mine", "Ontario gold mine",
            "Agnico Eagle", "Timmins mine", "Val-d'Or mine",
        ],
        "global_supply_pct": {"gold": 5},
        "stocks": {
            "gold": {"Long": ["AEM", "KGC", "GDX"], "Short": []},
        },
        "typical_events": ["strike", "accident", "closure", "flood_mine"],
    },

    # ── LITHIUM ─────────────────────────────────────────────────────────────────
    {
        "region": "Lithium Triangle Argentina",
        "country": "Argentina",
        "commodities": ["lithium"],
        "keywords": [
            "Argentina lithium", "Jujuy lithium", "Salta lithium",
            "Livent Argentina", "Allkem lithium", "Cauchari lithium",
            "Olaroz lithium", "lithium brine Argentina",
        ],
        "global_supply_pct": {"lithium": 15},
        "stocks": {
            "lithium": {"Long": ["ALB", "LTHM", "LAC", "PLL"], "Short": []},
        },
        "typical_events": ["government_seizure", "export_control", "strike", "drought"],
    },
    {
        "region": "Greenbushes",
        "country": "Australia",
        "commodities": ["lithium"],
        "keywords": [
            "Greenbushes lithium", "Talison Lithium",
            "Pilbara Minerals", "Western Australia lithium",
            "spodumene mine", "lithium hydroxide Australia",
        ],
        "global_supply_pct": {"lithium": 22},
        "stocks": {
            "lithium": {"Long": ["ALB", "LTHM"], "Short": []},
        },
        "typical_events": ["accident", "strike", "export_ban", "flood_mine"],
    },

    # ── NICKEL ──────────────────────────────────────────────────────────────────
    {
        "region": "North Sulawesi / Halmahera",
        "country": "Indonesia",
        "commodities": ["nickel"],
        "keywords": [
            "Indonesia nickel", "Halmahera mine", "Sulawesi nickel",
            "Morowali nickel", "HPAL nickel Indonesia",
            "export ban nickel Indonesia", "PT Vale Indonesia",
            "nickel pig iron Indonesia",
        ],
        "global_supply_pct": {"nickel": 30},
        "stocks": {
            "nickel": {"Long": ["NICL", "VALE", "FCX"], "Short": []},
        },
        "typical_events": ["export_ban", "government_seizure", "accident", "environmental_shutdown"],
    },
    {
        "region": "Norilsk",
        "country": "Russia",
        "commodities": ["nickel", "palladium", "platinum", "copper"],
        "keywords": [
            "Norilsk Nickel", "Nornickel", "Russia nickel",
            "Siberia mine", "palladium Russia", "Russian mining sanctions",
        ],
        "global_supply_pct": {"nickel": 10, "palladium": 40, "platinum": 10},
        "stocks": {
            "nickel":    {"Long": ["NICL", "VALE"],  "Short": []},
            "palladium": {"Long": ["PALL", "SBSW"],  "Short": []},
        },
        "typical_events": ["geopolitical", "accident", "environmental_shutdown", "sanctions"],
    },
    {
        "region": "Sudbury Basin",
        "country": "Canada",
        "commodities": ["nickel", "copper", "cobalt"],
        "keywords": [
            "Sudbury nickel", "Vale Sudbury", "Glencore Sudbury",
            "Ontario nickel mine",
        ],
        "global_supply_pct": {"nickel": 4},
        "stocks": {
            "nickel": {"Long": ["VALE", "NICL"], "Short": []},
            "copper": {"Long": ["FCX", "COPX"],  "Short": []},
        },
        "typical_events": ["strike", "accident", "closure", "flood_mine"],
    },

    # ── URANIUM ─────────────────────────────────────────────────────────────────
    {
        "region": "Athabasca Basin",
        "country": "Canada",
        "commodities": ["uranium"],
        "keywords": [
            "Athabasca Basin uranium", "Cameco", "Cigar Lake",
            "McArthur River uranium", "Saskatchewan uranium",
            "Key Lake uranium",
        ],
        "global_supply_pct": {"uranium": 15},
        "stocks": {
            "uranium": {"Long": ["CCJ", "UEC", "URA", "NXE"], "Short": []},
        },
        "typical_events": ["flood_mine", "accident", "mine_closure", "strike"],
    },
    {
        "region": "Kazakhstan Steppes",
        "country": "Kazakhstan",
        "commodities": ["uranium"],
        "keywords": [
            "Kazakhstan uranium", "Kazatomprom",
            "in-situ uranium Kazakhstan", "uranium production Kazakhstan",
        ],
        "global_supply_pct": {"uranium": 45},
        "stocks": {
            "uranium": {"Long": ["CCJ", "URA", "UEC"], "Short": []},
        },
        "typical_events": ["government_seizure", "production_cut", "geopolitical", "drought"],
    },

    # ── RARE EARTHS ─────────────────────────────────────────────────────────────
    {
        "region": "Mountain Pass",
        "country": "USA",
        "commodities": ["rare earths"],
        "keywords": [
            "Mountain Pass rare earth", "MP Materials",
            "neodymium California", "rare earth USA mine",
        ],
        "global_supply_pct": {"rare earths": 12},
        "stocks": {
            "rare earths": {"Long": ["MP", "REMX"], "Short": []},
        },
        "typical_events": ["accident", "environmental_shutdown", "regulatory"],
    },
    {
        "region": "Inner Mongolia",
        "country": "China",
        "commodities": ["rare earths"],
        "keywords": [
            "Bayan Obo rare earth", "China rare earth export",
            "rare earth export quota China", "REE China ban",
            "Inner Mongolia mine",
        ],
        "global_supply_pct": {"rare earths": 58},
        "stocks": {
            "rare earths": {"Long": ["MP", "REMX"], "Short": []},
        },
        "typical_events": ["export_ban", "government_seizure", "environmental_shutdown"],
    },

    # ── PRECIOUS METALS ─────────────────────────────────────────────────────────
    {
        "region": "Mexican Silver Belt",
        "country": "Mexico",
        "commodities": ["silver", "zinc", "lead"],
        "keywords": [
            "Mexico silver mine", "Fresnillo", "First Majestic Silver",
            "Newmont Mexico", "Peñoles", "Sinaloa mine", "Zacatecas mine",
        ],
        "global_supply_pct": {"silver": 23},
        "stocks": {
            "silver": {"Long": ["AG", "PAAS", "SLV", "SILJ"], "Short": []},
        },
        "typical_events": ["cartel_security", "strike", "government_seizure", "accident"],
    },

    # ── COAL ────────────────────────────────────────────────────────────────────
    {
        "region": "Hunter Valley",
        "country": "Australia",
        "commodities": ["coal"],
        "keywords": [
            "Hunter Valley coal", "Newcastle coal port",
            "Glencore coal Australia", "BHP Mitsubishi coal",
            "coking coal Queensland", "thermal coal NSW",
        ],
        "global_supply_pct": {"coal": 8},
        "stocks": {
            "coal": {"Long": ["BTU", "ARCH", "HCC"], "Short": []},
        },
        "typical_events": ["cyclone", "flood_mine", "strike", "port_disruption"],
    },

    # ── POTASH / FERTILIZERS ────────────────────────────────────────────────────
    {
        "region": "Saskatchewan Potash Belt",
        "country": "Canada",
        "commodities": ["potash"],
        "keywords": [
            "Saskatchewan potash", "Nutrien Vanscoy", "Mosaic potash mine",
            "potash mine Canada", "K+S potash",
        ],
        "global_supply_pct": {"potash": 30},
        "stocks": {
            "potash": {"Long": ["NTR", "MOS", "IPI"], "Short": []},
        },
        "typical_events": ["flood_mine", "accident", "strike", "government_seizure"],
    },

    # ── BAUXITE / ALUMINIUM ─────────────────────────────────────────────────────
    {
        "region": "Boké Bauxite Belt",
        "country": "Guinea",
        "commodities": ["bauxite", "aluminium"],
        "keywords": [
            "Guinea bauxite", "Boké mine", "CBG bauxite",
            "Rio Tinto Sangarédi", "Rusal Guinea", "West Africa bauxite",
        ],
        "global_supply_pct": {"bauxite": 22},
        "stocks": {
            "bauxite":   {"Long": ["AA", "CENX", "RIO"], "Short": []},
            "aluminium": {"Long": ["AA", "CENX", "NHYDY"], "Short": []},
        },
        "typical_events": ["geopolitical", "strike", "government_seizure", "coup"],
    },
]


# ─── Event type definitions ────────────────────────────────────────────────────
EVENT_TYPES: dict[str, dict] = {
    "strike": {
        "keywords": [
            "strike", "walkout", "labor dispute", "union action",
            "work stoppage", "industrial action", "wage dispute",
            "worker protest", "picket",
        ],
        "base_severity": 5,
        "trade_bias": "Long",
        "duration_est": "days_to_weeks",
        "description": "Labor strike / work stoppage",
        "trend": "new",
    },
    "mine_closure": {
        "keywords": [
            "mine closure", "shutdown", "halt production", "suspend operations",
            "force majeure", "production halt", "mine shut", "close mine",
        ],
        "base_severity": 7,
        "trade_bias": "Long",
        "duration_est": "weeks_to_months",
        "description": "Mine closure or production suspension",
        "trend": "new",
    },
    "accident": {
        "keywords": [
            "mining accident", "collapse", "explosion at mine", "blast mine",
            "fatality mine", "worker killed mine", "cave-in", "rockfall",
            "mine rescue",
        ],
        "base_severity": 6,
        "trade_bias": "Long",
        "duration_est": "days_to_weeks",
        "description": "Mining accident / safety incident",
        "trend": "new",
    },
    "tailings_dam": {
        "keywords": [
            "tailings dam", "dam failure", "dam breach", "tailings collapse",
            "tailings spill", "mining waste spill",
        ],
        "base_severity": 9,
        "trade_bias": "Long",
        "duration_est": "months",
        "description": "Tailings dam failure",
        "trend": "new",
    },
    "government_seizure": {
        "keywords": [
            "nationalization", "licence revoked", "mining licence cancelled",
            "government takeover mine", "expropriation mine",
            "permit suspended mine", "seized mine",
        ],
        "base_severity": 8,
        "trade_bias": "Long",
        "duration_est": "months",
        "description": "Government seizure / nationalization",
        "trend": "new",
    },
    "export_ban": {
        "keywords": [
            "export ban", "export restriction mineral", "export quota mineral",
            "ban mineral exports", "embargo mineral", "trade restriction mineral",
        ],
        "base_severity": 7,
        "trade_bias": "Long",
        "duration_est": "months",
        "description": "Export ban / restriction",
        "trend": "new",
    },
    "geopolitical": {
        "keywords": [
            "coup mining", "unrest mine region", "civil conflict mine",
            "sanctions mine", "militia mine", "armed group mine",
            "instability mine",
        ],
        "base_severity": 7,
        "trade_bias": "Long",
        "duration_est": "weeks_to_months",
        "description": "Geopolitical disruption",
        "trend": "new",
    },
    "environmental_shutdown": {
        "keywords": [
            "environmental violation mine", "EPA order mine",
            "environmental shutdown mine", "pollution fine mine",
            "water contamination mine", "environmental halt mine",
        ],
        "base_severity": 5,
        "trade_bias": "Long",
        "duration_est": "weeks",
        "description": "Environmental / regulatory shutdown",
        "trend": "new",
    },
    "flood_mine": {
        "keywords": [
            "mine flooded", "flood mine", "inundation mine",
            "cyclone mine", "hurricane mine damage", "storm mine damage",
            "mine operations suspended rain",
        ],
        "base_severity": 5,
        "trade_bias": "Long",
        "duration_est": "days_to_weeks",
        "description": "Flood / extreme weather at mine site",
        "trend": "new",
    },
    "power_disruption": {
        "keywords": [
            "power cut mine", "load shedding mine", "electricity shortage mine",
            "grid failure mine", "blackout mine operations",
        ],
        "base_severity": 4,
        "trade_bias": "Long",
        "duration_est": "days",
        "description": "Power disruption at mine",
        "trend": "new",
    },
    "production_increase": {
        "keywords": [
            "production record mine", "new mine opened", "first ore production",
            "capacity expansion mine", "new discovery mine", "feasibility approved",
        ],
        "base_severity": 3,
        "trade_bias": "Short",
        "duration_est": "months",
        "description": "New supply / production expansion",
        "trend": "new",
    },
}


# ─── Commodity → best standalone ETF / proxy ──────────────────────────────────
COMMODITY_VEHICLES: dict[str, str] = {
    "copper":      "COPX",
    "lithium":     "LTHM",
    "iron ore":    "VALE",
    "gold":        "GDX",
    "silver":      "SLV",
    "platinum":    "PPLT",
    "palladium":   "PALL",
    "nickel":      "NICL",
    "uranium":     "URA",
    "rare earths": "REMX",
    "coal":        "BTU",
    "potash":      "NTR",
    "bauxite":     "AA",
    "aluminium":   "AA",
    "cobalt":      "FCX",
    "zinc":        "TECK",
}

SIGNAL_BUCKETS: dict[int, str] = {
    1: "WATCH",
    2: "MODERATE",
    3: "STRONG",
    4: "EXTREME",
}


def supply_to_signal_level(supply_pct: float) -> int:
    """Convert % of global supply affected to signal level 1-4."""
    if supply_pct >= 25:
        return 4  # EXTREME
    if supply_pct >= 10:
        return 3  # STRONG
    if supply_pct >= 3:
        return 2  # MODERATE
    return 1      # WATCH


def get_region_by_name(region: str) -> dict | None:
    """Return a region dict by exact name, or None."""
    for r in MINING_REGIONS:
        if r["region"] == region:
            return r
    return None
