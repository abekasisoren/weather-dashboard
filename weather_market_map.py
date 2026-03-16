from copy import deepcopy


def _candidate(symbol, direction, role, tier, directness, notes=""):
    return {
        "symbol": symbol,
        "direction": direction,   # "long" or "short"
        "role": role,             # sector pathway
        "tier": tier,             # 1 = best/direct, 2 = secondary, 3 = context
        "directness": directness, # 1.0 = strongest direct mapping
        "notes": notes,
    }


BASE_WEATHER_MARKET_MAP = {
    "heatwave": {
        "commodities": ["corn", "soybeans", "wheat", "natural_gas"],
        "vehicles_preferred": ["CORN", "SOYB", "WEAT", "UNG"],
        "sectors": [
            "fertilizer",
            "ag_equipment",
            "grain_trading",
            "utilities",
            "power_demand",
            "natural_gas",
        ],
        "long_candidates": [
            _candidate("ADM", "long", "grain_trading", 1, 0.95, "Direct grain merchandising exposure"),
            _candidate("BG", "long", "grain_trading", 1, 0.95, "Direct grain handling / origination exposure"),
            _candidate("CF", "long", "fertilizer", 2, 0.75, "Crop stress can support fertilizer demand / pricing"),
            _candidate("MOS", "long", "fertilizer", 2, 0.75, "Crop stress can support fertilizer theme"),
            _candidate("CTVA", "long", "crop_inputs", 2, 0.70, "Agricultural input and seed exposure"),
            _candidate("DE", "long", "ag_equipment", 3, 0.45, "More indirect capex / farm equipment linkage"),
            _candidate("UNP", "long", "rail", 3, 0.35, "Rail/logistics linkage is real but less direct"),
            _candidate("EQT", "long", "natural_gas", 2, 0.65, "Heat can lift power demand / gas burn"),
            _candidate("LNG", "long", "lng_export", 2, 0.60, "Heat/power demand linkage"),
            _candidate("NEE", "long", "power_demand", 3, 0.35, "Utility demand angle but weaker trade"),
        ],
        "short_candidates": [],
    },

    "extreme_heat": {
        "commodities": ["corn", "soybeans", "wheat", "natural_gas"],
        "vehicles_preferred": ["CORN", "SOYB", "WEAT", "UNG"],
        "sectors": [
            "fertilizer",
            "grain_trading",
            "power_demand",
            "backup_power",
            "natural_gas",
        ],
        "long_candidates": [
            _candidate("ADM", "long", "grain_trading", 1, 0.95, "Direct crop stress/grain pricing beneficiary"),
            _candidate("BG", "long", "grain_trading", 1, 0.95, "Direct crop stress/grain pricing beneficiary"),
            _candidate("CF", "long", "fertilizer", 2, 0.75, "Ag input support"),
            _candidate("MOS", "long", "fertilizer", 2, 0.75, "Ag input support"),
            _candidate("GNRC", "long", "backup_power", 1, 0.85, "Extreme heat raises grid stress / backup demand"),
            _candidate("EQT", "long", "natural_gas", 2, 0.70, "Heat-driven gas burn"),
            _candidate("LNG", "long", "lng_export", 2, 0.60, "Gas demand sensitivity"),
        ],
        "short_candidates": [],
    },

    "drought": {
        "commodities": ["corn", "soybeans", "wheat", "coffee", "sugar"],
        "vehicles_preferred": ["CORN", "SOYB", "WEAT", "JO", "CANE"],
        "sectors": [
            "fertilizer",
            "agriculture",
            "grain_trading",
            "soft_commodities",
            "beverages",
        ],
        "long_candidates": [
            _candidate("ADM", "long", "grain_trading", 1, 0.95, "Direct grains/softs supply stress linkage"),
            _candidate("BG", "long", "grain_trading", 1, 0.95, "Direct grains/softs supply stress linkage"),
            _candidate("CF", "long", "fertilizer", 2, 0.75, "Crop stress can support ag input theme"),
            _candidate("MOS", "long", "fertilizer", 2, 0.75, "Crop stress can support ag input theme"),
            _candidate("CTVA", "long", "crop_inputs", 2, 0.70, "Crop input exposure"),
            _candidate("SBUX", "long", "coffee_price_pass_through", 3, 0.25, "Very indirect; usually not top trading expression"),
            _candidate("CZZ", "long", "sugar_exposure", 2, 0.60, "Sugar linkage more direct than consumer names"),
        ],
        "short_candidates": [],
    },

    "flood": {
        "commodities": [],
        "vehicles_preferred": ["XHB", "ITB", "XLRE"],
        "sectors": [
            "infrastructure_repair",
            "construction_materials",
            "water_engineering",
            "home_repair",
            "insurance",
        ],
        "long_candidates": [
            _candidate("HD", "long", "home_repair", 1, 0.90, "Clear rebuild / repair beneficiary"),
            _candidate("LOW", "long", "home_repair", 1, 0.90, "Clear rebuild / repair beneficiary"),
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.75, "Heavy equipment for repair cycle"),
            _candidate("VMC", "long", "construction_materials", 2, 0.75, "Aggregates/materials exposure"),
            _candidate("MLM", "long", "construction_materials", 2, 0.75, "Aggregates/materials exposure"),
            _candidate("XYL", "long", "water_engineering", 2, 0.70, "Water infrastructure angle"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.95, "Claims exposure"),
            _candidate("TRV", "short", "insurance", 1, 0.95, "Claims exposure"),
            _candidate("CB", "short", "insurance", 1, 0.90, "Claims exposure"),
        ],
    },

    "flood_risk": {
        "commodities": [],
        "vehicles_preferred": ["XHB", "ITB"],
        "sectors": [
            "infrastructure_repair",
            "construction_materials",
            "water_engineering",
            "home_repair",
            "insurance",
        ],
        "long_candidates": [
            _candidate("HD", "long", "home_repair", 1, 0.90, "Repair / rebuild demand"),
            _candidate("LOW", "long", "home_repair", 1, 0.90, "Repair / rebuild demand"),
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.75, "Repair cycle equipment"),
            _candidate("VMC", "long", "construction_materials", 2, 0.75, "Materials exposure"),
            _candidate("MLM", "long", "construction_materials", 2, 0.75, "Materials exposure"),
            _candidate("XYL", "long", "water_engineering", 2, 0.70, "Flood / water infrastructure"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.95, "Claims risk"),
            _candidate("TRV", "short", "insurance", 1, 0.95, "Claims risk"),
            _candidate("CB", "short", "insurance", 1, 0.90, "Claims risk"),
        ],
    },

    "heavy_rain": {
        "commodities": [],
        "vehicles_preferred": [],
        "sectors": [
            "construction_materials",
            "insurance",
            "water_engineering",
        ],
        "long_candidates": [
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.65, "Potential repair demand"),
            _candidate("VMC", "long", "construction_materials", 2, 0.70, "Repair/materials angle"),
            _candidate("MLM", "long", "construction_materials", 2, 0.70, "Repair/materials angle"),
            _candidate("XYL", "long", "water_engineering", 2, 0.65, "Water management angle"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.85, "Weather claims exposure"),
            _candidate("TRV", "short", "insurance", 1, 0.85, "Weather claims exposure"),
            _candidate("CB", "short", "insurance", 1, 0.80, "Weather claims exposure"),
        ],
    },

    "hurricane": {
        "commodities": ["oil", "natural_gas"],
        "vehicles_preferred": ["USO", "UNG", "XLE"],
        "sectors": [
            "construction",
            "generators",
            "shipping",
            "energy",
            "insurance",
            "infrastructure_repair",
        ],
        "long_candidates": [
            _candidate("GNRC", "long", "generators", 1, 0.95, "One of the cleanest hurricane trades"),
            _candidate("HD", "long", "home_repair", 1, 0.90, "Repair / prep demand"),
            _candidate("LOW", "long", "home_repair", 1, 0.90, "Repair / prep demand"),
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.75, "Post-storm reconstruction"),
            _candidate("VMC", "long", "construction_materials", 2, 0.75, "Rebuild materials"),
            _candidate("XOM", "long", "energy", 2, 0.60, "Can benefit if storm disrupts supply"),
            _candidate("CVX", "long", "energy", 2, 0.60, "Can benefit if storm disrupts supply"),
            _candidate("LNG", "long", "lng_export", 3, 0.35, "Too indirect for top idea unless gas-specific"),
            _candidate("ZIM", "long", "shipping", 3, 0.20, "Very noisy; usually not a top hurricane long"),
            _candidate("MATX", "long", "shipping", 3, 0.20, "Very noisy; usually not a top hurricane long"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.95, "High claims sensitivity"),
            _candidate("TRV", "short", "insurance", 1, 0.95, "High claims sensitivity"),
            _candidate("CB", "short", "insurance", 1, 0.90, "Claims sensitivity"),
        ],
    },

    "hurricane_risk": {
        "commodities": ["oil", "natural_gas"],
        "vehicles_preferred": ["USO", "UNG", "XLE"],
        "sectors": [
            "construction",
            "generators",
            "shipping",
            "energy",
            "insurance",
        ],
        "long_candidates": [
            _candidate("GNRC", "long", "generators", 1, 0.95, "Clean storm preparation trade"),
            _candidate("HD", "long", "home_repair", 1, 0.88, "Preparation / repair demand"),
            _candidate("LOW", "long", "home_repair", 1, 0.88, "Preparation / repair demand"),
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.70, "Repair cycle"),
            _candidate("VMC", "long", "construction_materials", 2, 0.70, "Repair cycle"),
            _candidate("XOM", "long", "energy", 2, 0.60, "Storm supply disruption angle"),
            _candidate("CVX", "long", "energy", 2, 0.60, "Storm supply disruption angle"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.95, "Claims sensitivity"),
            _candidate("TRV", "short", "insurance", 1, 0.95, "Claims sensitivity"),
            _candidate("CB", "short", "insurance", 1, 0.90, "Claims sensitivity"),
        ],
    },

    "wildfire": {
        "commodities": [],
        "vehicles_preferred": [],
        "sectors": [
            "power_infrastructure",
            "construction",
            "backup_power",
            "utilities_liability",
            "water_engineering",
        ],
        "long_candidates": [
            _candidate("GNRC", "long", "backup_power", 1, 0.95, "Clean wildfire/grid instability trade"),
            _candidate("HD", "long", "home_repair", 2, 0.65, "Repair / replacement demand"),
            _candidate("LOW", "long", "home_repair", 2, 0.65, "Repair / replacement demand"),
            _candidate("CAT", "long", "construction", 2, 0.65, "Rebuild / land clearing"),
            _candidate("VMC", "long", "construction_materials", 2, 0.60, "Rebuild materials"),
            _candidate("XYL", "long", "water_engineering", 3, 0.35, "Water/fire management angle"),
        ],
        "short_candidates": [
            _candidate("PCG", "short", "utilities_liability", 1, 0.98, "Direct historical wildfire liability profile"),
        ],
    },

    "wildfire_risk": {
        "commodities": [],
        "vehicles_preferred": [],
        "sectors": [
            "backup_power",
            "construction",
            "utilities_liability",
        ],
        "long_candidates": [
            _candidate("GNRC", "long", "backup_power", 1, 0.95, "Grid stress / backup power"),
            _candidate("CAT", "long", "construction", 2, 0.65, "Repair / rebuild"),
            _candidate("VMC", "long", "construction_materials", 2, 0.60, "Repair / rebuild"),
            _candidate("HD", "long", "home_repair", 2, 0.60, "Repair / rebuild"),
            _candidate("LOW", "long", "home_repair", 2, 0.60, "Repair / rebuild"),
        ],
        "short_candidates": [
            _candidate("PCG", "short", "utilities_liability", 1, 0.98, "Direct liability exposure"),
        ],
    },

    "tornado": {
        "commodities": [],
        "vehicles_preferred": [],
        "sectors": [
            "home_repair",
            "construction",
            "generators",
            "insurance",
        ],
        "long_candidates": [
            _candidate("GNRC", "long", "generators", 1, 0.90, "Storm outage trade"),
            _candidate("HD", "long", "home_repair", 1, 0.88, "Repair demand"),
            _candidate("LOW", "long", "home_repair", 1, 0.88, "Repair demand"),
            _candidate("CAT", "long", "construction", 2, 0.65, "Rebuild exposure"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.90, "Claims risk"),
            _candidate("TRV", "short", "insurance", 1, 0.90, "Claims risk"),
        ],
    },

    "storm_wind": {
        "commodities": ["natural_gas", "coal"],
        "vehicles_preferred": ["UNG", "KOL"],
        "sectors": [
            "power_disruption",
            "backup_power",
            "shipping_disruption",
            "insurance",
            "coal",
        ],
        "long_candidates": [
            _candidate("GNRC", "long", "backup_power", 1, 0.92, "Storm outage / backup power"),
            _candidate("BTU", "long", "coal", 2, 0.60, "Backup generation / fuel switching angle"),
            _candidate("AMR", "long", "coal", 2, 0.60, "Backup generation / fuel switching angle"),
            _candidate("ARCH", "long", "coal", 2, 0.60, "Backup generation / fuel switching angle"),
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.65, "Repair / cleanup cycle"),
            _candidate("MATX", "long", "shipping", 3, 0.20, "Too indirect/noisy for top trade"),
            _candidate("ZIM", "long", "shipping", 3, 0.20, "Too indirect/noisy for top trade"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.90, "Claims risk"),
            _candidate("TRV", "short", "insurance", 1, 0.90, "Claims risk"),
            _candidate("CB", "short", "insurance", 1, 0.85, "Claims risk"),
        ],
    },

    "cold_wave": {
        "commodities": ["natural_gas"],
        "vehicles_preferred": ["UNG"],
        "sectors": [
            "utilities",
            "lng_exporters",
            "gas_producers",
            "airlines",
        ],
        "long_candidates": [
            _candidate("EQT", "long", "gas_producers", 1, 0.95, "Direct gas sensitivity"),
            _candidate("RRC", "long", "gas_producers", 1, 0.95, "Direct gas sensitivity"),
            _candidate("CTRA", "long", "gas_producers", 1, 0.90, "Direct gas sensitivity"),
            _candidate("LNG", "long", "lng_exporters", 2, 0.70, "Gas / LNG sensitivity"),
            _candidate("NGG", "long", "utilities", 3, 0.30, "Less direct equity expression"),
        ],
        "short_candidates": [
            _candidate("DAL", "short", "airlines", 1, 0.85, "Weather disruption / cost sensitivity"),
            _candidate("UAL", "short", "airlines", 1, 0.85, "Weather disruption / cost sensitivity"),
            _candidate("AAL", "short", "airlines", 1, 0.85, "Weather disruption / cost sensitivity"),
        ],
    },

    "frost": {
        "commodities": ["wheat", "natural_gas"],
        "vehicles_preferred": ["WEAT", "UNG"],
        "sectors": [
            "gas_producers",
            "lng_exporters",
            "agriculture",
        ],
        "long_candidates": [
            _candidate("EQT", "long", "gas_producers", 1, 0.85, "Gas demand support"),
            _candidate("RRC", "long", "gas_producers", 1, 0.85, "Gas demand support"),
            _candidate("CTRA", "long", "gas_producers", 1, 0.82, "Gas demand support"),
            _candidate("LNG", "long", "lng_exporters", 2, 0.65, "Gas demand support"),
            _candidate("ADM", "long", "grain_trading", 2, 0.55, "Crop damage support"),
            _candidate("BG", "long", "grain_trading", 2, 0.55, "Crop damage support"),
        ],
        "short_candidates": [],
    },
}


def _sorted_symbols(candidates, direction, max_tier=3):
    filtered = [
        c for c in candidates
        if c["direction"] == direction and c["tier"] <= max_tier
    ]
    filtered = sorted(
        filtered,
        key=lambda x: (x["tier"], -x["directness"], x["symbol"])
    )
    return [c["symbol"] for c in filtered]


def _attach_legacy_fields(event_map):
    event_map = deepcopy(event_map)

    for event_name, event_data in event_map.items():
        long_candidates = event_data.get("long_candidates", [])
        short_candidates = event_data.get("short_candidates", [])

        event_data["equities_long"] = _sorted_symbols(long_candidates, "long", max_tier=3)
        event_data["equities_short"] = _sorted_symbols(short_candidates, "short", max_tier=3)

        event_data["equities_long_tier1"] = _sorted_symbols(long_candidates, "long", max_tier=1)
        event_data["equities_short_tier1"] = _sorted_symbols(short_candidates, "short", max_tier=1)

        event_data["equities_long_tier2"] = _sorted_symbols(long_candidates, "long", max_tier=2)
        event_data["equities_short_tier2"] = _sorted_symbols(short_candidates, "short", max_tier=2)

    return event_map


WEATHER_MARKET_MAP = _attach_legacy_fields(BASE_WEATHER_MARKET_MAP)


def get_event_market_map(event_name: str) -> dict:
    return deepcopy(WEATHER_MARKET_MAP.get(str(event_name).strip().lower(), {}))


def get_event_candidates(event_name: str, direction: str = None, max_tier: int = 3) -> list[dict]:
    event_data = get_event_market_map(event_name)
    candidates = []

    if direction in (None, "long"):
        candidates.extend(event_data.get("long_candidates", []))
    if direction in (None, "short"):
        candidates.extend(event_data.get("short_candidates", []))

    candidates = [c for c in candidates if c.get("tier", 99) <= max_tier]

    return sorted(
        candidates,
        key=lambda x: (
            x.get("tier", 99),
            -float(x.get("directness", 0)),
            x.get("symbol", "")
        )
    )


def get_top_equities(event_name: str, direction: str, max_names: int = 3, max_tier: int = 2) -> list[str]:
    candidates = get_event_candidates(event_name, direction=direction, max_tier=max_tier)
    return [c["symbol"] for c in candidates[:max_names]]


def get_preferred_vehicle(event_name: str) -> str:
    event_data = get_event_market_map(event_name)
    vehicles = event_data.get("vehicles_preferred", [])
    return vehicles[0] if vehicles else ""


def get_best_trade_expressions(event_name: str) -> dict:
    event_data = get_event_market_map(event_name)
    return {
        "preferred_vehicle": get_preferred_vehicle(event_name),
        "best_longs": get_top_equities(event_name, "long", max_names=3, max_tier=2),
        "best_shorts": get_top_equities(event_name, "short", max_names=3, max_tier=2),
        "commodities": event_data.get("commodities", []),
        "sectors": event_data.get("sectors", []),
    }
