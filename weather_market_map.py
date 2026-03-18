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
            "hvac_cooling",
        ],
        "long_candidates": [
            _candidate("ADM", "long", "grain_trading", 1, 0.95, "Direct grain merchandising exposure"),
            _candidate("BG", "long", "grain_trading", 1, 0.95, "Direct grain handling / origination exposure"),
            _candidate("CF", "long", "fertilizer", 2, 0.75, "Crop stress can support fertilizer demand / pricing"),
            _candidate("MOS", "long", "fertilizer", 2, 0.75, "Crop stress can support fertilizer theme"),
            _candidate("NTR", "long", "fertilizer", 2, 0.72, "Nutrien fertilizer and crop inputs"),
            _candidate("CTVA", "long", "crop_inputs", 2, 0.70, "Agricultural input and seed exposure"),
            _candidate("CARR", "long", "hvac_cooling", 1, 0.88, "HVAC demand surge during extreme heat"),
            _candidate("GNRC", "long", "backup_power", 1, 0.85, "Grid stress / backup power demand"),
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
            "hvac_cooling",
        ],
        "long_candidates": [
            _candidate("ADM", "long", "grain_trading", 1, 0.95, "Direct crop stress/grain pricing beneficiary"),
            _candidate("BG", "long", "grain_trading", 1, 0.95, "Direct crop stress/grain pricing beneficiary"),
            _candidate("CF", "long", "fertilizer", 2, 0.75, "Ag input support"),
            _candidate("MOS", "long", "fertilizer", 2, 0.75, "Ag input support"),
            _candidate("NTR", "long", "fertilizer", 2, 0.72, "Nutrien crop inputs exposure"),
            _candidate("GNRC", "long", "backup_power", 1, 0.90, "Extreme heat raises grid stress / backup demand"),
            _candidate("CARR", "long", "hvac_cooling", 1, 0.90, "HVAC demand at peak during extreme heat events"),
            _candidate("EQT", "long", "natural_gas", 2, 0.70, "Heat-driven gas burn"),
            _candidate("LNG", "long", "lng_export", 2, 0.60, "Gas demand sensitivity"),
            _candidate("AWK", "long", "water_demand", 2, 0.60, "Water utility demand during extreme heat"),
        ],
        "short_candidates": [],
    },

    "drought": {
        "commodities": ["corn", "soybeans", "wheat", "coffee", "sugar", "copper", "lithium"],
        "vehicles_preferred": ["CORN", "SOYB", "WEAT", "JO", "CANE", "COPX"],
        "sectors": [
            "fertilizer",
            "agriculture",
            "grain_trading",
            "soft_commodities",
            "beverages",
            "water_infrastructure",
            "copper_mining",
            "lithium_mining",
        ],
        "long_candidates": [
            _candidate("ADM", "long", "grain_trading", 1, 0.95, "Direct grains/softs supply stress linkage"),
            _candidate("BG", "long", "grain_trading", 1, 0.95, "Direct grains/softs supply stress linkage"),
            _candidate("FCX", "long", "copper_mining", 1, 0.88, "Drought disrupts water-intensive copper mining — supply shock"),
            _candidate("SCCO", "long", "copper_mining", 1, 0.88, "Southern Copper — Andes mining water dependency"),
            _candidate("ALB", "long", "lithium_mining", 1, 0.85, "Lithium brine operations require water — drought = supply stress"),
            _candidate("CF", "long", "fertilizer", 2, 0.75, "Crop stress can support ag input theme"),
            _candidate("MOS", "long", "fertilizer", 2, 0.75, "Crop stress can support ag input theme"),
            _candidate("NTR", "long", "fertilizer", 2, 0.72, "Nutrien fertilizer linkage"),
            _candidate("CTVA", "long", "crop_inputs", 2, 0.70, "Crop input exposure"),
            _candidate("AWK", "long", "water_infrastructure", 1, 0.88, "Water utility demand surges in drought conditions"),
            _candidate("XYL", "long", "water_engineering", 2, 0.72, "Water management / drought infrastructure"),
            _candidate("SQM", "long", "lithium_mining", 2, 0.72, "SQM — Andes lithium, water-intensive operations"),
            _candidate("BHP", "long", "diversified_mining", 2, 0.68, "Diversified miner — copper/iron drought exposure"),
            _candidate("RIO", "long", "diversified_mining", 2, 0.65, "Rio Tinto — copper/aluminum drought exposure"),
            _candidate("SBUX", "long", "coffee_price_pass_through", 3, 0.25, "Very indirect; usually not top trading expression"),
            _candidate("CZZ", "long", "sugar_exposure", 2, 0.60, "Sugar linkage more direct than consumer names"),
            _candidate("MDLZ", "long", "cocoa_cost_pass_through", 3, 0.30, "Cocoa/wheat cost pass-through angle"),
            _candidate("HSY", "long", "cocoa_consumer", 3, 0.28, "Cocoa input cost sensitivity"),
        ],
        "short_candidates": [
            _candidate("BEP", "short", "hydropower", 2, 0.72, "Brookfield Renewable — drought reduces hydro generation output"),
            _candidate("AES", "short", "hydropower", 2, 0.68, "AES Corp — significant hydro exposure, drought headwind"),
        ],
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
            "reinsurance",
        ],
        "long_candidates": [
            _candidate("HD", "long", "home_repair", 1, 0.90, "Clear rebuild / repair beneficiary"),
            _candidate("LOW", "long", "home_repair", 1, 0.90, "Clear rebuild / repair beneficiary"),
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.75, "Heavy equipment for repair cycle"),
            _candidate("VMC", "long", "construction_materials", 2, 0.75, "Aggregates/materials exposure"),
            _candidate("MLM", "long", "construction_materials", 2, 0.75, "Aggregates/materials exposure"),
            _candidate("XYL", "long", "water_engineering", 2, 0.70, "Water infrastructure angle"),
            _candidate("AWK", "long", "water_infrastructure", 2, 0.65, "Water system demand post-flood"),
            _candidate("LEN", "long", "homebuilder", 2, 0.65, "Rebuild / replacement housing demand"),
            _candidate("PHM", "long", "homebuilder", 2, 0.62, "Rebuild / replacement housing demand"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.95, "Claims exposure"),
            _candidate("TRV", "short", "insurance", 1, 0.95, "Claims exposure"),
            _candidate("CB", "short", "insurance", 1, 0.90, "Claims exposure"),
            _candidate("RNR", "short", "reinsurance", 1, 0.88, "Catastrophe reinsurance loss exposure"),
            _candidate("AXS", "short", "reinsurance", 2, 0.75, "Specialty lines flood exposure"),
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
            _candidate("LEN", "long", "homebuilder", 2, 0.65, "Post-flood rebuild demand"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.95, "Claims risk"),
            _candidate("TRV", "short", "insurance", 1, 0.95, "Claims risk"),
            _candidate("CB", "short", "insurance", 1, 0.90, "Claims risk"),
            _candidate("RNR", "short", "reinsurance", 1, 0.85, "Catastrophe reinsurance loss exposure"),
        ],
    },

    "heavy_rain": {
        "commodities": [],
        "vehicles_preferred": ["XHB", "ITB", "XLRE"],
        "sectors": [
            "construction_materials",
            "insurance",
            "water_engineering",
            "homebuilder",
            "hydropower",
        ],
        "long_candidates": [
            _candidate("BEP", "long", "hydropower", 1, 0.82, "Brookfield Renewable — heavy rainfall boosts hydro reservoir levels"),
            _candidate("AES", "long", "hydropower", 2, 0.75, "AES Corp — hydro generation benefits from heavy rainfall"),
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.65, "Potential repair demand"),
            _candidate("VMC", "long", "construction_materials", 2, 0.70, "Repair/materials angle"),
            _candidate("MLM", "long", "construction_materials", 2, 0.70, "Repair/materials angle"),
            _candidate("XYL", "long", "water_engineering", 2, 0.65, "Water management angle"),
            _candidate("HD", "long", "home_repair", 2, 0.65, "Repair demand"),
            _candidate("LOW", "long", "home_repair", 2, 0.65, "Repair demand"),
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
            "reinsurance",
            "infrastructure_repair",
            "cruise",
        ],
        "long_candidates": [
            _candidate("GNRC", "long", "generators", 1, 0.95, "One of the cleanest hurricane trades"),
            _candidate("HD", "long", "home_repair", 1, 0.90, "Repair / prep demand"),
            _candidate("LOW", "long", "home_repair", 1, 0.90, "Repair / prep demand"),
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.75, "Post-storm reconstruction"),
            _candidate("VMC", "long", "construction_materials", 2, 0.75, "Rebuild materials"),
            _candidate("XOM", "long", "energy", 2, 0.60, "Can benefit if storm disrupts supply"),
            _candidate("CVX", "long", "energy", 2, 0.60, "Can benefit if storm disrupts supply"),
            _candidate("SHEL", "long", "energy_intl", 2, 0.58, "Gulf / offshore exposure via ADR"),
            _candidate("LEN", "long", "homebuilder", 2, 0.65, "Post-storm rebuild/replacement housing"),
            _candidate("PHM", "long", "homebuilder", 2, 0.62, "Post-storm rebuild/replacement housing"),
            _candidate("LNG", "long", "lng_export", 3, 0.35, "Too indirect for top idea unless gas-specific"),
            _candidate("FRO", "long", "tankers", 3, 0.30, "Crude tanker disruption / rerouting angle"),
            _candidate("ZIM", "long", "shipping", 3, 0.20, "Very noisy; usually not a top hurricane long"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.95, "High claims sensitivity"),
            _candidate("TRV", "short", "insurance", 1, 0.95, "High claims sensitivity"),
            _candidate("CB", "short", "insurance", 1, 0.90, "Claims sensitivity"),
            _candidate("RNR", "short", "reinsurance", 1, 0.92, "Catastrophe reinsurance — hurricane is the top peril"),
            _candidate("AXS", "short", "reinsurance", 2, 0.78, "Specialty lines hurricane exposure"),
            _candidate("MKL", "short", "specialty_insurance", 2, 0.70, "Specialty insurance claims"),
            _candidate("RCL", "short", "cruise", 1, 0.92, "Royal Caribbean — direct itinerary cancellations, port closures"),
            _candidate("CCL", "short", "cruise", 1, 0.90, "Carnival Corp — Caribbean route hurricane losses"),
            _candidate("NCLH", "short", "cruise", 2, 0.82, "Norwegian Cruise — Caribbean itinerary exposure"),
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
            "reinsurance",
            "cruise",
        ],
        "long_candidates": [
            _candidate("GNRC", "long", "generators", 1, 0.95, "Clean storm preparation trade"),
            _candidate("HD", "long", "home_repair", 1, 0.88, "Preparation / repair demand"),
            _candidate("LOW", "long", "home_repair", 1, 0.88, "Preparation / repair demand"),
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.70, "Repair cycle"),
            _candidate("VMC", "long", "construction_materials", 2, 0.70, "Repair cycle"),
            _candidate("XOM", "long", "energy", 2, 0.60, "Storm supply disruption angle"),
            _candidate("CVX", "long", "energy", 2, 0.60, "Storm supply disruption angle"),
            _candidate("SHEL", "long", "energy_intl", 2, 0.55, "Gulf/offshore exposure"),
            _candidate("LEN", "long", "homebuilder", 2, 0.60, "Pre-storm prep / rebuild"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.95, "Claims sensitivity"),
            _candidate("TRV", "short", "insurance", 1, 0.95, "Claims sensitivity"),
            _candidate("CB", "short", "insurance", 1, 0.90, "Claims sensitivity"),
            _candidate("RNR", "short", "reinsurance", 1, 0.90, "Cat reinsurance loss exposure"),
            _candidate("AXS", "short", "reinsurance", 2, 0.75, "Specialty lines hurricane exposure"),
            _candidate("RCL", "short", "cruise", 1, 0.88, "Royal Caribbean — itinerary cancellations, port disruptions"),
            _candidate("CCL", "short", "cruise", 1, 0.85, "Carnival Corp — Caribbean route hurricane exposure"),
            _candidate("NCLH", "short", "cruise", 2, 0.80, "Norwegian Cruise — Caribbean itinerary disruption"),
        ],
    },

    "wildfire": {
        "commodities": [],
        "vehicles_preferred": ["GNRC", "XHB"],
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
            _candidate("LEN", "long", "homebuilder", 2, 0.60, "Replacement housing demand"),
            _candidate("XYL", "long", "water_engineering", 3, 0.35, "Water/fire management angle"),
        ],
        "short_candidates": [
            _candidate("PCG", "short", "utilities_liability", 1, 0.98, "Direct historical wildfire liability profile"),
            _candidate("ALL", "short", "insurance", 2, 0.72, "Wildfire claims exposure"),
            _candidate("CB", "short", "insurance", 2, 0.68, "Wildfire claims exposure"),
        ],
    },

    "wildfire_risk": {
        "commodities": [],
        "vehicles_preferred": ["GNRC", "XHB"],
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
            _candidate("CARR", "long", "hvac_cooling", 3, 0.40, "Post-wildfire HVAC/air quality system demand"),
        ],
        "short_candidates": [
            _candidate("PCG", "short", "utilities_liability", 1, 0.98, "Direct liability exposure"),
            _candidate("ALL", "short", "insurance", 2, 0.68, "Wildfire claims exposure"),
        ],
    },

    "tornado": {
        "commodities": [],
        "vehicles_preferred": ["XHB", "ITB"],
        "sectors": [
            "home_repair",
            "construction",
            "generators",
            "insurance",
            "reinsurance",
        ],
        "long_candidates": [
            _candidate("GNRC", "long", "generators", 1, 0.90, "Storm outage trade"),
            _candidate("HD", "long", "home_repair", 1, 0.88, "Repair demand"),
            _candidate("LOW", "long", "home_repair", 1, 0.88, "Repair demand"),
            _candidate("CAT", "long", "construction", 2, 0.65, "Rebuild exposure"),
            _candidate("VMC", "long", "construction_materials", 2, 0.62, "Rebuild materials"),
            _candidate("LEN", "long", "homebuilder", 2, 0.60, "Replacement housing demand"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.90, "Claims risk"),
            _candidate("TRV", "short", "insurance", 1, 0.90, "Claims risk"),
            _candidate("RNR", "short", "reinsurance", 2, 0.75, "Catastrophe reinsurance tornado exposure"),
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
            "offshore_energy",
        ],
        "long_candidates": [
            _candidate("GNRC", "long", "backup_power", 1, 0.92, "Storm outage / backup power"),
            _candidate("BTU", "long", "coal", 2, 0.60, "Backup generation / fuel switching angle"),
            _candidate("AMR", "long", "coal", 2, 0.60, "Backup generation / fuel switching angle"),
            _candidate("ARCH", "long", "coal", 2, 0.60, "Backup generation / fuel switching angle"),
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.65, "Repair / cleanup cycle"),
            _candidate("EQNR", "long", "offshore_energy", 2, 0.62, "North Sea offshore exposure"),
            _candidate("GOGL", "long", "dry_bulk_shipping", 3, 0.30, "Storm disruption to grain/coal shipments"),
            _candidate("MATX", "long", "shipping", 3, 0.20, "Too indirect/noisy for top trade"),
            _candidate("ZIM", "long", "shipping", 3, 0.20, "Too indirect/noisy for top trade"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.90, "Claims risk"),
            _candidate("TRV", "short", "insurance", 1, 0.90, "Claims risk"),
            _candidate("CB", "short", "insurance", 1, 0.85, "Claims risk"),
            _candidate("RNR", "short", "reinsurance", 2, 0.72, "Cat reinsurance storm exposure"),
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
            "hvac_heating",
            "ski_resorts",
        ],
        "long_candidates": [
            _candidate("EQT", "long", "gas_producers", 1, 0.95, "Direct gas sensitivity"),
            _candidate("RRC", "long", "gas_producers", 1, 0.95, "Direct gas sensitivity"),
            _candidate("CTRA", "long", "gas_producers", 1, 0.90, "Direct gas sensitivity"),
            _candidate("LNG", "long", "lng_exporters", 2, 0.70, "Gas / LNG sensitivity"),
            _candidate("SHEL", "long", "lng_intl", 2, 0.65, "LNG exporter with European exposure"),
            _candidate("BP", "long", "lng_intl", 2, 0.62, "LNG and gas exposure"),
            _candidate("EQNR", "long", "lng_intl", 2, 0.65, "North Sea gas / LNG exporter"),
            _candidate("CARR", "long", "hvac_heating", 2, 0.65, "Heating system demand during cold events"),
            _candidate("MTN", "long", "ski_resorts", 2, 0.72, "Vail Resorts — cold/snow conditions boost ski season bookings"),
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
            "ski_resorts",
        ],
        "long_candidates": [
            _candidate("EQT", "long", "gas_producers", 1, 0.85, "Gas demand support"),
            _candidate("RRC", "long", "gas_producers", 1, 0.85, "Gas demand support"),
            _candidate("CTRA", "long", "gas_producers", 1, 0.82, "Gas demand support"),
            _candidate("LNG", "long", "lng_exporters", 2, 0.65, "Gas demand support"),
            _candidate("EQNR", "long", "lng_intl", 2, 0.62, "North Sea LNG/gas exposure"),
            _candidate("ADM", "long", "grain_trading", 2, 0.55, "Crop damage support"),
            _candidate("BG", "long", "grain_trading", 2, 0.55, "Crop damage support"),
            _candidate("NTR", "long", "fertilizer", 2, 0.50, "Spring replant demand post-frost"),
            _candidate("MTN", "long", "ski_resorts", 2, 0.68, "Frost / cold snap supports ski season conditions"),
        ],
        "short_candidates": [],
    },

    # --- New anomaly type mappings ---
    "polar_vortex": {
        "commodities": ["natural_gas", "lng"],
        "vehicles_preferred": ["UNG"],
        "sectors": [
            "gas_producers",
            "lng_exporters",
            "utilities",
            "airlines",
            "coal",
        ],
        "long_candidates": [
            _candidate("EQT", "long", "gas_producers", 1, 0.98, "Extreme cold = maximum gas demand spike"),
            _candidate("RRC", "long", "gas_producers", 1, 0.98, "Extreme cold = maximum gas demand spike"),
            _candidate("CTRA", "long", "gas_producers", 1, 0.95, "Direct gas sensitivity"),
            _candidate("LNG", "long", "lng_exporters", 1, 0.88, "Global LNG demand surge"),
            _candidate("SHEL", "long", "lng_intl", 1, 0.85, "LNG exporter — extreme cold spike"),
            _candidate("EQNR", "long", "lng_intl", 1, 0.82, "North Sea gas / LNG exporter"),
            _candidate("BP", "long", "lng_intl", 2, 0.72, "LNG and gas exposure"),
            _candidate("BTU", "long", "coal", 2, 0.65, "Fuel switching / backup generation"),
            _candidate("AMR", "long", "coal", 2, 0.65, "Fuel switching / backup generation"),
            _candidate("GNRC", "long", "backup_power", 2, 0.70, "Grid stress during polar vortex"),
            _candidate("NGG", "long", "utilities", 2, 0.55, "UK/European utility gas exposure"),
        ],
        "short_candidates": [
            _candidate("DAL", "short", "airlines", 1, 0.90, "Severe disruption / cancellations / fuel cost spike"),
            _candidate("UAL", "short", "airlines", 1, 0.90, "Severe disruption / cancellations"),
            _candidate("AAL", "short", "airlines", 1, 0.88, "Severe disruption / cancellations"),
        ],
    },

    "atmospheric_river": {
        "commodities": [],
        "vehicles_preferred": ["AWK", "XYL"],
        "sectors": [
            "water_infrastructure",
            "water_engineering",
            "construction",
            "insurance",
            "utilities",
            "hydropower",
        ],
        "long_candidates": [
            _candidate("AWK", "long", "water_infrastructure", 1, 0.90, "Water utility demand / infrastructure"),
            _candidate("XYL", "long", "water_engineering", 1, 0.88, "Flood management / water engineering"),
            _candidate("BEP", "long", "hydropower", 1, 0.85, "Brookfield Renewable — heavy rain boosts hydro generation capacity"),
            _candidate("AES", "long", "hydropower", 2, 0.78, "AES Corp — significant hydro portfolio benefits from AR precipitation"),
            _candidate("HD", "long", "home_repair", 2, 0.70, "Repair demand"),
            _candidate("LOW", "long", "home_repair", 2, 0.70, "Repair demand"),
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.72, "Flood cleanup / infrastructure repair"),
            _candidate("VMC", "long", "construction_materials", 2, 0.68, "Rebuild materials"),
            _candidate("MLM", "long", "construction_materials", 2, 0.65, "Rebuild materials"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.90, "Extreme rainfall claims"),
            _candidate("TRV", "short", "insurance", 1, 0.90, "Extreme rainfall claims"),
            _candidate("CB", "short", "insurance", 1, 0.85, "Claims exposure"),
            _candidate("RNR", "short", "reinsurance", 1, 0.88, "Cat reinsurance exposure"),
            _candidate("PCG", "short", "utilities_liability", 2, 0.65, "Mudslide / infrastructure liability in CA"),
        ],
    },

    "monsoon_failure": {
        "commodities": ["rice", "sugar", "palm_oil"],
        "vehicles_preferred": ["DBA", "NIB"],
        "sectors": [
            "soft_commodities",
            "agriculture",
            "food_manufacturers",
        ],
        "long_candidates": [
            _candidate("ADM", "long", "grain_trading", 1, 0.90, "Global soft commodity supply stress"),
            _candidate("BG", "long", "grain_trading", 1, 0.90, "Global soft commodity supply stress"),
            _candidate("NTR", "long", "fertilizer", 2, 0.70, "Crop input demand post-failure replant"),
            _candidate("CTVA", "long", "crop_inputs", 2, 0.68, "Seed / input demand for replanting"),
            _candidate("MDLZ", "long", "food_cost_pass_through", 3, 0.35, "Soft commodity input cost pressure"),
        ],
        "short_candidates": [],
    },

    "ice_storm": {
        "commodities": ["natural_gas"],
        "vehicles_preferred": ["UNG", "XLU"],
        "sectors": [
            "gas_producers",
            "utilities",
            "insurance",
            "power_infrastructure",
        ],
        "long_candidates": [
            _candidate("EQT", "long", "gas_producers", 1, 0.90, "Gas demand spike from heating"),
            _candidate("RRC", "long", "gas_producers", 1, 0.90, "Gas demand spike from heating"),
            _candidate("CTRA", "long", "gas_producers", 1, 0.85, "Gas demand sensitivity"),
            _candidate("GNRC", "long", "backup_power", 1, 0.88, "Power outage / backup generator demand"),
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.70, "Infrastructure repair cycle"),
            _candidate("HD", "long", "home_repair", 2, 0.65, "Repair / cleanup demand"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.85, "Ice storm property claims"),
            _candidate("TRV", "short", "insurance", 1, 0.85, "Ice storm property claims"),
            _candidate("DAL", "short", "airlines", 1, 0.88, "Severe flight disruption"),
            _candidate("UAL", "short", "airlines", 1, 0.88, "Severe flight disruption"),
        ],
    },

    "extreme_wind": {
        "commodities": ["oil", "natural_gas", "lng"],
        "vehicles_preferred": ["USO", "UNG", "XLE"],
        "sectors": [
            "offshore_energy",
            "lng_exporters",
            "insurance",
            "shipping",
        ],
        "long_candidates": [
            _candidate("EQNR", "long", "offshore_energy", 1, 0.92, "North Sea / offshore wind / supply disruption"),
            _candidate("SHEL", "long", "offshore_energy", 1, 0.88, "Offshore oil / gas supply disruption"),
            _candidate("XOM", "long", "energy", 2, 0.65, "Energy supply disruption"),
            _candidate("CVX", "long", "energy", 2, 0.62, "Energy supply disruption"),
            _candidate("BP", "long", "offshore_energy", 2, 0.68, "Offshore energy exposure"),
            _candidate("SLB", "long", "oilfield_services", 2, 0.60, "Offshore services demand"),
            _candidate("HAL", "long", "oilfield_services", 2, 0.58, "Offshore services demand"),
            _candidate("GNRC", "long", "backup_power", 2, 0.65, "Grid disruption backup power"),
        ],
        "short_candidates": [
            _candidate("ALL", "short", "insurance", 1, 0.88, "Extreme wind property claims"),
            _candidate("TRV", "short", "insurance", 1, 0.88, "Extreme wind property claims"),
            _candidate("RNR", "short", "reinsurance", 1, 0.85, "Cat reinsurance windstorm exposure"),
            _candidate("FRO", "short", "tankers", 2, 0.55, "Tanker disruption in extreme wind"),
            _candidate("GOGL", "short", "dry_bulk", 2, 0.52, "Dry bulk disruption in extreme wind"),
        ],
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
