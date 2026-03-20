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
        "commodities": ["corn", "soybeans", "wheat", "coffee", "sugar", "copper", "lithium",
                        "shipping", "phosphate", "iron_ore", "platinum"],
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
            "shipping_rates",
            "phosphate",
            "iron_ore_supply",
            "platinum_mining",
        ],
        "long_candidates": [
            _candidate("ADM",   "long", "grain_trading",       1, 0.95, "Direct grains/softs supply stress linkage"),
            _candidate("BG",    "long", "grain_trading",       1, 0.95, "Direct grains/softs supply stress linkage"),
            _candidate("GOGL",  "long", "shipping_rates",      1, 0.90, "Panama Canal drought → restricted transits → dry bulk rate spike"),
            _candidate("DSX",   "long", "shipping_rates",      1, 0.88, "Diana Shipping — Panama Canal low water = rerouting = higher ton-miles"),
            _candidate("MOS",   "long", "phosphate",           1, 0.88, "Mosaic — Morocco drought = phosphate supply crunch → pricing power"),
            _candidate("FCX",   "long", "copper_mining",       1, 0.88, "Drought disrupts water-intensive copper mining — supply shock"),
            _candidate("SCCO",  "long", "copper_mining",       1, 0.88, "Southern Copper — Andes mining water dependency"),
            _candidate("ALB",   "long", "lithium_mining",      1, 0.85, "Lithium brine operations require water — drought = supply stress"),
            _candidate("ZIM",   "long", "shipping_rates",      2, 0.75, "Container shipping — Panama Canal drought raises rates globally"),
            _candidate("DAC",   "long", "shipping_rates",      2, 0.72, "Danaos — container shipping drought/transit disruption"),
            _candidate("SBSW",  "long", "platinum_mining",     2, 0.75, "Sibanye Stillwater — South Africa drought → Eskom power rationing → platinum mine cuts"),
            _candidate("IMPUY", "long", "platinum_mining",     2, 0.68, "Impala Platinum — drought/power rationing mine production disruption"),
            _candidate("AWK",   "long", "water_infrastructure",1, 0.88, "Water utility demand surges in drought conditions"),
            _candidate("XYL",   "long", "water_engineering",   2, 0.72, "Water management / drought infrastructure"),
            _candidate("CF",    "long", "fertilizer",          2, 0.75, "Crop stress can support ag input theme"),
            _candidate("NTR",   "long", "fertilizer",          2, 0.72, "Nutrien fertilizer linkage + Morocco phosphate angle"),
            _candidate("SQM",   "long", "lithium_mining",      2, 0.72, "SQM — Andes lithium, water-intensive operations"),
            _candidate("BHP",   "long", "iron_ore_supply",     2, 0.68, "Western Australia drought disrupts iron ore and lithium mining"),
            _candidate("RIO",   "long", "iron_ore_supply",     2, 0.65, "Rio Tinto — drought exposure at Australian and global mining"),
            _candidate("CTVA",  "long", "crop_inputs",         2, 0.70, "Crop input exposure"),
            _candidate("CZZ",   "long", "sugar_exposure",      2, 0.60, "Sugar linkage more direct than consumer names"),
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
        "commodities": ["garments"],
        "vehicles_preferred": ["XHB", "ITB"],
        "sectors": [
            "infrastructure_repair",
            "construction_materials",
            "water_engineering",
            "home_repair",
            "insurance",
            "garment_supply",
        ],
        "long_candidates": [
            _candidate("HD",  "long", "home_repair", 1, 0.90, "Repair / rebuild demand"),
            _candidate("LOW", "long", "home_repair", 1, 0.90, "Repair / rebuild demand"),
            _candidate("CAT", "long", "infrastructure_repair", 2, 0.75, "Repair cycle equipment"),
            _candidate("VMC", "long", "construction_materials", 2, 0.75, "Materials exposure"),
            _candidate("MLM", "long", "construction_materials", 2, 0.75, "Materials exposure"),
            _candidate("XYL", "long", "water_engineering", 2, 0.70, "Flood / water infrastructure"),
            _candidate("LEN", "long", "homebuilder", 2, 0.65, "Post-flood rebuild demand"),
        ],
        "short_candidates": [
            _candidate("PVH", "short", "garment_supply",  1, 0.85, "Bangladesh/Asia flood disrupts garment production — PVH (Calvin Klein, Tommy) sourcing"),
            _candidate("HBI", "short", "garment_supply",  1, 0.82, "Hanesbrands — Bangladesh flooding disrupts supply chain"),
            _candidate("VFC", "short", "garment_supply",  2, 0.75, "VF Corp — Asian apparel supply chain flood exposure"),
            _candidate("RL",  "short", "garment_supply",  2, 0.68, "Ralph Lauren — Asia supply chain flood disruption"),
            _candidate("ALL", "short", "insurance", 1, 0.95, "Claims risk"),
            _candidate("TRV", "short", "insurance", 1, 0.95, "Claims risk"),
            _candidate("CB",  "short", "insurance", 1, 0.90, "Claims risk"),
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
        "commodities": ["oil", "natural_gas", "iron_ore", "semiconductors", "automotive"],
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
            "semiconductor_fab",
            "automotive_supply",
            "iron_ore_supply",
        ],
        "long_candidates": [
            _candidate("GNRC", "long", "generators", 1, 0.95, "One of the cleanest hurricane trades"),
            _candidate("HD",   "long", "home_repair", 1, 0.90, "Repair / prep demand"),
            _candidate("LOW",  "long", "home_repair", 1, 0.90, "Repair / prep demand"),
            _candidate("AMAT", "long", "semiconductor_equip", 2, 0.72, "Fab rebuild/replacement equipment demand post-storm"),
            _candidate("KLAC", "long", "semiconductor_equip", 2, 0.70, "Fab equipment demand post-storm"),
            _candidate("LRCX", "long", "semiconductor_equip", 2, 0.68, "Lam Research — fab rebuild equipment"),
            _candidate("BHP",  "long", "iron_ore_supply",     2, 0.68, "Western Australia cyclone disrupts iron ore supply → price spike"),
            _candidate("RIO",  "long", "iron_ore_supply",     2, 0.65, "Rio Tinto — cyclone disruption to Port Hedland iron ore"),
            _candidate("CAT",  "long", "infrastructure_repair", 2, 0.75, "Post-storm reconstruction"),
            _candidate("VMC",  "long", "construction_materials", 2, 0.75, "Rebuild materials"),
            _candidate("XOM",  "long", "energy", 2, 0.60, "Can benefit if storm disrupts supply"),
            _candidate("CVX",  "long", "energy", 2, 0.60, "Can benefit if storm disrupts supply"),
            _candidate("SHEL", "long", "energy_intl", 2, 0.58, "Gulf / offshore exposure via ADR"),
            _candidate("LEN",  "long", "homebuilder", 2, 0.65, "Post-storm rebuild/replacement housing"),
            _candidate("PHM",  "long", "homebuilder", 2, 0.62, "Post-storm rebuild/replacement housing"),
            _candidate("FRO",  "long", "tankers", 3, 0.30, "Crude tanker disruption / rerouting angle"),
        ],
        "short_candidates": [
            _candidate("TSM",  "short", "semiconductor_fab",  1, 0.92, "TSMC — Taiwan typhoon/hurricane direct fab damage risk"),
            _candidate("TM",   "short", "automotive_supply",  2, 0.72, "Toyota — Japan/Asia typhoon factory and supply chain disruption"),
            _candidate("HMC",  "short", "automotive_supply",  2, 0.68, "Honda — Japan typhoon manufacturing disruption"),
            _candidate("SONY", "short", "electronics_supply", 2, 0.65, "Sony — Japan typhoon electronics supply chain risk"),
            _candidate("ALL",  "short", "insurance", 1, 0.95, "High claims sensitivity"),
            _candidate("TRV",  "short", "insurance", 1, 0.95, "High claims sensitivity"),
            _candidate("CB",   "short", "insurance", 1, 0.90, "Claims sensitivity"),
            _candidate("RNR",  "short", "reinsurance", 1, 0.92, "Catastrophe reinsurance — hurricane is the top peril"),
            _candidate("AXS",  "short", "reinsurance", 2, 0.78, "Specialty lines hurricane exposure"),
            _candidate("MKL",  "short", "specialty_insurance", 2, 0.70, "Specialty insurance claims"),
            _candidate("RCL",  "short", "cruise", 1, 0.92, "Royal Caribbean — direct itinerary cancellations, port closures"),
            _candidate("CCL",  "short", "cruise", 1, 0.90, "Carnival Corp — Caribbean route hurricane losses"),
            _candidate("NCLH", "short", "cruise", 2, 0.82, "Norwegian Cruise — Caribbean itinerary exposure"),
            _candidate("PVH",  "short", "garment_supply", 2, 0.72, "PVH (Calvin Klein, Tommy) — Bangladesh/Asia garment supply disruption"),
            _candidate("HBI",  "short", "garment_supply", 2, 0.68, "Hanesbrands — heavy Bangladesh/Asia sourcing exposure"),
        ],
    },

    "hurricane_risk": {
        "commodities": ["oil", "natural_gas", "iron_ore", "semiconductors"],
        "vehicles_preferred": ["USO", "UNG", "XLE"],
        "sectors": [
            "construction",
            "generators",
            "shipping",
            "energy",
            "insurance",
            "reinsurance",
            "cruise",
            "semiconductor_fab",
            "iron_ore_supply",
        ],
        "long_candidates": [
            _candidate("GNRC", "long", "generators", 1, 0.95, "Clean storm preparation trade"),
            _candidate("HD",   "long", "home_repair", 1, 0.88, "Preparation / repair demand"),
            _candidate("LOW",  "long", "home_repair", 1, 0.88, "Preparation / repair demand"),
            _candidate("BHP",  "long", "iron_ore_supply", 2, 0.65, "Cyclone risk to Western Australia iron ore supply"),
            _candidate("RIO",  "long", "iron_ore_supply", 2, 0.62, "Rio Tinto — cyclone disruption risk to iron ore"),
            _candidate("AMAT", "long", "semiconductor_equip", 2, 0.68, "Fab equipment demand if Taiwan/Asia fabs impacted"),
            _candidate("CAT",  "long", "infrastructure_repair", 2, 0.70, "Repair cycle"),
            _candidate("VMC",  "long", "construction_materials", 2, 0.70, "Repair cycle"),
            _candidate("XOM",  "long", "energy", 2, 0.60, "Storm supply disruption angle"),
            _candidate("CVX",  "long", "energy", 2, 0.60, "Storm supply disruption angle"),
            _candidate("SHEL", "long", "energy_intl", 2, 0.55, "Gulf/offshore exposure"),
            _candidate("LEN",  "long", "homebuilder", 2, 0.60, "Pre-storm prep / rebuild"),
        ],
        "short_candidates": [
            _candidate("TSM",  "short", "semiconductor_fab", 1, 0.90, "TSMC — typhoon track risk to Taiwan fab operations"),
            _candidate("TM",   "short", "automotive_supply", 2, 0.70, "Toyota — typhoon/storm supply chain disruption risk"),
            _candidate("HMC",  "short", "automotive_supply", 2, 0.65, "Honda — Asia storm factory/supply risk"),
            _candidate("ALL",  "short", "insurance", 1, 0.95, "Claims sensitivity"),
            _candidate("TRV",  "short", "insurance", 1, 0.95, "Claims sensitivity"),
            _candidate("CB",   "short", "insurance", 1, 0.90, "Claims sensitivity"),
            _candidate("RNR",  "short", "reinsurance", 1, 0.90, "Cat reinsurance loss exposure"),
            _candidate("AXS",  "short", "reinsurance", 2, 0.75, "Specialty lines hurricane exposure"),
            _candidate("RCL",  "short", "cruise", 1, 0.88, "Royal Caribbean — itinerary cancellations, port disruptions"),
            _candidate("CCL",  "short", "cruise", 1, 0.85, "Carnival Corp — Caribbean route hurricane exposure"),
            _candidate("NCLH", "short", "cruise", 2, 0.80, "Norwegian Cruise — Caribbean itinerary disruption"),
            _candidate("PVH",  "short", "garment_supply", 2, 0.68, "PVH — Bangladesh cyclone garment supply disruption risk"),
            _candidate("HBI",  "short", "garment_supply", 2, 0.65, "Hanesbrands — Asia garment supply chain risk"),
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
        "commodities": ["natural_gas", "automotive", "semiconductors"],
        "vehicles_preferred": ["UNG"],
        "sectors": [
            "utilities",
            "lng_exporters",
            "gas_producers",
            "airlines",
            "hvac_heating",
            "ski_resorts",
            "automotive_supply",
            "semiconductor_fab",
        ],
        "long_candidates": [
            _candidate("EQT",  "long", "gas_producers", 1, 0.95, "Direct gas sensitivity"),
            _candidate("RRC",  "long", "gas_producers", 1, 0.95, "Direct gas sensitivity"),
            _candidate("CTRA", "long", "gas_producers", 1, 0.90, "Direct gas sensitivity"),
            _candidate("LNG",  "long", "lng_exporters", 2, 0.70, "Gas / LNG sensitivity"),
            _candidate("SHEL", "long", "lng_intl", 2, 0.65, "LNG exporter with European exposure — Japan/Korea cold wave spikes LNG imports"),
            _candidate("BP",   "long", "lng_intl", 2, 0.62, "LNG and gas exposure"),
            _candidate("EQNR", "long", "lng_intl", 2, 0.65, "North Sea gas / LNG exporter"),
            _candidate("CARR", "long", "hvac_heating", 2, 0.65, "Heating system demand during cold events"),
            _candidate("MTN",  "long", "ski_resorts", 2, 0.72, "Vail Resorts — cold/snow conditions boost ski season bookings"),
        ],
        "short_candidates": [
            _candidate("TM",   "short", "automotive_supply",  1, 0.80, "Toyota — Japan/Korea cold wave freezes auto factories and logistics"),
            _candidate("HMC",  "short", "automotive_supply",  1, 0.78, "Honda — Korea/Japan cold snap disrupts manufacturing"),
            _candidate("SONY", "short", "electronics_supply", 2, 0.65, "Sony — Japan cold wave electronics/semiconductor disruption"),
            _candidate("TSM",  "short", "semiconductor_fab",  2, 0.62, "TSMC — cold snap disrupts fab operations in Taiwan/Korea"),
            _candidate("DAL",  "short", "airlines", 1, 0.85, "Weather disruption / cost sensitivity"),
            _candidate("UAL",  "short", "airlines", 1, 0.85, "Weather disruption / cost sensitivity"),
            _candidate("AAL",  "short", "airlines", 1, 0.85, "Weather disruption / cost sensitivity"),
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
        "commodities": ["rice", "sugar", "palm_oil", "garments", "semiconductors", "automotive"],
        "vehicles_preferred": ["DBA", "NIB"],
        "sectors": [
            "soft_commodities",
            "agriculture",
            "food_manufacturers",
            "garment_supply",
            "semiconductor_supply",
            "automotive_supply",
        ],
        "long_candidates": [
            _candidate("ADM",   "long", "grain_trading", 1, 0.90, "Global soft commodity supply stress"),
            _candidate("BG",    "long", "grain_trading", 1, 0.90, "Global soft commodity supply stress"),
            _candidate("NTR",   "long", "fertilizer",    2, 0.70, "Crop input demand post-failure replant"),
            _candidate("CTVA",  "long", "crop_inputs",   2, 0.68, "Seed / input demand for replanting"),
            _candidate("AMAT",  "long", "semiconductor_equip", 2, 0.65, "Fab equipment replacement if monsoon failure hits Asia chip production"),
        ],
        "short_candidates": [
            _candidate("PVH",  "short", "garment_supply",    1, 0.90, "PVH (Calvin Klein, Tommy Hilfiger) — Bangladesh monsoon failure = garment production collapse"),
            _candidate("HBI",  "short", "garment_supply",    1, 0.88, "Hanesbrands — Bangladesh/S. Asia sourcing = direct monsoon exposure"),
            _candidate("VFC",  "short", "garment_supply",    1, 0.82, "VF Corp (North Face, Timberland) — Asian manufacturing monsoon disruption"),
            _candidate("RL",   "short", "garment_supply",    2, 0.75, "Ralph Lauren — premium apparel Asia supply chain disruption"),
            _candidate("COLM", "short", "garment_supply",    2, 0.70, "Columbia Sportswear — Asian manufacturing monsoon exposure"),
            _candidate("TSM",  "short", "semiconductor_fab", 2, 0.68, "TSMC — monsoon failure / water shortage can disrupt fab cooling/water supply"),
            _candidate("TM",   "short", "automotive_supply", 2, 0.65, "Toyota — Asia monsoon failure disrupts component supply chains"),
        ],
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
        "commodities": ["oil", "natural_gas", "lng", "iron_ore", "semiconductors", "shipping"],
        "vehicles_preferred": ["USO", "UNG", "XLE"],
        "sectors": [
            "offshore_energy",
            "lng_exporters",
            "insurance",
            "shipping",
            "semiconductor_fab",
            "iron_ore_supply",
        ],
        "long_candidates": [
            _candidate("EQNR", "long", "offshore_energy",     1, 0.92, "North Sea / offshore wind / supply disruption"),
            _candidate("SHEL", "long", "offshore_energy",     1, 0.88, "Offshore oil / gas supply disruption"),
            _candidate("BHP",  "long", "iron_ore_supply",     1, 0.85, "Western Australia extreme wind shuts Port Hedland → iron ore price spike"),
            _candidate("RIO",  "long", "iron_ore_supply",     1, 0.82, "Rio Tinto — Port Hedland closure drives iron ore supply shock"),
            _candidate("AMAT", "long", "semiconductor_equip", 2, 0.70, "Fab equipment demand when Taiwan/Asia fabs are damaged"),
            _candidate("XOM",  "long", "energy", 2, 0.65, "Energy supply disruption"),
            _candidate("CVX",  "long", "energy", 2, 0.62, "Energy supply disruption"),
            _candidate("BP",   "long", "offshore_energy", 2, 0.68, "Offshore energy exposure"),
            _candidate("SLB",  "long", "oilfield_services", 2, 0.60, "Offshore services demand"),
            _candidate("HAL",  "long", "oilfield_services", 2, 0.58, "Offshore services demand"),
            _candidate("GNRC", "long", "backup_power", 2, 0.65, "Grid disruption backup power"),
        ],
        "short_candidates": [
            _candidate("TSM",  "short", "semiconductor_fab", 1, 0.90, "TSMC — extreme wind / typhoon direct Taiwan fab risk"),
            _candidate("TM",   "short", "automotive_supply", 2, 0.68, "Toyota — extreme wind disrupts Japan/Korea auto manufacturing"),
            _candidate("ALL",  "short", "insurance", 1, 0.88, "Extreme wind property claims"),
            _candidate("TRV",  "short", "insurance", 1, 0.88, "Extreme wind property claims"),
            _candidate("RNR",  "short", "reinsurance", 1, 0.85, "Cat reinsurance windstorm exposure"),
            _candidate("FRO",  "short", "tankers", 2, 0.55, "Tanker disruption in extreme wind"),
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
