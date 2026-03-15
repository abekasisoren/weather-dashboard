RADAR_EVENT_RULES = {
    "hurricane": {
        "keywords": ["hurricane", "cyclone", "tropical cyclone", "tropical storm"],
        "trade_map_key": "hurricane",
        "description": "Tropical cyclone or hurricane formation risk",
    },
    "cyclone_cluster": {
        "keywords": ["cyclone_cluster", "storm cluster", "multi-storm cluster"],
        "trade_map_key": "hurricane",
        "description": "Cluster of strong rotating storms",
    },
    "storm_wind_cluster": {
        "keywords": ["storm_wind", "windstorm", "severe wind", "gale", "squall"],
        "trade_map_key": "storm_wind",
        "description": "Severe wind or storm cluster",
    },
    "wildfire_risk": {
        "keywords": ["wildfire", "fire weather", "extreme fire risk", "dry lightning"],
        "trade_map_key": "wildfire",
        "description": "Wildfire probability or fire-weather setup",
    },
    "flood_risk": {
        "keywords": ["flood", "flash flood", "heavy rain", "extreme rainfall"],
        "trade_map_key": "flood",
        "description": "Flood or major rainfall risk",
    },
    "cold_wave": {
        "keywords": ["cold wave", "freeze", "frost", "arctic blast", "polar vortex"],
        "trade_map_key": "cold_wave",
        "description": "Cold outbreak / freeze / polar event",
    },
    "heatwave": {
        "keywords": ["heatwave", "extreme heat", "hot dry"],
        "trade_map_key": "heatwave",
        "description": "Extreme heat event",
    },
    "drought": {
        "keywords": ["drought", "dry spell", "rainfall deficit"],
        "trade_map_key": "drought",
        "description": "Low-rain / drought setup",
    },
}
