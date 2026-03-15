import pandas as pd
from pathlib import Path

weather_df = pd.read_csv("weather_values.csv")
weather = dict(zip(weather_df["metric"], weather_df["value"]))

history_file = Path("weather_history.csv")
if not history_file.exists():
    raise FileNotFoundError("weather_history.csv not found. Run update_weather_values.py first.")

history_df = pd.read_csv(history_file).sort_values(["run_date", "run_time"]).reset_index(drop=True)

signals = []


def history_for(metric):
    if metric not in history_df.columns:
        current = float(weather.get(metric, 0))
        return current, current, 0.0, 0.0
    vals = history_df[metric].dropna().astype(float).tolist()
    if not vals:
        current = float(weather.get(metric, 0))
        return current, current, 0.0, 0.0
    current = vals[-1]
    previous = vals[-2] if len(vals) >= 2 else current
    last_run_change = current - previous
    five_run_change = current - vals[0]
    return current, previous, last_run_change, five_run_change


def storm_level_score(v):
    if v < 15: return 0
    if v < 30: return 1
    if v < 60: return 2
    if v < 100: return 3
    return 4


def storm_trend_score(v):
    v = max(0, v)
    if v < 5: return 0
    if v < 15: return 1
    if v < 30: return 2
    if v < 60: return 3
    return 4


def heat_level_score(v):
    if v < 22: return 0
    if v < 28: return 1
    if v < 34: return 2
    if v < 40: return 3
    return 4


def heat_trend_score(v):
    v = max(0, v)
    if v < 1.5: return 0
    if v < 3: return 1
    if v < 5: return 2
    if v < 8: return 3
    return 4


def cold_level_score(v):
    if v > 8: return 0
    if v > 4: return 1
    if v > 0: return 2
    if v > -5: return 3
    return 4


def cold_trend_score(v):
    v = abs(min(0, v))
    if v < 1.5: return 0
    if v < 3: return 1
    if v < 5: return 2
    if v < 8: return 3
    return 4


def hotdry_level_score(v):
    if v < 10: return 0
    if v < 15: return 1
    if v < 20: return 2
    if v < 25: return 3
    return 4


def hotdry_trend_score(v):
    v = max(0, v)
    if v < 2: return 0
    if v < 5: return 1
    if v < 8: return 2
    if v < 12: return 3
    return 4


def rain_shortage_level_score(v):
    if v > 20: return 0
    if v > 12: return 1
    if v > 7: return 2
    if v > 3: return 3
    return 4


def rain_shortage_trend_score(v):
    v = abs(min(0, v))
    if v < 2: return 0
    if v < 5: return 1
    if v < 8: return 2
    if v < 12: return 3
    return 4


def flood_level_score(v):
    if v < 20: return 0
    if v < 40: return 1
    if v < 70: return 2
    if v < 100: return 3
    return 4


def flood_trend_score(v):
    v = max(0, v)
    if v < 5: return 0
    if v < 15: return 1
    if v < 30: return 2
    if v < 50: return 3
    return 4


def signal_strength_label(score):
    return {0: "Ignore", 1: "Weak", 2: "Moderate", 3: "Strong", 4: "Extreme"}[score]


def action_label(score):
    if score <= 1:
        return "IGNORE"
    if score <= 3:
        return "WATCH"
    return "TRADE"


def format_change(current, previous, last_change, five_change, unit=""):
    return (
        f"Now {current:.2f}{unit} | "
        f"Last run {previous:.2f}{unit} | "
        f"1-run change {last_change:+.2f}{unit} | "
        f"5-run change {five_change:+.2f}{unit}"
    )


def add_signal(
    region,
    weather_event,
    market,
    affected_area,
    affected_industries,
    current_value,
    previous_value,
    last_run_change,
    five_run_change,
    score,
    trade_idea,
    long_stocks="",
    short_stocks="",
    long_etfs="",
    short_etfs="",
    primary_stock="None",
    primary_etf="None",
    primary_futures="None",
    confidence="Medium",
    risk="",
    weather_reasoning="",
    market_reasoning="",
    unit=""
):
    if score <= 1:
        return

    signals.append({
        "Region": region,
        "WeatherEvent": weather_event,
        "Market": market,
        "AffectedArea": affected_area,
        "AffectedIndustries": affected_industries,
        "ForecastChange": format_change(current_value, previous_value, last_run_change, five_run_change, unit),
        "SignalStrength": signal_strength_label(score),
        "Action": action_label(score),
        "TradeIdea": trade_idea,
        "PrimaryStock": primary_stock,
        "PrimaryETF": primary_etf,
        "PrimaryFutures": primary_futures,
        "LongStocks": long_stocks,
        "ShortStocks": short_stocks,
        "LongETFs": long_etfs,
        "ShortETFs": short_etfs,
        "Confidence": confidence,
        "Score": score,
        "CurrentValue": round(current_value, 2),
        "PreviousValue": round(previous_value, 2),
        "LastRunChange": round(last_run_change, 2),
        "FiveRunChange": round(five_run_change, 2),
        "WeatherReasoning": weather_reasoning,
        "MarketReasoning": market_reasoning,
        "RiskNote": risk,
    })


# Existing signals
cur, prev, d1, d5 = history_for("gulf_storm_index")
score = max(storm_level_score(cur), storm_trend_score(d5))
add_signal(
    "Gulf of Mexico", "Storm intensifying", "Oil / Offshore / Insurance",
    "US Gulf Coast / Gulf of Mexico",
    "Offshore oil, LNG terminals, coastal logistics, insurers, cruise lines",
    cur, prev, d1, d5, score,
    "Long offshore energy and LNG; hedge or short insurers and cruise exposure",
    "SLB, HAL, XOM, CVX, LNG, FLNG", "TRV, ALL, HIG, RCL, CCL", "XLE, OIH", "KIE, JETS",
    "SLB", "XLE", "WTI crude / Nat Gas", "Medium",
    "Storm path uncertainty remains important.",
    f"Gulf storm index is {cur:.1f}. One-run change {d1:+.1f}. Five-run change {d5:+.1f}.",
    "Gulf storms can disrupt offshore production, LNG exports and coastal activity."
)

cur, prev, d1, d5 = history_for("us_east_coast_storm_index")
score = max(storm_level_score(cur), storm_trend_score(d5))
add_signal(
    "US East Coast", "Storm intensifying", "Insurance / Utilities / Travel",
    "US East Coast", "Insurers, utilities, airlines, coastal leisure",
    cur, prev, d1, d5, score,
    "Watch utilities; fade insurers, airlines and leisure if storm risk keeps rising",
    "DUK, SO", "TRV, ALL, HIG, UAL, JBLU, RCL, CCL", "", "KIE, JETS",
    "DUK", "KIE", "Regional disruption proxy", "Medium",
    "Exact path matters a lot.",
    f"US East Coast storm index is {cur:.1f}. One-run change {d1:+.1f}. Five-run change {d5:+.1f}.",
    "East Coast storms can hit utilities, travel, coastal leisure and insurers."
)

cur, prev, d1, d5 = history_for("china_east_storm_index")
score = max(storm_level_score(cur), storm_trend_score(d5))
add_signal(
    "East China", "Storm intensifying", "Shipping / Logistics / Insurance",
    "East China coast", "Ports, logistics, shipping, insurers, industrial supply chains",
    cur, prev, d1, d5, score,
    "Watch shipping and logistics disruption; marine risk rising",
    "STNG, TK, FLNG", "TRV, ALL, HIG", "", "KIE",
    "STNG", "KIE", "Freight / shipping proxy", "Low",
    "US-listed proxies are indirect.",
    f"East China storm index is {cur:.1f}. One-run change {d1:+.1f}. Five-run change {d5:+.1f}.",
    "East China storms can disrupt ports, logistics and marine insurance sentiment."
)

cur, prev, d1, d5 = history_for("north_sea_storm_index")
score = max(storm_level_score(cur), storm_trend_score(d5))
add_signal(
    "North Sea", "Storm intensifying", "Energy / Shipping / Insurance",
    "North Sea / Northern Europe", "Offshore energy, shipping, insurance, regional power",
    cur, prev, d1, d5, score,
    "Watch offshore energy and shipping exposure",
    "SHEL, BP, LNG", "TRV, ALL, HIG", "UNG", "KIE",
    "SHEL", "UNG", "Gas / freight proxy", "Low",
    "US-listed proxies are imperfect for direct North Sea exposure.",
    f"North Sea storm index is {cur:.1f}. One-run change {d1:+.1f}. Five-run change {d5:+.1f}.",
    "North Sea storms can affect offshore energy operations and shipping routes."
)

cur, prev, d1, d5 = history_for("texas_mean_temp_c")
score = max(heat_level_score(cur), heat_trend_score(d5))
add_signal(
    "Texas", "Heat building", "Power Demand",
    "Texas / ERCOT power region", "Utilities, power generation, cooling demand, grid stress",
    cur, prev, d1, d5, score,
    "Watch Texas power names as cooling demand rises",
    "VST, NRG, SO, DUK", "", "XLU", "",
    "VST", "XLU", "ERCOT power proxy", "Medium",
    "Public market exposure is indirect.",
    f"Texas mean 96h temperature is {cur:.1f}°C. One-run change {d1:+.1f}°C. Five-run change {d5:+.1f}°C.",
    "Hotter Texas forecasts can increase cooling demand and tighten regional power conditions.",
    "°C"
)

cur, prev, d1, d5 = history_for("nw_europe_mean_temp_c")
score = max(cold_level_score(cur), cold_trend_score(d5))
add_signal(
    "Northwest Europe", "Cold deepening", "Gas / Power",
    "Northwest Europe", "Natural gas, power generation, utilities, heating demand",
    cur, prev, d1, d5, score,
    "Watch gas and power-sensitive names if colder trend continues",
    "LNG, SHEL, BP", "", "UNG", "",
    "LNG", "UNG", "Natural Gas Futures", "Medium",
    "US gas proxies are imperfect for European exposure.",
    f"NW Europe mean 96h temperature is {cur:.1f}°C. One-run change {d1:+.1f}°C. Five-run change {d5:+.1f}°C.",
    "Colder European forecasts can increase heating demand and support gas and power-sensitive assets.",
    "°C"
)

cur, prev, d1, d5 = history_for("argentina_soy_hotdry_score")
score = max(hotdry_level_score(cur), hotdry_trend_score(d5))
add_signal(
    "Argentina Pampas", "Soy crop stress rising", "Soy / Fertilizer / Agribusiness",
    "Argentina Pampas", "Soybeans, agribusiness, fertilizers, crop inputs",
    cur, prev, d1, d5, score,
    "Watch soy and fertilizer-linked names",
    "BG, ADM, MOS, NTR, CF", "", "SOYB, MOO", "",
    "BG", "SOYB", "Soybean Futures", "Medium",
    "South American crop timing matters.",
    f"Argentina hot-dry score is {cur:.1f}. One-run change {d1:+.1f}. Five-run change {d5:+.1f}.",
    "Dryness in Argentina can support soy pricing and input-sensitive names."
)

cur, prev, d1, d5 = history_for("brazil_coffee_precip_mm")
score = max(rain_shortage_level_score(cur), rain_shortage_trend_score(d5))
add_signal(
    "Brazil Coffee Belt", "Rainfall falling", "Coffee",
    "Brazil coffee-growing regions", "Coffee supply chain, coffee futures, beverage input costs",
    cur, prev, d1, d5, score,
    "Coffee risk rising; watch coffee-linked trades",
    "JO", "SBUX", "", "",
    "SBUX", "JO", "Coffee Futures", "Low",
    "Coffee also depends on inventories, FX and crop reports.",
    f"Brazil coffee-belt mean 96h precipitation is {cur:.1f} mm. One-run change {d1:+.1f} mm. Five-run change {d5:+.1f} mm.",
    "Lower expected rainfall can tighten coffee supply expectations.",
    " mm"
)

cur, prev, d1, d5 = history_for("west_africa_cocoa_precip_mm")
score = max(rain_shortage_level_score(cur), rain_shortage_trend_score(d5))
add_signal(
    "West Africa Cocoa Belt", "Rainfall falling", "Cocoa / Food inputs",
    "Ivory Coast / Ghana cocoa belt", "Cocoa supply, chocolate inputs, food manufacturers",
    cur, prev, d1, d5, score,
    "Cocoa supply risk rising; watch cocoa-linked names",
    "HSY, MDLZ, GIS", "", "", "",
    "HSY", "None", "Cocoa Futures", "Low",
    "Cocoa markets are very inventory- and flow-driven.",
    f"West Africa cocoa-belt mean 96h precipitation is {cur:.1f} mm. One-run change {d1:+.1f} mm. Five-run change {d5:+.1f} mm.",
    "Reduced rainfall can worsen cocoa supply stress and pressure food-input-sensitive names.",
    " mm"
)

# Phase 1 additions

cur, prev, d1, d5 = history_for("canadian_prairies_hotdry_score")
score = max(hotdry_level_score(cur), hotdry_trend_score(d5))
add_signal(
    "Canadian Prairies", "Crop stress rising", "Wheat / Canola / Fertilizer",
    "Canadian Prairies", "Wheat, canola, fertilizers, agribusiness",
    cur, prev, d1, d5, score,
    "Watch grain and fertilizer exposure if prairie stress keeps building",
    "NTR, MOS, CF, ADM, BG", "", "WEAT, MOO", "",
    "NTR", "MOO", "Wheat / canola proxy", "Medium",
    "Canadian crop timing matters.",
    f"Canadian Prairies hot-dry score is {cur:.1f}. One-run change {d1:+.1f}. Five-run change {d5:+.1f}.",
    "Hotter and drier prairie weather can affect wheat/canola yields and fertilizer demand."
)

cur, prev, d1, d5 = history_for("mato_grosso_hotdry_score")
score = max(hotdry_level_score(cur), hotdry_trend_score(d5))
add_signal(
    "Mato Grosso", "Soy / corn stress rising", "Soy / Corn / Fertilizer",
    "Central Brazil / Mato Grosso", "Soybeans, corn, fertilizers, export-linked agribusiness",
    cur, prev, d1, d5, score,
    "Watch soy, corn and fertilizer exposure if central Brazil dries further",
    "BG, ADM, MOS, NTR, CF", "", "SOYB, CORN, MOO", "",
    "BG", "SOYB", "Soy / corn futures", "Medium",
    "Brazil crop timing and logistics matter.",
    f"Mato Grosso hot-dry score is {cur:.1f}. One-run change {d1:+.1f}. Five-run change {d5:+.1f}.",
    "Dryness in central Brazil can affect soy/corn yields and fertilizer-sensitive names."
)

cur, prev, d1, d5 = history_for("rhine_corridor_precip_mm")
score = max(flood_level_score(cur), flood_trend_score(d5))
add_signal(
    "Rhine Corridor", "River / logistics disruption risk", "Chemicals / Logistics / Inland Shipping",
    "Rhine River corridor", "Chemicals, inland shipping, industrial logistics, European supply chains",
    cur, prev, d1, d5, score,
    "Watch logistics and chemical flow disruption in the Rhine corridor",
    "SHEL, BP", "", "", "",
    "SHEL", "None", "European logistics proxy", "Low",
    "US-listed direct proxies are limited.",
    f"Rhine corridor precipitation is {cur:.1f} mm. One-run change {d1:+.1f} mm. Five-run change {d5:+.1f} mm.",
    "Heavy rain or river disruption can affect industrial logistics and inland shipping.",
    " mm"
)

cur, prev, d1, d5 = history_for("panama_canal_precip_mm")
score = max(rain_shortage_level_score(cur), rain_shortage_trend_score(d5))
add_signal(
    "Panama Canal", "Rainfall falling", "Shipping / Container Flow",
    "Panama Canal watershed", "Shipping, container routing, tanker flows, canal logistics",
    cur, prev, d1, d5, score,
    "Watch shipping-rate sensitivity and routing stress if canal rainfall drops",
    "STNG, TK, FLNG", "", "", "",
    "STNG", "None", "Freight / tanker proxy", "Low",
    "Canal impacts filter into rates with a lag.",
    f"Panama Canal zone precipitation is {cur:.1f} mm. One-run change {d1:+.1f} mm. Five-run change {d5:+.1f} mm.",
    "Lower rainfall can tighten canal operations and affect shipping routes."
)

cur, prev, d1, d5 = history_for("sea_palm_oil_precip_mm")
score = max(rain_shortage_level_score(cur), rain_shortage_trend_score(d5))
add_signal(
    "SE Asia Palm Oil Belt", "Rainfall falling", "Palm Oil / Food Inputs",
    "Malaysia / Indonesia palm oil belt", "Palm oil, food inputs, ag commodities",
    cur, prev, d1, d5, score,
    "Watch palm-oil-linked food input risk if rainfall keeps dropping",
    "MDLZ, GIS, ADM", "", "MOO", "",
    "ADM", "MOO", "Palm oil / food input proxy", "Low",
    "US-listed direct palm oil proxies are limited.",
    f"SE Asia palm belt precipitation is {cur:.1f} mm. One-run change {d1:+.1f} mm. Five-run change {d5:+.1f} mm.",
    "Lower rainfall can tighten palm oil supply expectations and food input pressure.",
    " mm"
)

if not signals:
    signals.append({
        "Region": "None",
        "WeatherEvent": "No meaningful weather signal",
        "Market": "None",
        "AffectedArea": "None",
        "AffectedIndustries": "None",
        "ForecastChange": "No meaningful move across the last 5 ECMWF runs",
        "SignalStrength": "Ignore",
        "Action": "IGNORE",
        "TradeIdea": "No action",
        "PrimaryStock": "None",
        "PrimaryETF": "None",
        "PrimaryFutures": "None",
        "LongStocks": "",
        "ShortStocks": "",
        "LongETFs": "",
        "ShortETFs": "",
        "Confidence": "Low",
        "Score": 0,
        "CurrentValue": 0,
        "PreviousValue": 0,
        "LastRunChange": 0,
        "FiveRunChange": 0,
        "WeatherReasoning": "No level or 5-run trend signal was strong enough.",
        "MarketReasoning": "No fresh weather-based market signal.",
        "RiskNote": "Wait for the next forecast update."
    })

df = pd.DataFrame(signals)
df.to_csv("signals.csv", index=False)

print("signals.csv generated with phase 1 expansion")
print(df[["Region", "WeatherEvent", "SignalStrength", "Action", "TradeIdea"]])
