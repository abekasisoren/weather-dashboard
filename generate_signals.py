import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set.")

weather = pd.read_csv("weather_values.csv")


def normalize(value: float, low: float, high: float) -> float:
    if high == low:
        return 0.0
    scaled = 10.0 * (value - low) / (high - low)
    return float(max(0.0, min(10.0, scaled)))


def score_signal(severity: float, surprise: float, market: float, persistence: float = 5.0) -> float:
    score = (
        0.40 * surprise +
        0.30 * severity +
        0.20 * market +
        0.10 * persistence
    )
    return round(min(10.0, score), 2)


def action_from_score(score: float) -> str:
    if score >= 8:
        return "TRADE"
    if score >= 6:
        return "WATCH"
    return "IGNORE"


weather_map = {row["metric"]: float(row["value"]) for _, row in weather.iterrows()}

signals = []

# 1. Brazil coffee
if "brazil_coffee_precip_mm" in weather_map:
    value = weather_map["brazil_coffee_precip_mm"]
    severity = normalize(20 - value, 0, 20)
    surprise = normalize(15 - value, 0, 15)
    market = 9.0
    score = score_signal(severity, surprise, market)

    signals.append({
        "region": "Brazil Coffee Belt",
        "weather_event": "Rainfall stress building",
        "score": score,
        "recommendation": action_from_score(score),
        "weather_logic": "Rainfall forecasts are falling across Brazilian coffee-growing regions in recent ECMWF runs.",
        "market_logic": "Lower rainfall can tighten coffee supply expectations and support coffee-linked instruments.",
        "best_vehicle": "Coffee Futures / JO ETF",
        "proxy_equities": "SBUX, coffee exporters"
    })

# 2. Argentina soy
if "argentina_soy_hotdry_score" in weather_map:
    value = weather_map["argentina_soy_hotdry_score"]
    severity = normalize(value, 0, 15)
    surprise = normalize(value, 0, 15)
    market = 8.0
    score = score_signal(severity, surprise, market)

    signals.append({
        "region": "Argentina Pampas",
        "weather_event": "Hot/Dry crop stress",
        "score": score,
        "recommendation": action_from_score(score),
        "weather_logic": "Hot and dry conditions are strengthening across major soybean-growing regions in Argentina.",
        "market_logic": "Soybean yields are sensitive to hot/dry stress, which can tighten supply expectations.",
        "best_vehicle": "Soybean Futures / SOYB",
        "proxy_equities": "ADM, BG"
    })

# 3. US Corn Belt
if "cornbelt_hotdry_score" in weather_map:
    value = weather_map["cornbelt_hotdry_score"]
    severity = normalize(value, 0, 15)
    surprise = normalize(value, 0, 15)
    market = 9.0
    score = score_signal(severity, surprise, market)

    signals.append({
        "region": "US Corn Belt",
        "weather_event": "Heat and dryness rising",
        "score": score,
        "recommendation": action_from_score(score),
        "weather_logic": "Heat and dryness are increasing across the US Corn Belt.",
        "market_logic": "Corn yield expectations are highly sensitive to combined heat and moisture stress.",
        "best_vehicle": "Corn Futures / CORN ETF",
        "proxy_equities": "ADM, fertilizer companies"
    })

# 4. West Africa cocoa
if "west_africa_cocoa_precip_mm" in weather_map:
    value = weather_map["west_africa_cocoa_precip_mm"]
    severity = normalize(15 - value, 0, 15)
    surprise = normalize(15 - value, 0, 15)
    market = 8.0
    score = score_signal(severity, surprise, market)

    signals.append({
        "region": "West Africa Cocoa Belt",
        "weather_event": "Rainfall risk shifting",
        "score": score,
        "recommendation": action_from_score(score),
        "weather_logic": "Rainfall anomalies are emerging across Ivory Coast and Ghana.",
        "market_logic": "Cocoa production is sensitive to rainfall shifts during key growing periods.",
        "best_vehicle": "Cocoa Futures",
        "proxy_equities": "Chocolate producers"
    })

# 5. Panama Canal
if "panama_canal_precip_mm" in weather_map:
    value = weather_map["panama_canal_precip_mm"]
    severity = normalize(12 - value, 0, 12)
    surprise = normalize(10 - value, 0, 10)
    market = 8.0
    score = score_signal(severity, surprise, market)

    signals.append({
        "region": "Panama Canal",
        "weather_event": "Rainfall deficit",
        "score": score,
        "recommendation": action_from_score(score),
        "weather_logic": "Lower rainfall threatens canal water levels.",
        "market_logic": "Reduced shipping throughput can affect freight rates and routing stress.",
        "best_vehicle": "Shipping exposure",
        "proxy_equities": "ZIM, shipping companies"
    })

# 6. Northwest Europe cold
if "nw_europe_mean_temp_c" in weather_map:
    value = weather_map["nw_europe_mean_temp_c"]
    severity = normalize(8 - value, 0, 8)
    surprise = normalize(6 - value, 0, 6)
    market = 7.5
    score = score_signal(severity, surprise, market)

    signals.append({
        "region": "Northwest Europe",
        "weather_event": "Cold deepening",
        "score": score,
        "recommendation": action_from_score(score),
        "weather_logic": "Temperatures are trending colder across Northwest Europe in the ECMWF forecast.",
        "market_logic": "Colder weather can lift heating demand and increase power and gas sensitivity.",
        "best_vehicle": "Natural Gas / Power",
        "proxy_equities": "LNG, SHEL, BP"
    })

# 7. Canadian Prairies
if "canadian_prairies_hotdry_score" in weather_map:
    value = weather_map["canadian_prairies_hotdry_score"]
    severity = normalize(value, 0, 15)
    surprise = normalize(value, 0, 15)
    market = 7.0
    score = score_signal(severity, surprise, market)

    signals.append({
        "region": "Canadian Prairies",
        "weather_event": "Crop stress rising",
        "score": score,
        "recommendation": action_from_score(score),
        "weather_logic": "Hot/dry crop stress is building across the Canadian Prairies.",
        "market_logic": "Wheat and canola exposure becomes more sensitive as prairie stress builds.",
        "best_vehicle": "Wheat / Canola theme",
        "proxy_equities": "NTR, MOS, CF, ADM, BG"
    })

# 8. US wheat
if "us_wheat_hotdry_score" in weather_map:
    value = weather_map["us_wheat_hotdry_score"]
    severity = normalize(value, 0, 15)
    surprise = normalize(value, 0, 15)
    market = 8.0
    score = score_signal(severity, surprise, market)

    signals.append({
        "region": "US Wheat Plains",
        "weather_event": "Wheat stress rising",
        "score": score,
        "recommendation": action_from_score(score),
        "weather_logic": "Hot/dry conditions are increasing across US wheat-growing areas.",
        "market_logic": "Wheat supply expectations can tighten when Plains stress builds.",
        "best_vehicle": "Wheat Futures / WEAT",
        "proxy_equities": "ADM, BG"
    })

# 9. SE Asia palm oil
if "sea_palm_oil_hotdry_score" in weather_map:
    value = weather_map["sea_palm_oil_hotdry_score"]
    severity = normalize(value, 0, 20)
    surprise = normalize(value, 0, 20)
    market = 7.5
    score = score_signal(severity, surprise, market)

    signals.append({
        "region": "SE Asia Palm Oil Belt",
        "weather_event": "Palm oil weather stress",
        "score": score,
        "recommendation": action_from_score(score),
        "weather_logic": "Heat and dryness are building across palm-oil-sensitive regions in Southeast Asia.",
        "market_logic": "Palm oil and related food-input supply can tighten when stress rises.",
        "best_vehicle": "Palm oil / food-input theme",
        "proxy_equities": "ADM, MDLZ, GIS"
    })

signals_df = pd.DataFrame(signals)

if signals_df.empty:
    signals_df = pd.DataFrame(columns=[
        "region",
        "weather_event",
        "score",
        "recommendation",
        "weather_logic",
        "market_logic",
        "best_vehicle",
        "proxy_equities",
        "updated_at",
    ])
else:
    signals_df["updated_at"] = datetime.now(timezone.utc)

signals_df = signals_df.sort_values(by="score", ascending=False, na_position="last")

with psycopg.connect(DATABASE_URL) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_signals (
                id BIGSERIAL PRIMARY KEY,
                region TEXT NOT NULL,
                weather_event TEXT NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                recommendation TEXT NOT NULL,
                weather_logic TEXT NOT NULL,
                market_logic TEXT NOT NULL,
                best_vehicle TEXT NOT NULL,
                proxy_equities TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
        """)

        cur.execute("TRUNCATE TABLE weather_signals")

        if not signals_df.empty:
            rows = [
                (
                    r["region"],
                    r["weather_event"],
                    float(r["score"]),
                    r["recommendation"],
                    r["weather_logic"],
                    r["market_logic"],
                    r["best_vehicle"],
                    r["proxy_equities"],
                    r["updated_at"],
                )
                for _, r in signals_df.iterrows()
            ]

            cur.executemany("""
                INSERT INTO weather_signals (
                    region,
                    weather_event,
                    score,
                    recommendation,
                    weather_logic,
                    market_logic,
                    best_vehicle,
                    proxy_equities,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, rows)

        conn.commit()

print("weather_signals table updated in Postgres")
print(signals_df[["region", "weather_event", "score", "recommendation"]].to_string(index=False))
