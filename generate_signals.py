import os
from datetime import datetime, timezone

import pandas as pd
import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set.")

history = pd.read_csv("weather_history.csv")


def latest_and_prior_avg(metric_name: str):
    if metric_name not in history.columns:
        return None, None, None

    series = pd.to_numeric(history[metric_name], errors="coerce").dropna()
    if len(series) == 0:
        return None, None, None

    current = float(series.iloc[-1])

    if len(series) == 1:
        prior_avg = current
    else:
        prior_avg = float(series.iloc[:-1].mean())

    delta = current - prior_avg
    return current, prior_avg, delta


def signal_level(change_material: bool, outcome_material: bool):
    if change_material and outcome_material:
        return "HIGH CONVICTION", "ACT"
    if change_material or outcome_material:
        if change_material:
            return "EARLY SIGNAL", "WATCH"
        return "ACTIONABLE", "PREPARE"
    return None, None


signals = []


def add_signal(
    region: str,
    weather_event: str,
    metric_name: str,
    change_rule,
    outcome_rule,
    what_changed_builder,
    why_it_matters: str,
    affected_market: str,
    best_vehicle: str,
    proxy_equities: str,
    what_to_watch_next: str,
):
    current, prior_avg, delta = latest_and_prior_avg(metric_name)

    if current is None:
        return

    change_material = change_rule(current, prior_avg, delta)
    outcome_material = outcome_rule(current, prior_avg, delta)

    level, recommendation = signal_level(change_material, outcome_material)

    if level is None:
        return

    what_changed = what_changed_builder(current, prior_avg, delta)

    signals.append({
        "region": region,
        "weather_event": weather_event,
        "signal_level": level,
        "recommendation": recommendation,
        "metric_name": metric_name,
        "current_value": round(current, 2),
        "prior_avg_value": round(prior_avg, 2),
        "delta_value": round(delta, 2),
        "what_changed": what_changed,
        "why_it_matters": why_it_matters,
        "affected_market": affected_market,
        "best_vehicle": best_vehicle,
        "proxy_equities": proxy_equities,
        "what_to_watch_next": what_to_watch_next,
        "updated_at": datetime.now(timezone.utc),
    })


# 1. Brazil coffee
add_signal(
    region="Brazil Coffee Belt",
    weather_event="Rainfall turning drier",
    metric_name="brazil_coffee_precip_mm",
    change_rule=lambda c, p, d: d <= -3.0,
    outcome_rule=lambda c, p, d: c <= 12.0,
    what_changed_builder=lambda c, p, d: (
        f"Rainfall forecast moved to {c:.2f} mm from a prior average of {p:.2f} mm "
        f"({d:.2f} mm change)."
    ),
    why_it_matters=(
        "If dryness persists in key coffee-growing regions, coffee supply concerns can rise "
        "before it becomes a mainstream weather story."
    ),
    affected_market="Coffee futures, coffee-linked ETFs, coffee-sensitive equities",
    best_vehicle="Coffee futures / JO ETF",
    proxy_equities="SBUX, coffee exporters",
    what_to_watch_next="Another 1-2 drier ECMWF runs or media coverage about Brazilian coffee dryness.",
)

# 2. West Africa cocoa
add_signal(
    region="West Africa Cocoa Belt",
    weather_event="Rainfall slipping in cocoa belt",
    metric_name="west_africa_cocoa_precip_mm",
    change_rule=lambda c, p, d: d <= -2.0,
    outcome_rule=lambda c, p, d: c <= 5.0,
    what_changed_builder=lambda c, p, d: (
        f"Rainfall forecast moved to {c:.2f} mm from a prior average of {p:.2f} mm "
        f"({d:.2f} mm change)."
    ),
    why_it_matters=(
        "Ivory Coast and Ghana are critical for cocoa supply. A drier turn can matter early for cocoa pricing."
    ),
    affected_market="Cocoa futures, chocolate-input names",
    best_vehicle="Cocoa futures",
    proxy_equities="Chocolate producers",
    what_to_watch_next="Persistent rainfall weakness across the next few ECMWF runs.",
)

# 3. Panama Canal
add_signal(
    region="Panama Canal",
    weather_event="Rainfall deficit risk",
    metric_name="panama_canal_precip_mm",
    change_rule=lambda c, p, d: d <= -1.5,
    outcome_rule=lambda c, p, d: c <= 3.0,
    what_changed_builder=lambda c, p, d: (
        f"Canal-basin rainfall forecast moved to {c:.2f} mm from a prior average of {p:.2f} mm "
        f"({d:.2f} mm change)."
    ),
    why_it_matters=(
        "If canal rainfall weakens, water levels and ship throughput can become a logistics story before it is widely covered."
    ),
    affected_market="Shipping, freight-sensitive equities, routing-sensitive trade flows",
    best_vehicle="Shipping exposure / freight-sensitive names",
    proxy_equities="ZIM, shipping companies",
    what_to_watch_next="Further dry revisions or headlines about canal draft restrictions.",
)

# 4. Northwest Europe cold
add_signal(
    region="Northwest Europe",
    weather_event="Cold deepening",
    metric_name="nw_europe_mean_temp_c",
    change_rule=lambda c, p, d: d <= -1.0,
    outcome_rule=lambda c, p, d: c <= 4.0,
    what_changed_builder=lambda c, p, d: (
        f"Temperature forecast moved to {c:.2f}°C from a prior average of {p:.2f}°C "
        f"({d:.2f}°C change)."
    ),
    why_it_matters=(
        "A colder turn in Northwest Europe can raise heating demand and energy sensitivity before mainstream coverage catches up."
    ),
    affected_market="European gas, power, energy-sensitive equities",
    best_vehicle="Natural gas / power exposure",
    proxy_equities="LNG, SHEL, BP",
    what_to_watch_next="Another colder run and broader press focus on European cold demand.",
)

# 5. US Corn Belt
add_signal(
    region="US Corn Belt",
    weather_event="Hot/dry stress building",
    metric_name="cornbelt_hotdry_score",
    change_rule=lambda c, p, d: d >= 1.5,
    outcome_rule=lambda c, p, d: c >= 6.0,
    what_changed_builder=lambda c, p, d: (
        f"Hot/dry score moved to {c:.2f} from a prior average of {p:.2f} "
        f"({d:.2f} change)."
    ),
    why_it_matters=(
        "If corn-belt heat and dryness keep building, crop stress can become a real supply story for grains."
    ),
    affected_market="Corn, fertilizers, agribusiness",
    best_vehicle="Corn futures / CORN ETF",
    proxy_equities="ADM, fertilizer companies",
    what_to_watch_next="Repeated hotter/drier runs and any crop-stress headlines.",
)

# 6. US Wheat Plains
add_signal(
    region="US Wheat Plains",
    weather_event="Wheat weather stress rising",
    metric_name="us_wheat_hotdry_score",
    change_rule=lambda c, p, d: d >= 1.5,
    outcome_rule=lambda c, p, d: c >= 6.0,
    what_changed_builder=lambda c, p, d: (
        f"Hot/dry score moved to {c:.2f} from a prior average of {p:.2f} "
        f"({d:.2f} change)."
    ),
    why_it_matters=(
        "Wheat supply expectations can tighten if Plains stress persists and starts appearing in agricultural headlines."
    ),
    affected_market="Wheat, agribusiness",
    best_vehicle="Wheat futures / WEAT",
    proxy_equities="ADM, BG",
    what_to_watch_next="Another hotter/drier forecast turn and crop-condition concern in media.",
)

# 7. Argentina soy
add_signal(
    region="Argentina Pampas",
    weather_event="Soy weather stress rising",
    metric_name="argentina_soy_hotdry_score",
    change_rule=lambda c, p, d: d >= 1.5,
    outcome_rule=lambda c, p, d: c >= 6.0,
    what_changed_builder=lambda c, p, d: (
        f"Hot/dry score moved to {c:.2f} from a prior average of {p:.2f} "
        f"({d:.2f} change)."
    ),
    why_it_matters=(
        "Argentina matters for soy supply. A hotter/drier shift can become a real market story if it persists."
    ),
    affected_market="Soybeans, agribusiness",
    best_vehicle="Soybean futures / SOYB",
    proxy_equities="ADM, BG",
    what_to_watch_next="More hot/dry confirmation and headlines about Argentine crop stress.",
)

# 8. Canadian Prairies
add_signal(
    region="Canadian Prairies",
    weather_event="Prairie crop stress rising",
    metric_name="canadian_prairies_hotdry_score",
    change_rule=lambda c, p, d: d >= 1.5,
    outcome_rule=lambda c, p, d: c >= 6.0,
    what_changed_builder=lambda c, p, d: (
        f"Hot/dry score moved to {c:.2f} from a prior average of {p:.2f} "
        f"({d:.2f} change)."
    ),
    why_it_matters=(
        "Canadian prairies matter for grains and fertilizers. Weather stress there can turn into a supply story."
    ),
    affected_market="Wheat, canola, fertilizers",
    best_vehicle="Wheat / canola theme",
    proxy_equities="NTR, MOS, CF, ADM, BG",
    what_to_watch_next="Repeated hot/dry runs and crop-stress reporting.",
)

# 9. Mato Grosso
add_signal(
    region="Mato Grosso",
    weather_event="Brazil soy/corn stress rising",
    metric_name="mato_grosso_hotdry_score",
    change_rule=lambda c, p, d: d >= 1.5,
    outcome_rule=lambda c, p, d: c >= 6.0,
    what_changed_builder=lambda c, p, d: (
        f"Hot/dry score moved to {c:.2f} from a prior average of {p:.2f} "
        f"({d:.2f} change)."
    ),
    why_it_matters=(
        "Mato Grosso is a key agricultural region. Stress there can matter for soy and corn expectations."
    ),
    affected_market="Soybeans, corn, fertilizers",
    best_vehicle="Soybean / corn exposure",
    proxy_equities="ADM, BG, fertilizer companies",
    what_to_watch_next="More dry revisions and Brazil crop-stress headlines.",
)

# 10. SE Asia palm oil
add_signal(
    region="SE Asia Palm Oil Belt",
    weather_event="Palm oil weather stress rising",
    metric_name="sea_palm_oil_hotdry_score",
    change_rule=lambda c, p, d: d >= 1.5,
    outcome_rule=lambda c, p, d: c >= 8.0,
    what_changed_builder=lambda c, p, d: (
        f"Hot/dry score moved to {c:.2f} from a prior average of {p:.2f} "
        f"({d:.2f} change)."
    ),
    why_it_matters=(
        "Palm oil and food-input supply can tighten if Southeast Asia keeps getting hotter and drier."
    ),
    affected_market="Palm oil, food inputs",
    best_vehicle="Palm oil / food-input theme",
    proxy_equities="ADM, MDLZ, GIS",
    what_to_watch_next="Further persistence and any mainstream discussion of palm oil stress.",
)

# 11. Gulf of Mexico storms
add_signal(
    region="Gulf of Mexico",
    weather_event="Storm risk intensifying",
    metric_name="gulf_storm_index",
    change_rule=lambda c, p, d: d >= 8.0,
    outcome_rule=lambda c, p, d: c >= 25.0,
    what_changed_builder=lambda c, p, d: (
        f"Storm index moved to {c:.2f} from a prior average of {p:.2f} "
        f"({d:.2f} change)."
    ),
    why_it_matters=(
        "A rising Gulf storm signal can matter early for offshore energy, insurers and travel-sensitive names."
    ),
    affected_market="Offshore energy, insurance, travel",
    best_vehicle="Energy / insurance basket",
    proxy_equities="SLB, HAL, XOM, CVX, TRV, ALL, HIG",
    what_to_watch_next="Another stronger run or mainstream storm coverage.",
)

# 12. US East Coast storms
add_signal(
    region="US East Coast",
    weather_event="Storm risk intensifying",
    metric_name="us_east_coast_storm_index",
    change_rule=lambda c, p, d: d >= 8.0,
    outcome_rule=lambda c, p, d: c >= 25.0,
    what_changed_builder=lambda c, p, d: (
        f"Storm index moved to {c:.2f} from a prior average of {p:.2f} "
        f"({d:.2f} change)."
    ),
    why_it_matters=(
        "East Coast storm intensification can affect insurers, utilities, airlines and coastal infrastructure."
    ),
    affected_market="Insurance, utilities, airlines",
    best_vehicle="Insurance / utilities basket",
    proxy_equities="DUK, SO, TRV, ALL, HIG, UAL",
    what_to_watch_next="Further storm strengthening and widespread media attention.",
)

# 13. North Sea storms
add_signal(
    region="North Sea",
    weather_event="Storm corridor intensifying",
    metric_name="north_sea_storm_index",
    change_rule=lambda c, p, d: d >= 8.0,
    outcome_rule=lambda c, p, d: c >= 25.0,
    what_changed_builder=lambda c, p, d: (
        f"Storm index moved to {c:.2f} from a prior average of {p:.2f} "
        f"({d:.2f} change)."
    ),
    why_it_matters=(
        "North Sea storms can matter for offshore production, shipping and European energy sensitivity."
    ),
    affected_market="Shipping, offshore energy, insurers",
    best_vehicle="Shipping / energy basket",
    proxy_equities="SHEL, BP, STNG, TK, HIG",
    what_to_watch_next="Another stronger run or mainstream North Sea storm headlines.",
)

signals_df = pd.DataFrame(signals)

if not signals_df.empty:
    level_rank = {
        "HIGH CONVICTION": 0,
        "ACTIONABLE": 1,
        "EARLY SIGNAL": 2,
    }
    signals_df["level_rank"] = signals_df["signal_level"].map(level_rank)
    signals_df = signals_df.sort_values(
        by=["level_rank", "region"],
        ascending=[True, True]
    ).drop(columns=["level_rank"])

with psycopg.connect(DATABASE_URL) as conn:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS weather_signals")

        cur.execute("""
            CREATE TABLE weather_signals (
                id BIGSERIAL PRIMARY KEY,
                region TEXT NOT NULL,
                weather_event TEXT NOT NULL,
                signal_level TEXT NOT NULL,
                recommendation TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                current_value DOUBLE PRECISION NOT NULL,
                prior_avg_value DOUBLE PRECISION NOT NULL,
                delta_value DOUBLE PRECISION NOT NULL,
                what_changed TEXT NOT NULL,
                why_it_matters TEXT NOT NULL,
                affected_market TEXT NOT NULL,
                best_vehicle TEXT NOT NULL,
                proxy_equities TEXT NOT NULL,
                what_to_watch_next TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
        """)

        if not signals_df.empty:
            cur.executemany("""
                INSERT INTO weather_signals (
                    region,
                    weather_event,
                    signal_level,
                    recommendation,
                    metric_name,
                    current_value,
                    prior_avg_value,
                    delta_value,
                    what_changed,
                    why_it_matters,
                    affected_market,
                    best_vehicle,
                    proxy_equities,
                    what_to_watch_next,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, [
                (
                    r["region"],
                    r["weather_event"],
                    r["signal_level"],
                    r["recommendation"],
                    r["metric_name"],
                    float(r["current_value"]),
                    float(r["prior_avg_value"]),
                    float(r["delta_value"]),
                    r["what_changed"],
                    r["why_it_matters"],
                    r["affected_market"],
                    r["best_vehicle"],
                    r["proxy_equities"],
                    r["what_to_watch_next"],
                    r["updated_at"],
                )
                for _, r in signals_df.iterrows()
            ])

        conn.commit()

print("weather_signals table updated in Postgres")
if signals_df.empty:
    print("No relevant weather signals this run")
else:
    print(signals_df[[
        "region",
        "weather_event",
        "signal_level",
        "recommendation"
    ]].to_string(index=False))
