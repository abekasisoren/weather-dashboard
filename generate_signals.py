import pandas as pd
import numpy as np

weather = pd.read_csv("weather_values.csv")

signals = []

def normalize(value, low, high):
    return max(0, min(10, 10*(value-low)/(high-low)))

def score_signal(severity, surprise, market, persistence=5):

    score = (
        0.40*surprise +
        0.30*severity +
        0.20*market +
        0.10*persistence
    )

    return round(min(10, score),2)


for _, row in weather.iterrows():

    metric = row["metric"]
    value = row["value"]

    # BRAZIL COFFEE

    if metric == "brazil_coffee_precip_mm":

        severity = normalize(20-value,0,20)
        surprise = normalize(15-value,0,15)
        market = 9

        score = score_signal(severity,surprise,market)

        signals.append({

            "Region":"Brazil Coffee Belt",
            "WeatherEvent":"Rainfall stress building",
            "Score":score,
            "Recommendation":"TRADE" if score>=8 else "WATCH",
            "WeatherLogic":"Rainfall forecasts falling in key Brazilian coffee regions across recent ECMWF runs.",
            "MarketLogic":"Lower rainfall can tighten coffee supply expectations.",
            "BestVehicle":"Coffee Futures / JO ETF",
            "ProxyEquities":"SBUX, coffee exporters"

        })


    # ARGENTINA SOY

    if metric == "argentina_soy_hotdry_score":

        severity = normalize(value,0,10)
        surprise = normalize(value,0,10)
        market = 8

        score = score_signal(severity,surprise,market)

        signals.append({

            "Region":"Argentina Pampas",
            "WeatherEvent":"Hot/Dry crop stress",
            "Score":score,
            "Recommendation":"TRADE" if score>=8 else "WATCH",
            "WeatherLogic":"Hot and dry pattern strengthening across soybean growing regions.",
            "MarketLogic":"Crop stress can tighten global soy supply.",
            "BestVehicle":"Soybean futures",
            "ProxyEquities":"ADM, Bunge"

        })


    # US CORN BELT

    if metric == "cornbelt_hotdry_score":

        severity = normalize(value,0,10)
        surprise = normalize(value,0,10)
        market = 9

        score = score_signal(severity,surprise,market)

        signals.append({

            "Region":"US Corn Belt",
            "WeatherEvent":"Heat and dryness rising",
            "Score":score,
            "Recommendation":"TRADE" if score>=8 else "WATCH",
            "WeatherLogic":"Heat and dryness increasing across the US Corn Belt.",
            "MarketLogic":"Corn yield expectations sensitive to heat and moisture stress.",
            "BestVehicle":"Corn Futures / CORN ETF",
            "ProxyEquities":"ADM, fertilizer companies"

        })


    # WEST AFRICA COCOA

    if metric == "west_africa_cocoa_precip_mm":

        severity = normalize(15-value,0,15)
        surprise = normalize(15-value,0,15)
        market = 8

        score = score_signal(severity,surprise,market)

        signals.append({

            "Region":"West Africa Cocoa Belt",
            "WeatherEvent":"Rainfall risk shifting",
            "Score":score,
            "Recommendation":"TRADE" if score>=8 else "WATCH",
            "WeatherLogic":"Rainfall anomalies emerging across Ivory Coast and Ghana.",
            "MarketLogic":"Cocoa supply sensitive to rainfall shifts during growing periods.",
            "BestVehicle":"Cocoa futures",
            "ProxyEquities":"Chocolate producers"

        })


    # PANAMA CANAL

    if metric == "panama_canal_precip_mm":

        severity = normalize(12-value,0,12)
        surprise = normalize(10-value,0,10)
        market = 8

        score = score_signal(severity,surprise,market)

        signals.append({

            "Region":"Panama Canal",
            "WeatherEvent":"Rainfall deficit",
            "Score":score,
            "Recommendation":"TRADE" if score>=8 else "WATCH",
            "WeatherLogic":"Lower rainfall threatens canal water levels.",
            "MarketLogic":"Reduced shipping throughput can affect freight markets.",
            "BestVehicle":"Shipping exposure",
            "ProxyEquities":"ZIM, shipping companies"

        })


signals_df = pd.DataFrame(signals)

if len(signals_df)>0:

    signals_df = signals_df.sort_values(by="Score",ascending=False)

signals_df.to_csv("signals.csv",index=False)

print("Signals generated with improved scoring system")
