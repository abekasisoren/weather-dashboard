import pandas as pd

weather = pd.read_csv("weather_values.csv")

signals = []

def score_signal(severity, change, market):
    score = (0.35*severity + 0.35*change + 0.30*market)
    return round(min(10, score),2)

# Example signals based on weather metrics

for _, row in weather.iterrows():

    metric = row["metric"]
    value = row["value"]

    if metric == "brazil_coffee_precip_mm":

        severity = max(0, 20 - value)
        change = 7
        market = 9

        score = score_signal(severity, change, market)

        signals.append({
            "Region": "Brazil Coffee Belt",
            "WeatherEvent": "Rainfall falling",
            "Score": score,
            "Recommendation": "TRADE" if score >= 8 else "WATCH",
            "WeatherLogic":
                "Rainfall forecast dropping across ECMWF runs in key coffee regions.",
            "MarketLogic":
                "Lower rainfall risks tightening coffee supply expectations.",
            "BestVehicle": "Coffee futures / JO ETF",
            "ProxyEquities": "SBUX, coffee exporters"
        })

    if metric == "panama_canal_precip_mm":

        severity = max(0, 10 - value)
        change = 6
        market = 8

        score = score_signal(severity, change, market)

        signals.append({
            "Region": "Panama Canal",
            "WeatherEvent": "Rainfall falling",
            "Score": score,
            "Recommendation": "TRADE" if score >= 8 else "WATCH",
            "WeatherLogic":
                "Lower rainfall threatens canal water levels and ship throughput.",
            "MarketLogic":
                "Shipping congestion can increase freight rates.",
            "BestVehicle": "Shipping ETFs / freight rate exposure",
            "ProxyEquities": "ZIM, container shipping firms"
        })

signals_df = pd.DataFrame(signals)

signals_df = signals_df.sort_values(by="Score", ascending=False)

signals_df.to_csv("signals.csv", index=False)

print("signals.csv generated with scoring system")
