import pandas as pd
from pathlib import Path
from datetime import datetime
import yfinance as yf

signals_file = Path("signals.csv")
universe_file = Path("market_universe.csv")
output_file = Path("reaction_log.csv")

if not signals_file.exists():
    raise FileNotFoundError("signals.csv not found")

if not universe_file.exists():
    raise FileNotFoundError("market_universe.csv not found")

signals_df = pd.read_csv(signals_file)
universe_df = pd.read_csv(universe_file)

today = datetime.today().strftime("%Y-%m-%d")

rows = []

for _, signal in signals_df.iterrows():
    for _, asset in universe_df.iterrows():
        ticker = asset["Ticker"]
        name = asset["Name"]
        exchange = asset["Exchange"]

        try:
            hist = yf.download(ticker, period="10d", interval="1d", progress=False)

            if hist.empty:
                continue

            closes = hist["Close"].dropna()

            if len(closes) < 2:
                continue

            entry = float(closes.iloc[-1].iloc[0] if hasattr(closes.iloc[-1], "iloc") else closes.iloc[-1])

            r1 = None
            r3 = None
            r7 = None

            if len(closes) >= 2:
                prev1 = closes.iloc[-2]
                last1 = closes.iloc[-1]
                prev1 = float(prev1.iloc[0] if hasattr(prev1, "iloc") else prev1)
                last1 = float(last1.iloc[0] if hasattr(last1, "iloc") else last1)
                r1 = round((last1 / prev1 - 1) * 100, 2)

            if len(closes) >= 4:
                prev3 = closes.iloc[-4]
                last3 = closes.iloc[-1]
                prev3 = float(prev3.iloc[0] if hasattr(prev3, "iloc") else prev3)
                last3 = float(last3.iloc[0] if hasattr(last3, "iloc") else last3)
                r3 = round((last3 / prev3 - 1) * 100, 2)

            if len(closes) >= 8:
                prev7 = closes.iloc[-8]
                last7 = closes.iloc[-1]
                prev7 = float(prev7.iloc[0] if hasattr(prev7, "iloc") else prev7)
                last7 = float(last7.iloc[0] if hasattr(last7, "iloc") else last7)
                r7 = round((last7 / prev7 - 1) * 100, 2)

            rows.append({
                "Date": today,
                "Region": signal["Region"],
                "WeatherSignal": signal["WeatherSignal"],
                "Market": signal["Market"],
                "Ticker": ticker,
                "Name": name,
                "Exchange": exchange,
                "EntryPrice": entry,
                "Return1D_pct": r1,
                "Return3D_pct": r3,
                "Return7D_pct": r7
            })

        except Exception:
            print("skip", ticker)

df = pd.DataFrame(rows)
df.to_csv("reaction_log.csv", index=False)

print("reaction_log.csv created")
print("rows:", len(df))
