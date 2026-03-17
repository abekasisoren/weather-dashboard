"""
recommendations_tracker.py — Log and track Global Pulse Trader recommendations.

Stores each recommendation to the `recommendations_log` DB table with an entry
price fetched at logging time, then computes P&L using current prices (yfinance).

Usage:
    from recommendations_tracker import ensure_schema, log_recommendations, get_aftermath_table
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL")


# ─── Schema ───────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS recommendations_log (
    id              SERIAL PRIMARY KEY,
    logged_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    signal_date     TEXT,
    stock_symbol    TEXT NOT NULL,
    trade           TEXT NOT NULL,
    entry_price     DOUBLE PRECISION,
    region          TEXT,
    anomaly         TEXT,
    commodity       TEXT,
    final_trade_score DOUBLE PRECISION,
    conviction      TEXT,
    why_it_matters  TEXT
);
"""


def ensure_schema() -> None:
    """Create recommendations_log table if it does not exist."""
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()


# ─── Price fetching ───────────────────────────────────────────────────────────

def fetch_prices(symbols: list[str]) -> dict[str, Optional[float]]:
    """
    Fetch latest close prices for a list of symbols via yfinance.
    Uses individual Ticker.history() calls — most reliable method on all platforms.
    Returns {symbol: price} — price is None if fetch fails.
    """
    prices: dict[str, Optional[float]] = {s: None for s in symbols}
    if not symbols:
        return prices
    try:
        import yfinance as yf
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period="2d", auto_adjust=True)
                if not hist.empty:
                    price = float(hist["Close"].iloc[-1])
                    prices[symbol] = round(price, 4)
            except Exception:
                pass
    except Exception:
        pass
    return prices


# ─── Log recommendations ──────────────────────────────────────────────────────

def log_recommendations(pulse_df: pd.DataFrame) -> int:
    """
    Log current pulse trader recommendations to recommendations_log.
    Only logs symbols not already logged today (idempotent per day).
    Returns number of rows inserted.
    """
    if pulse_df.empty:
        return 0

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Fetch entry prices for all symbols
    symbols = pulse_df["Stock Trade"].dropna().unique().tolist()
    prices = fetch_prices(symbols)

    # Check which symbols already logged today
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT stock_symbol FROM recommendations_log WHERE signal_date = %s",
                (today,),
            )
            already_logged = {row[0] for row in cur.fetchall()}

    rows_to_insert = []
    for _, row in pulse_df.iterrows():
        symbol = str(row.get("Stock Trade", "")).strip()
        trade = str(row.get("Trade", "")).strip()
        if not symbol or trade == "No Trade" or symbol in already_logged:
            continue

        rows_to_insert.append({
            "signal_date": row.get("Date", today),
            "stock_symbol": symbol,
            "trade": trade,
            "entry_price": prices.get(symbol),
            "region": str(row.get("Region", "")),
            "anomaly": str(row.get("Anomaly", "")),
            "commodity": str(row.get("Commodity", "")),
            "final_trade_score": float(row.get("Final Trade Score", 0)),
            "conviction": str(row.get("Conviction", "")),
            "why_it_matters": str(row.get("Why It Matters", ""))[:500],
        })

    if not rows_to_insert:
        return 0

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            for r in rows_to_insert:
                cur.execute(
                    """
                    INSERT INTO recommendations_log
                        (signal_date, stock_symbol, trade, entry_price,
                         region, anomaly, commodity, final_trade_score,
                         conviction, why_it_matters)
                    VALUES (%(signal_date)s, %(stock_symbol)s, %(trade)s,
                            %(entry_price)s, %(region)s, %(anomaly)s,
                            %(commodity)s, %(final_trade_score)s,
                            %(conviction)s, %(why_it_matters)s)
                    """,
                    r,
                )
        conn.commit()

    return len(rows_to_insert)


# ─── Aftermath table ──────────────────────────────────────────────────────────

def get_aftermath_table() -> pd.DataFrame:
    """
    Load all logged recommendations, enrich with current prices, compute P&L.
    Returns a DataFrame ready for display.
    """
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, logged_at, signal_date, stock_symbol, trade,
                       entry_price, region, anomaly, commodity,
                       final_trade_score, conviction, why_it_matters
                FROM recommendations_log
                ORDER BY logged_at DESC
                """
            )
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=cols)

    # Fetch current prices for all unique symbols
    symbols = df["stock_symbol"].dropna().unique().tolist()
    current_prices = fetch_prices(symbols)
    df["current_price"] = df["stock_symbol"].map(current_prices)

    # Backfill missing entry prices: if entry_price is NULL, use current price as proxy
    # and persist to DB so future loads have it
    null_mask = df["entry_price"].isna()
    if null_mask.any():
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                for idx, row in df[null_mask].iterrows():
                    bp = current_prices.get(row["stock_symbol"])
                    if bp is not None:
                        cur.execute(
                            "UPDATE recommendations_log SET entry_price = %s WHERE id = %s",
                            (bp, int(row["id"])),
                        )
                        df.at[idx, "entry_price"] = bp
            conn.commit()

    # Compute P&L
    def compute_pnl(row):
        entry = row.get("entry_price")
        current = row.get("current_price")
        trade = str(row.get("trade", "")).strip()
        if entry is None or current is None or entry == 0:
            return None
        if trade == "Long":
            return round((current - entry) / entry * 100, 2)
        if trade == "Short":
            return round((entry - current) / entry * 100, 2)
        return None

    df["pnl_pct"] = df.apply(compute_pnl, axis=1)

    def outcome(pnl):
        if pnl is None:
            return "—"
        return "✅ Win" if pnl > 0 else "❌ Loss" if pnl < 0 else "➖ Flat"

    df["outcome"] = df["pnl_pct"].apply(outcome)

    # Format for display
    display = pd.DataFrame({
        "Date Logged":    pd.to_datetime(df["logged_at"]).dt.strftime("%Y-%m-%d"),
        "Signal Date":    df["signal_date"],
        "Stock":          df["stock_symbol"],
        "Trade":          df["trade"],
        "Entry Price":    df["entry_price"].apply(lambda x: f"${x:.2f}" if x else "—"),
        "Current Price":  df["current_price"].apply(lambda x: f"${x:.2f}" if x else "—"),
        "P&L %":          df["pnl_pct"].apply(lambda x: f"{x:+.2f}%" if x is not None else "—"),
        "Outcome":        df["outcome"],
        "Score":          df["final_trade_score"].round(2),
        "Conviction":     df["conviction"],
        "Region":         df["region"],
        "Anomaly":        df["anomaly"],
        "Why":            df["why_it_matters"],
        "_pnl_raw":       df["pnl_pct"],   # for sorting / stats (hidden)
    })

    return display


def get_performance_summary(aftermath_df: pd.DataFrame) -> dict:
    """Compute win rate, avg P&L, best/worst trade from aftermath table."""
    if aftermath_df.empty:
        return {}

    pnl_series = aftermath_df["_pnl_raw"].dropna()
    if pnl_series.empty:
        return {}

    wins = (pnl_series > 0).sum()
    losses = (pnl_series < 0).sum()
    total = len(pnl_series)

    best_idx = pnl_series.idxmax()
    worst_idx = pnl_series.idxmin()

    return {
        "total": total,
        "wins": int(wins),
        "losses": int(losses),
        "win_rate": round(wins / total * 100, 1) if total else 0,
        "avg_pnl": round(float(pnl_series.mean()), 2),
        "best_trade": f"{aftermath_df.loc[best_idx, 'Stock']} {pnl_series[best_idx]:+.2f}%",
        "worst_trade": f"{aftermath_df.loc[worst_idx, 'Stock']} {pnl_series[worst_idx]:+.2f}%",
    }
