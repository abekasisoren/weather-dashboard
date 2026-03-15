import os
import psycopg
import pandas as pd
from datetime import datetime, UTC

DATABASE_URL = os.environ.get("DATABASE_URL")


def reset_schema(conn):
    """
    Reset the table to the correct structure.
    This avoids endless schema mismatch problems.
    """

    with conn.cursor() as cur:

        cur.execute("""
        DROP TABLE IF EXISTS weather_global_shocks;
        """)

        cur.execute("""
        CREATE TABLE weather_global_shocks (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            region TEXT,
            commodity TEXT,
            anomaly_type TEXT,
            anomaly_value FLOAT,
            persistence_score INTEGER,
            severity_score INTEGER,
            market_score INTEGER,
            signal_level INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        conn.commit()


def calculate_signal_level(row):

    persistence = row["persistence_score"]
    severity = row["severity_score"]
    market = row["market_score"]

    total = persistence + severity + market

    if total >= 14:
        return 10
    elif total >= 11:
        return 8
    elif total >= 8:
        return 6
    elif total >= 5:
        return 4
    else:
        return 2


def generate_shocks():

    data = [
        {
            "timestamp": datetime.now(UTC),
            "region": "US Midwest",
            "commodity": "Corn",
            "anomaly_type": "Heatwave",
            "anomaly_value": 4.2,
            "persistence_score": 5,
            "severity_score": 4,
            "market_score": 3
        },
        {
            "timestamp": datetime.now(UTC),
            "region": "Brazil",
            "commodity": "Soybeans",
            "anomaly_type": "Drought",
            "anomaly_value": 3.1,
            "persistence_score": 4,
            "severity_score": 5,
            "market_score": 4
        }
    ]

    df = pd.DataFrame(data)

    df["signal_level"] = df.apply(calculate_signal_level, axis=1)

    return df


def insert_shocks(conn, df):

    with conn.cursor() as cur:

        for _, r in df.iterrows():

            cur.execute(
                """
                INSERT INTO weather_global_shocks (
                    timestamp,
                    region,
                    commodity,
                    anomaly_type,
                    anomaly_value,
                    persistence_score,
                    severity_score,
                    market_score,
                    signal_level
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    r["timestamp"],
                    r["region"],
                    r["commodity"],
                    r["anomaly_type"],
                    r["anomaly_value"],
                    r["persistence_score"],
                    r["severity_score"],
                    r["market_score"],
                    r["signal_level"],
                ),
            )

        conn.commit()


def main():

    conn = psycopg.connect(DATABASE_URL)

    print("Resetting schema...")
    reset_schema(conn)

    print("Generating shocks...")
    df = generate_shocks()

    print("Inserting shocks...")
    insert_shocks(conn, df)

    print("Completed successfully.")

    conn.close()


if __name__ == "__main__":
    main()
