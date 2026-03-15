import os
import psycopg
import pandas as pd
from datetime import datetime

DATABASE_URL = os.environ.get("DATABASE_URL")

def ensure_schema(conn):
    """
    Ensure the required table and columns exist.
    This prevents crashes if the schema changes.
    """

    with conn.cursor() as cur:

        # create table if it doesn't exist
        cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_global_shocks (
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

        # ensure column exists
        cur.execute("""
        ALTER TABLE weather_global_shocks
        ADD COLUMN IF NOT EXISTS signal_level INTEGER;
        """)

        conn.commit()


def calculate_signal_level(row):
    """
    Combines different scoring systems into a final signal level (1-10)
    """

    persistence = row.get("persistence_score", 0)
    severity = row.get("severity_score", 0)
    market = row.get("market_score", 0)

    score = persistence + severity + market

    if score >= 15:
        return 10
    elif score >= 12:
        return 8
    elif score >= 9:
        return 6
    elif score >= 6:
        return 4
    else:
        return 2


def generate_dummy_shocks():
    """
    Temporary generator until full ECMWF integration runs.
    """

    data = [
        {
            "timestamp": datetime.utcnow(),
            "region": "US Midwest",
            "commodity": "Corn",
            "anomaly_type": "heatwave",
            "anomaly_value": 4.2,
            "persistence_score": 5,
            "severity_score": 4,
            "market_score": 3
        },
        {
            "timestamp": datetime.utcnow(),
            "region": "Brazil",
            "commodity": "Soybeans",
            "anomaly_type": "drought",
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

        for _, row in df.iterrows():

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
                    row["timestamp"],
                    row["region"],
                    row["commodity"],
                    row["anomaly_type"],
                    row["anomaly_value"],
                    row["persistence_score"],
                    row["severity_score"],
                    row["market_score"],
                    row["signal_level"],
                ),
            )

        conn.commit()


def main():

    conn = psycopg.connect(DATABASE_URL)

    print("Ensuring schema...")
    ensure_schema(conn)

    print("Generating global weather shocks...")
    df = generate_dummy_shocks()

    print("Inserting shocks into database...")
    insert_shocks(conn, df)

    print("Done.")

    conn.close()


if __name__ == "__main__":
    main()
