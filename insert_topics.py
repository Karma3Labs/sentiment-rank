import os
import tomllib
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_outcomes_from_markets(topic: dict) -> list[str]:
    markets = topic.get("markets", [])
    return [m["name"] for m in markets]


def get_market_ids_from_markets(topic: dict) -> list[int]:
    markets = topic.get("markets", [])
    return [m["id"] for m in markets]


def main():
    conn = psycopg2.connect(
        host="localhost",
        port=os.getenv("SQL_PORT"),
        user=os.getenv("SQL_USERNAME"),
        password=os.getenv("SQL_PASSWORD"),
        database=os.getenv("SQL_DB"),
    )
    cur = conn.cursor()

    with open("config.toml", "rb") as f:
        config = tomllib.load(f)

    for topic in config["topics"]:
        outcomes = get_outcomes_from_markets(topic)
        market_ids = get_market_ids_from_markets(topic)
        cur.execute(
            """
            INSERT INTO events (event_id, name, description, outcomes, query, hashtags, category, market_ids)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                outcomes = EXCLUDED.outcomes,
                query = EXCLUDED.query,
                hashtags = EXCLUDED.hashtags,
                category = EXCLUDED.category,
                market_ids = EXCLUDED.market_ids,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                topic["event_id"],
                topic["name"],
                topic["description"],
                outcomes,
                topic["query"],
                topic.get("hashtags"),
                topic["category"],
                market_ids,
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {len(config['topics'])} events")

if __name__ == "__main__":
    main()
