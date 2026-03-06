import os
import tomllib
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))


def main():
    conn = get_db_connection()
    cur = conn.cursor()

    with open("config.toml", "rb") as f:
        config = tomllib.load(f)

    for topic in config["topics"]:
        markets = topic.get("markets", [])
        markets_array = [f'"({m["id"]},\\"{m["name"]}\\\")"' for m in markets]
        markets_literal = "{" + ",".join(markets_array) + "}"
        cur.execute(
            """
            INSERT INTO twitter_sentiment.events (event_id, name, description, markets, query, hashtags, category)
            VALUES (%s, %s, %s, %s::twitter_sentiment.market[], %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                markets = EXCLUDED.markets,
                query = EXCLUDED.query,
                hashtags = EXCLUDED.hashtags,
                category = EXCLUDED.category,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                topic["event_id"],
                topic["name"],
                topic["description"],
                markets_literal,
                topic["query"],
                topic.get("hashtags"),
                topic["category"],
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {len(config['topics'])} events")

if __name__ == "__main__":
    main()
