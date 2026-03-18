import os
import json
import glob
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))


def main():
    conn = get_db_connection()
    cur = conn.cursor()

    topic_files = glob.glob("raw/*_topics.json")
    total = 0

    for topic_file in topic_files:
        category = os.path.basename(topic_file).replace("_topics.json", "")

        with open(topic_file, "r") as f:
            topics = json.load(f)

        for topic in topics:
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
                    topic["slug"],
                    topic["description"],
                    markets_literal,
                    topic["query"],
                    topic.get("hashtags"),
                    category,
                ),
            )
            total += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {total} events")

if __name__ == "__main__":
    main()
