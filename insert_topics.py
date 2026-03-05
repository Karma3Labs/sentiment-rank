import os
import tomllib
import psycopg2
from dotenv import load_dotenv

load_dotenv()

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
        cur.execute(
            """
            INSERT INTO topics (name, description, outcomes, query, hashtags, category)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET
                description = EXCLUDED.description,
                outcomes = EXCLUDED.outcomes,
                query = EXCLUDED.query,
                hashtags = EXCLUDED.hashtags,
                category = EXCLUDED.category,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                topic["name"],
                topic["description"],
                topic["outcome"],
                topic["query"],
                topic.get("hashtags"),
                topic["category"],
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {len(config['topics'])} topics")

if __name__ == "__main__":
    main()
