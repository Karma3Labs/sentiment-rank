import os
import json
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def parse_twitter_date(date_str):
    return datetime.strptime(date_str, "%a %b %d %H:%M:%S %z %Y")

def main():
    conn = psycopg2.connect(
        host="localhost",
        port=os.getenv("SQL_PORT"),
        user=os.getenv("SQL_USERNAME"),
        password=os.getenv("SQL_PASSWORD"),
        database=os.getenv("SQL_DB"),
    )
    cur = conn.cursor()

    cur.execute("SELECT id, name FROM topics")
    topics = {name: id for id, name in cur.fetchall()}

    run_id = 1

    for topic_name, topic_id in topics.items():
        base_path = f"raw/{topic_name}"

        if not os.path.exists(f"{base_path}.json"):
            print(f"Skipping {topic_name}: no data file")
            continue

        with open(f"{base_path}.json") as f:
            posts = json.load(f)

        predictions = {}
        if os.path.exists(f"{base_path}_prediction.json"):
            with open(f"{base_path}_prediction.json") as f:
                for p in json.load(f):
                    predictions[p["post_id"]] = p["probabilities"]

        relevancy = {}
        if os.path.exists(f"{base_path}_relevancy.json"):
            with open(f"{base_path}_relevancy.json") as f:
                for r in json.load(f):
                    relevancy[r["post_id"]] = r["relevancy_score"]

        weights = {}
        if os.path.exists(f"{base_path}_weighted.json"):
            with open(f"{base_path}_weighted.json") as f:
                for w in json.load(f):
                    weights[w["post_id"]] = w["weight"]

        count = 0
        for post in posts:
            post_id = post["id"]
            author = post["author"]

            cur.execute(
                """
                INSERT INTO posts (
                    topic_id, run_id, post_id, text, posted_at,
                    like_count, retweet_count, reply_count, quote_count,
                    author_id, author_username, author_name, author_followers, author_is_verified,
                    relevancy_score, weight, probabilities
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (topic_id, run_id, post_id) DO UPDATE SET
                    relevancy_score = EXCLUDED.relevancy_score,
                    weight = EXCLUDED.weight,
                    probabilities = EXCLUDED.probabilities
                """,
                (
                    topic_id,
                    run_id,
                    post_id,
                    post["text"],
                    parse_twitter_date(post["createdAt"]),
                    post["likeCount"],
                    post["retweetCount"],
                    post["replyCount"],
                    post["quoteCount"],
                    author["id"],
                    author["userName"],
                    author["name"],
                    author["followers"],
                    author.get("isBlueVerified", False),
                    relevancy.get(post_id),
                    weights.get(post_id),
                    predictions.get(post_id),
                ),
            )
            count += 1

        print(f"Inserted {count} posts for {topic_name}")

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
