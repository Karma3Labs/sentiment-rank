import os
import re
import json
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))


def parse_twitter_date(date_str):
    return datetime.strptime(date_str, "%a %b %d %H:%M:%S %z %Y")


def format_predictions(probs, market_ids):
    if not probs or not market_ids:
        return None
    preds = [f'"({mid},{prob})"' for mid, prob in zip(market_ids, probs)]
    return "{" + ",".join(preds) + "}"

def parse_market_ids(markets_str):
    matches = re.findall(r'\((\d+),', markets_str)
    return [int(m) for m in matches]


def main():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT event_id, name, markets FROM twitter_sentiment.events")
    events = {}
    event_markets = {}
    for event_id, name, markets in cur.fetchall():
        events[name] = event_id
        event_markets[event_id] = parse_market_ids(markets)

    cur.execute("SELECT COALESCE(MAX(run_id), 0) FROM twitter_sentiment.posts")
    run_id = cur.fetchone()[0] + 1
    print(f"Using global run_id {run_id}")

    for topic_name, event_id in events.items():
        base_path = f"raw/{topic_name}"

        if not os.path.exists(f"{base_path}.json"):
            print(f"Skipping {topic_name}: no data file")
            continue

        with open(f"{base_path}.json") as f:
            posts = json.load(f)

        market_ids = event_markets.get(event_id, [])
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
        skipped = 0
        for post in posts:
            post_id = post["id"]
            author = post.get("author") or {}

            if not author.get("id"):
                skipped += 1
                continue

            cur.execute(
                """
                INSERT INTO twitter_sentiment.posts (
                    event_id, run_id, post_id, text, posted_at,
                    like_count, retweet_count, reply_count, quote_count,
                    author_id, author_username, author_name, author_followers, author_is_verified,
                    relevancy_score, weight, predictions
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::twitter_sentiment.prediction[])
                ON CONFLICT (event_id, run_id, post_id) DO UPDATE SET
                    relevancy_score = EXCLUDED.relevancy_score,
                    weight = EXCLUDED.weight,
                    predictions = EXCLUDED.predictions
                """,
                (
                    event_id,
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
                    format_predictions(predictions.get(post_id), market_ids),
                ),
            )
            count += 1

        print(f"Inserted {count} posts for {topic_name} (skipped {skipped} with missing author)")

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
