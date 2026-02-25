import os
import sys
import json
import tomllib
import csv
import glob
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def load_config():
    config_path = Path(__file__).parent / "config.toml"
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def load_posts(raw_dir: Path, topic_name: str) -> list[dict]:
    posts_path = raw_dir / f"{topic_name}.json"
    if not posts_path.exists():
        print(f"Posts file not found: {posts_path}")
        return []
    print(f"Loading {posts_path}...")
    with open(posts_path, "r") as f:
        return json.load(f)


def load_relevancy(raw_dir: Path, topic_name: str) -> dict[str, float]:
    relevancy_path = raw_dir / f"{topic_name}_relevancy.json"
    if not relevancy_path.exists():
        print(f"Relevancy file not found: {relevancy_path}")
        return {}
    print(f"Loading {relevancy_path}...")
    with open(relevancy_path, "r") as f:
        relevancy = json.load(f)
    return {r["post_id"]: r["relevancy_score"] for r in relevancy}


def load_user_scores(scores_dir: Path, category: str) -> dict[str, float]:
    filepath = scores_dir / f"{category}_processed.csv"
    if not filepath.exists():
        print(f"Scores file not found: {filepath}")
        return {}

    scores = {}
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            scores[row["i"]] = float(row["v"])
    return scores


def weight_posts(posts: list[dict], user_scores: dict[str, float], weights: dict) -> list[dict]:
    reply_weight = weights.get("reply", 0)
    retweet_weight = weights.get("retweet", 0)
    quote_weight = weights.get("quote", 0)
    post_weight = weights.get("post", 0)

    results = []
    users_without_score = set()
    users_with_score = set()

    for post in posts:
        post_id = post.get("id")
        total_weight = 0.0

        author = post.get("author", {})
        author_id = author.get("id")
        if author_id:
            if author_id not in user_scores:
                users_without_score.add(author_id)
            else:
                users_with_score.add(author_id)
            total_weight += user_scores.get(author_id, 0.1) * post_weight

        for reply in post.get("replies", []):
            author = reply.get("author", {})
            user_id = author.get("id")
            if user_id:
                if user_id not in user_scores:
                    users_without_score.add(user_id)
                else:
                    users_with_score.add(user_id)
                total_weight += user_scores.get(user_id, 0.1) * reply_weight

        for retweeter in post.get("retweeters", []):
            user_id = retweeter.get("id")
            if user_id:
                if user_id not in user_scores:
                    users_without_score.add(user_id)
                else:
                    users_with_score.add(user_id)
                total_weight += user_scores.get(user_id, 0.1) * retweet_weight

        for quote in post.get("quotes", []):
            author = quote.get("author", {})
            user_id = author.get("id")
            if user_id:
                if user_id not in user_scores:
                    users_without_score.add(user_id)
                else:
                    users_with_score.add(user_id)
                total_weight += user_scores.get(user_id, 0.1) * quote_weight

        results.append({
            "post_id": post_id,
            "weight": total_weight
        })

    print(f"Users with eigentrust score: {len(users_with_score)}")
    print(f"Users without eigentrust score: {len(users_without_score)}")
    return results


def main():
    if len(sys.argv) < 3:
        print("Usage: python weight_posts.py <topic_name> <category>")
        print("Example: python weight_posts.py btc_price_prediction crypto")
        sys.exit(1)

    topic_name = sys.argv[1]
    category = sys.argv[2]

    config = load_config()
    weights = config.get("trust_weights", {})

    raw_dir = Path(__file__).parent / "raw"
    scores_dir = Path(__file__).parent / "scores"

    print("Loading posts...")
    posts = load_posts(raw_dir, topic_name)
    print(f"Loaded {len(posts)} posts")

    print("Loading relevancy scores...")
    relevancy_map = load_relevancy(raw_dir, topic_name)
    print(f"Loaded {len(relevancy_map)} relevancy scores")

    relevant_posts = [p for p in posts if relevancy_map.get(p.get("id"), 0) > 0.5]
    print(f"Posts with score > 0.5: {len(relevant_posts)}")

    print(f"Loading user scores for {category}...")
    user_scores = load_user_scores(scores_dir, category)
    print(f"Loaded {len(user_scores)} user scores")

    print("Weighting posts...")
    weighted = weight_posts(relevant_posts, user_scores, weights)

    weighted.sort(key=lambda x: x["weight"], reverse=True)

    output_path = raw_dir / f"{topic_name}_weighted.json"

    with open(output_path, "w") as f:
        json.dump(weighted, f, indent=2)

    print(f"Saved {len(weighted)} weighted posts to {output_path}")


if __name__ == "__main__":
    main()
