import sys
import json
import tomllib
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


def weight_posts(posts: list[dict], weights: dict) -> list[dict]:
    reply_weight = weights.get("reply", 0)
    retweet_weight = weights.get("retweet", 0)
    quote_weight = weights.get("quote", 0)

    results = []

    for post in posts:
        post_id = post.get("id")

        num_replies = len(post.get("replies", []))
        num_retweets = len(post.get("retweeters", []))
        num_quotes = len(post.get("quotes", []))

        total_weight = (num_replies * reply_weight +
                        num_retweets * retweet_weight +
                        num_quotes * quote_weight)

        results.append({
            "post_id": post_id,
            "weight": total_weight
        })

    return results


def get_topic(config: dict, topic_name: str) -> dict | None:
    for topic in config.get("topics", []):
        if topic.get("name") == topic_name:
            return topic
    return None


def main():
    if len(sys.argv) < 2:
        print("Usage: python weight_posts.py <topic_name>")
        print("Example: python weight_posts.py btc_price_prediction")
        sys.exit(1)

    topic_name = sys.argv[1]

    config = load_config()

    topic = get_topic(config, topic_name)
    if not topic:
        print(f"Topic '{topic_name}' not found in config.toml")
        sys.exit(1)

    weights = config.get("trust_weights", {})

    raw_dir = Path(__file__).parent / "raw"

    print("Loading posts...")
    posts = load_posts(raw_dir, topic_name)
    print(f"Loaded {len(posts)} posts")

    print("Loading relevancy scores...")
    relevancy_map = load_relevancy(raw_dir, topic_name)
    print(f"Loaded {len(relevancy_map)} relevancy scores")

    relevant_posts = [p for p in posts if relevancy_map.get(p.get("id"), 0) > 0.5]
    print(f"Posts with score > 0.5: {len(relevant_posts)}")

    print("Weighting posts...")
    weighted = weight_posts(relevant_posts, weights)

    weighted.sort(key=lambda x: x["weight"], reverse=True)

    output_path = raw_dir / f"{topic_name}_weighted.json"

    with open(output_path, "w") as f:
        json.dump(weighted, f, indent=2)

    print(f"Saved {len(weighted)} weighted posts to {output_path}")


if __name__ == "__main__":
    main()
