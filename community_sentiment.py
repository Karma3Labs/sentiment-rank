import sys
import json
import tomllib
from pathlib import Path
from datetime import datetime


def load_config():
    config_path = Path(__file__).parent / "config.toml"
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def get_topic_by_name(config: dict, topic_name: str) -> dict | None:
    for topic in config.get("topics", []):
        if topic.get("name") == topic_name:
            return topic
    return None


def get_outcomes_from_markets(topic: dict) -> list[str]:
    markets = topic.get("markets", [])
    return [m["name"] for m in markets]


def load_json(path: Path) -> list:
    if not path.exists():
        print(f"File not found: {path}")
        return []
    with open(path, "r") as f:
        return json.load(f)


def main():
    if len(sys.argv) < 2:
        print("Usage: python community_sentiment.py <topic_name>")
        print("Example: python community_sentiment.py btc_price_prediction")
        sys.exit(1)

    topic_name = sys.argv[1]
    config = load_config()
    topic = get_topic_by_name(config, topic_name)
    if not topic:
        print(f"Topic '{topic_name}' not found in config.toml")
        sys.exit(1)

    outcomes = get_outcomes_from_markets(topic)
    raw_dir = Path(__file__).parent / "raw"

    predictions = load_json(raw_dir / f"{topic_name}_prediction.json")
    relevancy = load_json(raw_dir / f"{topic_name}_relevancy.json")
    weighted = load_json(raw_dir / f"{topic_name}_weighted.json")
    posts = load_json(raw_dir / f"{topic_name}.json")

    relevancy_map = {r["post_id"]: r["relevancy_score"] for r in relevancy}
    prediction_map = {p["post_id"]: p["probabilities"] for p in predictions}
    weight_map = {w["post_id"]: w["weight"] for w in weighted}
    post_map = {p["id"]: p for p in posts}

    aggregated = [0.0] * len(outcomes)
    relevant_posts = []

    for post_id, rel_score in relevancy_map.items():
        if rel_score <= 0.5:
            continue
        if post_id not in prediction_map or post_id not in weight_map:
            continue

        weight = weight_map[post_id]
        probs = prediction_map[post_id]

        for i, prob in enumerate(probs):
            aggregated[i] += weight * prob

        relevant_posts.append({
            "post_id": post_id,
            "weight": weight,
            "probs": probs,
            "relevancy_score": rel_score
        })

    total = sum(aggregated)
    if total > 0:
        normalized = [v / total for v in aggregated]
    else:
        normalized = [0.0] * len(outcomes)

    print("\n=== Community Sentiment ===")
    print(f"Topic: {topic_name}")
    print(f"Description: {topic.get('description')}")
    print()
    for i, outcome in enumerate(outcomes):
        print(f"  {outcome}: {normalized[i]:.2%}")

    relevant_posts.sort(key=lambda x: x["relevancy_score"] * x["weight"], reverse=True)

    print(f"\n=== All Posts (relevancy > 0.5): {len(relevant_posts)} posts ===")
    for i, p in enumerate(relevant_posts, 1):
        post = post_map.get(p["post_id"], {})
        author = post.get("author", {})
        text = post.get("text", "")
        created_at_raw = post.get("createdAt", "")
        try:
            dt = datetime.strptime(created_at_raw, "%a %b %d %H:%M:%S %z %Y")
            created_at = dt.strftime("%b %d, %Y")
        except:
            created_at = "unknown"
        combined_score = p["relevancy_score"] * p["weight"]
        print(f"\n{i}. @{author.get('userName', 'unknown')} - {created_at}")
        print(f"   Relevancy: {p['relevancy_score']:.2f}, Weight: {p['weight']:.2f}, Combined: {combined_score:.2f}")
        sentiment_str = ", ".join(f"{outcomes[j]}: {p['probs'][j]:.0%}" for j in range(len(outcomes)))
        print(f"   Sentiment: {sentiment_str}")
        print("   ----------------------------------------")
        print(f"{text}")
        print("   ----------------------------------------")


if __name__ == "__main__":
    main()
