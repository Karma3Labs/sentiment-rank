import os
import sys
import json
import tomllib
import requests
import time
import glob
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


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


def build_prompt(post: dict, topic: dict) -> str:
    topic_description = topic.get("description", "")
    outcomes = get_outcomes_from_markets(topic)
    outcomes_str = ", ".join(f'"{o}"' for o in outcomes)

    return f"""You are analyzing a social media post to predict which outcome it supports.

Question: {topic_description}
Possible outcomes: [{outcomes_str}]

Post text: {post.get("text", "")}

Based on this post, estimate the probability distribution across the possible outcomes.
The probabilities must sum to 1.0.

Respond with ONLY a JSON array of numbers representing the probability for each outcome in order.
For example, if outcomes are ["yes", "no"] and the post strongly suggests "yes", respond: [0.8, 0.2]

Response:"""


def predict_openai(post: dict, topic: dict, api_key: str) -> list[float] | None:
    prompt = build_prompt(post, topic)
    outcomes = get_outcomes_from_markets(topic)

    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 200,
                "temperature": 0
            },
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        text = result["choices"][0]["message"]["content"].strip()
        try:
            probs = json.loads(text)
        except json.JSONDecodeError:
            print(f"OpenAI returned invalid JSON for post {post.get('id')}: {text}")
            return None
        if len(probs) == len(outcomes) and abs(sum(probs) - 1.0) < 0.01:
            return probs
        return None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"OpenAI rate limited, waiting 60s...")
            time.sleep(60)
            return predict_openai(post, topic, api_key)
        print(f"OpenAI error for post {post.get('id')}: {e}")
        return None
    except Exception as e:
        print(f"OpenAI error for post {post.get('id')}: {e}")
        return None


def predict_claude(post: dict, topic: dict, api_key: str) -> list[float] | None:
    prompt = build_prompt(post, topic)
    outcomes = get_outcomes_from_markets(topic)

    try:
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json"
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 200,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        text = result["content"][0]["text"].strip()
        try:
            probs = json.loads(text)
        except json.JSONDecodeError:
            print(f"Claude returned invalid JSON for post {post.get('id')}: {text}")
            return None
        if len(probs) == len(outcomes) and abs(sum(probs) - 1.0) < 0.01:
            return probs
        return None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"Claude rate limited, waiting 60s...")
            time.sleep(60)
            return predict_claude(post, topic, api_key)
        print(f"Claude error for post {post.get('id')}: {e}")
        return None
    except Exception as e:
        print(f"Claude error for post {post.get('id')}: {e}")
        return None


def predict_sentiment(post: dict, topic: dict, openai_key: str, claude_key: str) -> list[float] | None:
    openai_probs = predict_openai(post, topic, openai_key)
    claude_probs = predict_claude(post, topic, claude_key)

    if openai_probs and claude_probs:
        return [(o + c) / 2 for o, c in zip(openai_probs, claude_probs)]
    elif openai_probs:
        return openai_probs
    elif claude_probs:
        return claude_probs
    return None


def main():
    openai_key = os.environ.get("OPENAI_API_KEY")
    if not openai_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")

    claude_key = os.environ.get("CLAUDE_API_KEY")
    if not claude_key:
        raise ValueError("CLAUDE_API_KEY environment variable not set")

    if len(sys.argv) < 2:
        print("Usage: python predict_sentiment.py <topic_name>")
        print("Example: python predict_sentiment.py btc_price_prediction")
        sys.exit(1)

    topic_name = sys.argv[1]
    config = load_config()
    topic = get_topic_by_name(config, topic_name)
    if not topic:
        print(f"Topic '{topic_name}' not found in config.toml")
        sys.exit(1)

    raw_dir = Path(__file__).parent / "raw"

    posts_path = raw_dir / f"{topic_name}.json"
    if not posts_path.exists():
        print(f"Posts file not found: {posts_path}")
        sys.exit(1)

    relevancy_path = raw_dir / f"{topic_name}_relevancy.json"
    if not relevancy_path.exists():
        print(f"Relevancy file not found: {relevancy_path}")
        sys.exit(1)

    print(f"Loading {posts_path}...")
    with open(posts_path, "r") as f:
        posts = json.load(f)

    print(f"Loading {relevancy_path}...")
    with open(relevancy_path, "r") as f:
        relevancy = json.load(f)

    relevancy_map = {r["post_id"]: r["relevancy_score"] for r in relevancy}
    relevant_posts = [p for p in posts if relevancy_map.get(p.get("id"), 0) > 0.5]
    total = len(relevant_posts)

    start_time = datetime.now()
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Topic: {topic_name}")
    print(f"Description: {topic.get('description')}")
    print(f"Outcomes: {get_outcomes_from_markets(topic)}")
    print(f"Total posts (score > 0.5): {total}")

    predictions = []
    for i, post in enumerate(relevant_posts, 1):
        probs = predict_sentiment(post, topic, openai_key, claude_key)
        if probs:
            predictions.append({
                "post_id": post.get("id"),
                "probabilities": probs
            })
            print(f"[{i}/{total}] Post {post.get('id')}: {probs}")
        else:
            print(f"[{i}/{total}] Post {post.get('id')}: failed")

    output_path = raw_dir / f"{topic_name}_prediction.json"

    with open(output_path, "w") as f:
        json.dump(predictions, f, indent=2)

    elapsed = datetime.now() - start_time
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total elapsed: {elapsed}")
    print(f"Predictions: {len(predictions)}/{total}")
    print(f"Output saved to: {output_path}")


if __name__ == "__main__":
    main()
