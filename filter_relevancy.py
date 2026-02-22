import os
import sys
import json
import tomllib
import base64
import requests
import time
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


def score_relevancy(post: dict, topic: dict, api_key: str) -> float:
    topic_name = topic.get("name", "")
    topic_description = topic.get("description", "")

    prompt = f"""You are evaluating if a social media post is relevant to a specific topic.

Topic: {topic_name}
Topic Description: {topic_description}

Post text: {post.get("text", "")}

Rate the relevancy of this post to the topic on a scale from 0.0 to 1.0:
- 1.0: Highly relevant, directly discusses the topic with specific predictions or analysis
- 0.7-0.9: Relevant, discusses the topic but may lack specific details
- 0.4-0.6: Somewhat relevant, tangentially related to the topic
- 0.1-0.3: Barely relevant, mentions related keywords but not the actual topic
- 0.0: Not relevant at all

Respond with ONLY a single decimal number between 0.0 and 1.0, nothing else."""

    messages = [{"role": "user", "content": []}]

    messages[0]["content"].append({"type": "text", "text": prompt})

    media_urls = post.get("mediaUrls", [])
    for url in media_urls[:4]:
        if url and any(ext in url.lower() for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]):
            messages[0]["content"].append({
                "type": "image_url",
                "image_url": {"url": url}
            })

    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "gpt-4o-mini",
                "messages": messages,
                "max_tokens": 10,
                "temperature": 0
            },
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        score_text = result["choices"][0]["message"]["content"].strip()
        return float(score_text)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"Rate limited, waiting 60s...")
            time.sleep(60)
            return score_relevancy(post, topic, api_key)
        print(f"Error scoring post {post.get('id')}: {e}")
        return 0.0
    except Exception as e:
        print(f"Error scoring post {post.get('id')}: {e}")
        return 0.0


def process_post(post: dict, topic: dict, api_key: str, index: int, total: int) -> tuple[dict, float]:
    score = score_relevancy(post, topic, api_key)
    print(f"[{index}/{total}] Post {post.get('id')}: {score:.2f}")
    post_with_score = post.copy()
    post_with_score["relevancy_score"] = score
    return post_with_score, score


def main():
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")

    if len(sys.argv) < 2:
        print("Usage: python filter_relevancy.py <input_file>")
        print("Example: python filter_relevancy.py raw/btc_price_prediction_20260219_184024.json")
        sys.exit(1)

    input_path = Path(__file__).parent / sys.argv[1]
    if not input_path.exists():
        print(f"File not found: {input_path}")
        sys.exit(1)

    filename = input_path.stem
    parts = filename.rsplit("_", 2)
    if len(parts) >= 3:
        topic_name = "_".join(parts[:-2])
    else:
        topic_name = parts[0]

    config = load_config()
    topic = get_topic_by_name(config, topic_name)
    if not topic:
        print(f"Topic '{topic_name}' not found in config.toml")
        sys.exit(1)



    with open(input_path, "r") as f:
        posts = json.load(f)

    total = len(posts)
    start_time = datetime.now()
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Topic: {topic_name}")
    print(f"Description: {topic.get('description')}")
    print(f"Total posts: {total}")
    scored_posts = []

    for i, post in enumerate(posts, 1):
        post_with_score, score = process_post(post, topic, api_key, i, total)
        scored_posts.append(post_with_score)

    scored_posts.sort(key=lambda x: x.get("relevancy_score", 0), reverse=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = input_path.parent / f"{topic_name}_scored_{timestamp}.json"

    with open(output_path, "w") as f:
        json.dump(scored_posts, f, indent=2)

    elapsed = datetime.now() - start_time
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total elapsed: {elapsed}")
    print(f"Scored posts: {len(scored_posts)}/{total}")
    print(f"Output saved to: {output_path}")


if __name__ == "__main__":
    main()
