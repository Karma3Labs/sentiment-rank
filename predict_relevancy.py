import os
import sys
import json
import tomllib
import base64
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


def build_prompt(post: dict, topic: dict) -> str:
    topic_name = topic.get("name", "")
    topic_description = topic.get("description", "")

    return f"""You are evaluating if a social media post is relevant to a specific topic.

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


def score_relevancy_openai(post: dict, topic: dict, api_key: str) -> float:
    prompt = build_prompt(post, topic)
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
            print(f"OpenAI rate limited, waiting 60s...")
            time.sleep(60)
            return score_relevancy_openai(post, topic, api_key)
        print(f"OpenAI error scoring post {post.get('id')}: {e}")
        return 0.0
    except Exception as e:
        print(f"OpenAI error scoring post {post.get('id')}: {e}")
        return 0.0


def score_relevancy_claude(post: dict, topic: dict, api_key: str) -> float:
    prompt = build_prompt(post, topic)
    content = [{"type": "text", "text": prompt}]

    media_urls = post.get("mediaUrls", [])
    for url in media_urls[:4]:
        if url and any(ext in url.lower() for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]):
            content.append({
                "type": "image",
                "source": {"type": "url", "url": url}
            })

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
                "max_tokens": 10,
                "messages": [{"role": "user", "content": content}]
            },
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        score_text = result["content"][0]["text"].strip()
        return float(score_text)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"Claude rate limited, waiting 60s...")
            time.sleep(60)
            return score_relevancy_claude(post, topic, api_key)
        print(f"Claude error scoring post {post.get('id')}: {e}")
        return 0.0
    except Exception as e:
        print(f"Claude error scoring post {post.get('id')}: {e}")
        return 0.0


def score_relevancy(post: dict, topic: dict, openai_key: str, claude_key: str) -> float:
    openai_score = score_relevancy_openai(post, topic, openai_key)
    claude_score = score_relevancy_claude(post, topic, claude_key)
    return (openai_score + claude_score) / 2


def process_post(post: dict, topic: dict, openai_key: str, claude_key: str, index: int, total: int) -> dict:
    score = score_relevancy(post, topic, openai_key, claude_key)
    print(f"[{index}/{total}] Post {post.get('id')}: {score:.2f}")
    return {"post_id": post.get("id"), "relevancy_score": score}


def main():
    openai_key = os.environ.get("OPENAI_API_KEY")
    if not openai_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")

    claude_key = os.environ.get("CLAUDE_API_KEY")
    if not claude_key:
        raise ValueError("CLAUDE_API_KEY environment variable not set")

    if len(sys.argv) < 2:
        print("Usage: python assign_relevancy.py <topic_name>")
        print("Example: python assign_relevancy.py btc_price_prediction")
        sys.exit(1)

    topic_name = sys.argv[1]
    config = load_config()
    topic = get_topic_by_name(config, topic_name)
    if not topic:
        print(f"Topic '{topic_name}' not found in config.toml")
        sys.exit(1)

    raw_dir = Path(__file__).parent / "raw"
    pattern = str(raw_dir / f"{topic_name}*.json")
    input_files = sorted(glob.glob(pattern))
    input_files = [f for f in input_files if f.endswith(f"{topic_name}.json")]

    if not input_files:
        print(f"No files found matching: {pattern}")
        sys.exit(1)

    print(f"Found {len(input_files)} input files")

    posts = []
    for input_path in input_files:
        print(f"Loading {input_path}...")
        with open(input_path, "r") as f:
            posts.extend(json.load(f))

    total = len(posts)
    start_time = datetime.now()
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Topic: {topic_name}")
    print(f"Description: {topic.get('description')}")
    print(f"Total posts: {total}")
    results = []

    for i, post in enumerate(posts, 1):
        result = process_post(post, topic, openai_key, claude_key, i, total)
        results.append(result)

    results.sort(key=lambda x: x.get("relevancy_score", 0), reverse=True)

    output_path = raw_dir / f"{topic_name}_relevancy.json"

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    elapsed = datetime.now() - start_time
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total elapsed: {elapsed}")
    print(f"Scored posts: {len(results)}/{total}")
    print(f"Output saved to: {output_path}")


if __name__ == "__main__":
    main()
