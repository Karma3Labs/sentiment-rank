import os
import sys
import json
import glob
import tomllib
import requests
import time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()


def load_topics_from_raw() -> list[dict]:
    raw_dir = Path(__file__).parent / "raw"
    topic_files = glob.glob(str(raw_dir / "*_topics.json"))

    topics = []
    for topic_file in topic_files:
        category = os.path.basename(topic_file).replace("_topics.json", "")
        with open(topic_file, "r") as f:
            for topic in json.load(f):
                topic["category"] = category
                topics.append(topic)

    return topics


def get_topic_by_slug(topic_slug: str) -> dict | None:
    topics = load_topics_from_raw()
    for topic in topics:
        if topic.get("slug") == topic_slug:
            return topic
    return None


def build_prompt(topic: dict, post_text: str) -> str:
    markets_str = ", ".join([m.get("name", "") for m in topic.get("markets", [])])
    return f"""You are evaluating if a social media post is relevant to a prediction market event.

Event: {topic.get("description", "")}
Available markets: {markets_str}

Post text:
{post_text[:4000]}

Based on the event and available markets, rate how relevant this post is to the topic.
Return ONLY a number between 0.0 and 1.0 where:
- 0.0 = completely irrelevant
- 1.0 = highly relevant (directly discusses the event or makes predictions about it)

Return only the number, nothing else."""


def get_openai_relevancy(post_text: str, topic: dict, api_key: str, max_retries: int = 3) -> float | None:
    prompt = build_prompt(topic, post_text)
    retries = 0
    while retries < max_retries:
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
                    "max_tokens": 10,
                    "temperature": 0
                },
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            score_str = result["choices"][0]["message"]["content"].strip()
            return max(0.0, min(1.0, float(score_str)))
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"OpenAI rate limited, waiting 60s...")
                time.sleep(60)
                continue
            retries += 1
        except (ValueError, KeyError):
            retries += 1
        except Exception:
            retries += 1
    return None


def get_claude_relevancy(post_text: str, topic: dict, api_key: str, max_retries: int = 3) -> float | None:
    prompt = build_prompt(topic, post_text)
    retries = 0
    while retries < max_retries:
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
                    "messages": [{"role": "user", "content": prompt}]
                },
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            score_str = result["content"][0]["text"].strip()
            return max(0.0, min(1.0, float(score_str)))
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"Claude rate limited, waiting 60s...")
                time.sleep(60)
                continue
            retries += 1
        except (ValueError, KeyError):
            retries += 1
        except Exception:
            retries += 1
    return None


def score_single_post(post: dict, topic: dict, openai_key: str, anthropic_key: str) -> dict:
    post_text = post.get("text", "")

    openai_score = get_openai_relevancy(post_text, topic, openai_key)
    claude_score = get_claude_relevancy(post_text, topic, anthropic_key)

    scores = [s for s in [openai_score, claude_score] if s is not None]
    if scores:
        avg_score = sum(scores) / len(scores)
    else:
        avg_score = 0.0

    return {
        "post_id": post.get("id"),
        "relevancy_score": avg_score,
        "openai_score": openai_score,
        "claude_score": claude_score
    }


def score_relevancy(posts: list[dict], topic: dict, openai_key: str, anthropic_key: str, parallel_requests: int) -> list[dict]:
    results = []
    total = len(posts)

    with ThreadPoolExecutor(max_workers=parallel_requests) as executor:
        futures = {
            executor.submit(score_single_post, post, topic, openai_key, anthropic_key): post
            for post in posts
        }

        for future in as_completed(futures):
            post = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(f"[{len(results)}/{total}] Post {result['post_id']}: openai={result['openai_score']}, claude={result['claude_score']}, avg={result['relevancy_score']:.2f}")
            except Exception as e:
                print(f"Error processing post {post.get('id')}: {e}")

    return results


def load_config(config_path: str = "config.toml") -> dict:
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def main():
    openai_key = os.environ.get("OPENAI_API_KEY")
    anthropic_key = os.environ.get("CLAUDE_API_KEY")
    if not openai_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")
    if not anthropic_key:
        raise ValueError("CLAUDE_API_KEY environment variable not set")

    config = load_config()
    parallel_requests = config.get("search", {}).get("parallel_requests", 10)

    if len(sys.argv) < 2:
        print("Usage: python predict_relevancy.py <topic_slug>")
        print("Example: python predict_relevancy.py what-price-will-bitcoin-hit-in-march-2026")
        sys.exit(1)

    topic_slug = sys.argv[1]
    topic = get_topic_by_slug(topic_slug)
    if not topic:
        print(f"Topic '{topic_slug}' not found in raw/*_topics.json")
        sys.exit(1)

    raw_dir = Path(__file__).parent / "raw"
    pattern = str(raw_dir / f"{topic_slug}*.json")
    input_files = sorted(glob.glob(pattern))
    input_files = [f for f in input_files if f.endswith(f"{topic_slug}.json")]

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
    print(f"Topic: {topic_slug}")
    print(f"Description: {topic.get('description')}")
    print(f"Markets: {[m.get('name') for m in topic.get('markets', [])]}")
    print(f"Total posts: {total}")
    print(f"Parallel requests: {parallel_requests}")

    results = score_relevancy(posts, topic, openai_key, anthropic_key, parallel_requests)
    results.sort(key=lambda x: x.get("relevancy_score", 0), reverse=True)

    output_path = raw_dir / f"{topic_slug}_relevancy.json"

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    elapsed = datetime.now() - start_time
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total elapsed: {elapsed}")
    print(f"Scored posts: {len(results)}/{total}")
    print(f"Output saved to: {output_path}")


if __name__ == "__main__":
    main()
