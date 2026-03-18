import os
import sys
import json
import glob
import requests
import time
import math
from pathlib import Path
from datetime import datetime
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


def get_embeddings(texts: list[str], api_key: str, max_retries: int = 3) -> list[list[float]]:
    retries = 0
    while retries < max_retries:
        try:
            response = requests.post(
                "https://api.openai.com/v1/embeddings",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "text-embedding-3-large",
                    "input": texts
                },
                timeout=60
            )
            response.raise_for_status()
            result = response.json()
            embeddings = [None] * len(texts)
            for item in result["data"]:
                embeddings[item["index"]] = item["embedding"]
            return embeddings
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"Rate limited, waiting 60s...")
                time.sleep(60)
                continue
            print(f"Error getting embeddings: {e}, retrying...")
            retries += 1
        except Exception as e:
            print(f"Error getting embeddings: {e}, retrying...")
            retries += 1
    return []


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot_product = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot_product / (norm_a * norm_b)


def score_relevancy_batch(posts: list[dict], topic: dict, api_key: str, batch_size: int = 2000) -> list[dict]:
    topic_text = f"{topic.get('description', '')} {topic.get('slug', '')}"
    post_texts = [post.get("text", "")[:8000] for post in posts]

    all_texts = [topic_text] + post_texts

    all_embeddings = []
    for i in range(0, len(all_texts), batch_size):
        batch = all_texts[i:i + batch_size]
        print(f"  Getting embeddings for batch {i // batch_size + 1}/{math.ceil(len(all_texts) / batch_size)}...")
        embeddings = get_embeddings(batch, api_key)
        if not embeddings:
            print(f"  Failed to get embeddings for batch, using zeros")
            embeddings = [[0.0] * 3072] * len(batch)
        all_embeddings.extend(embeddings)

    topic_embedding = all_embeddings[0]
    post_embeddings = all_embeddings[1:]

    results = []
    for i, post in enumerate(posts):
        similarity = cosine_similarity(topic_embedding, post_embeddings[i])
        score = max(0.0, min(1.0, similarity))
        results.append({
            "post_id": post.get("id"),
            "relevancy_score": score
        })
        print(f"[{i + 1}/{len(posts)}] Post {post.get('id')}: {score:.2f}")

    return results


def main():
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")

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
    print(f"Total posts: {total}")

    results = score_relevancy_batch(posts, topic, api_key)
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
