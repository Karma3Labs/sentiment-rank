#!/usr/bin/env python3
import os
import sys
import json
import glob
import subprocess
from pathlib import Path
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


def run_step(step_num: int, total_steps: int, description: str, script: str, args: list[str]):
    print(f"\n[{step_num}/{total_steps}] {description}...")
    result = subprocess.run([sys.executable, script] + args, cwd=Path(__file__).parent)
    if result.returncode != 0:
        print(f"Step failed with exit code {result.returncode}")
        return False
    return True


def run_topic(topic_slug: str) -> bool:
    print(f"\n{'=' * 40}")
    print(f"Running pipeline for topic: {topic_slug}")
    print('=' * 40)

    raw_dir = Path(__file__).parent / "raw"

    steps = [
        ("Searching tweets", "search_tweets.py", [topic_slug], f"{topic_slug}.json"),
        ("Predicting relevancy", "predict_relevancy.py", [topic_slug], f"{topic_slug}_relevancy.json"),
        ("Predicting sentiment", "predict_sentiment.py", [topic_slug], f"{topic_slug}_prediction.json"),
        ("Weighting posts", "weight_posts.py", [topic_slug], f"{topic_slug}_weighted.json"),
    ]

    for i, (desc, script, args, output_file) in enumerate(steps, 1):
        if (raw_dir / output_file).exists():
            print(f"\n[{i}/{len(steps)}] {desc}... SKIPPED (file exists)")
            continue
        if not run_step(i, len(steps), desc, script, args):
            return False

    print(f"\n{'=' * 40}")
    print(f"Pipeline complete for topic: {topic_slug}")
    print('=' * 40)
    return True


def main():
    topics = load_topics_from_raw()
    topic_slugs = [t["slug"] for t in topics]

    if not topic_slugs:
        print("No topics found in raw/*_topics.json")
        return 1

    print(f"Found {len(topic_slugs)} topics")

    failed = []
    for i, topic_slug in enumerate(topic_slugs, 1):
        print(f"\n\n{'#' * 50}")
        print(f"# Topic {i}/{len(topic_slugs)}: {topic_slug}")
        print('#' * 50)

        if not run_topic(topic_slug):
            failed.append(topic_slug)
            print(f"Topic {topic_slug} failed, continuing...")

    print("\n[Final] Inserting topics...")
    subprocess.run([sys.executable, "insert_topics.py"], cwd=Path(__file__).parent)

    print("\n[Final] Inserting posts...")
    subprocess.run([sys.executable, "insert_posts.py"], cwd=Path(__file__).parent)

    print(f"\n{'=' * 50}")
    print("All topics processed")
    print(f"Succeeded: {len(topic_slugs) - len(failed)}/{len(topic_slugs)}")
    if failed:
        print(f"Failed: {failed}")
    print('=' * 50)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
