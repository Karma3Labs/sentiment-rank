#!/usr/bin/env python3
import os
import sys
import tomllib
import subprocess
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def load_config():
    config_path = Path(__file__).parent / "config.toml"
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def run_step(step_num: int, total_steps: int, description: str, script: str, args: list[str]):
    print(f"\n[{step_num}/{total_steps}] {description}...")
    result = subprocess.run([sys.executable, script] + args, cwd=Path(__file__).parent)
    if result.returncode != 0:
        print(f"Step failed with exit code {result.returncode}")
        return False
    return True


def run_topic(topic_name: str) -> bool:
    print(f"\n{'=' * 40}")
    print(f"Running pipeline for topic: {topic_name}")
    print('=' * 40)

    raw_dir = Path(__file__).parent / "raw"

    steps = [
        ("Searching tweets", "search_tweets.py", [topic_name], f"{topic_name}.json"),
        ("Predicting relevancy", "predict_relevancy.py", [topic_name], f"{topic_name}_relevancy.json"),
        ("Predicting sentiment", "predict_sentiment.py", [topic_name], f"{topic_name}_prediction.json"),
        ("Weighting posts", "weight_posts.py", [topic_name], f"{topic_name}_weighted.json"),
    ]

    for i, (desc, script, args, output_file) in enumerate(steps, 1):
        if (raw_dir / output_file).exists():
            print(f"\n[{i}/{len(steps)}] {desc}... SKIPPED (file exists)")
            continue
        if not run_step(i, len(steps), desc, script, args):
            return False

    print(f"\n{'=' * 40}")
    print(f"Pipeline complete for topic: {topic_name}")
    print('=' * 40)
    return True


def main():
    config = load_config()
    topics = [t["name"] for t in config.get("topics", [])]

    if not topics:
        print("No topics found in config.toml")
        return 1

    print(f"Found {len(topics)} topics")

    failed = []
    for i, topic_name in enumerate(topics, 1):
        print(f"\n\n{'#' * 50}")
        print(f"# Topic {i}/{len(topics)}: {topic_name}")
        print('#' * 50)

        if not run_topic(topic_name):
            failed.append(topic_name)
            print(f"Topic {topic_name} failed, continuing...")

    print("\n[Final] Inserting topics...")
    subprocess.run([sys.executable, "insert_topics.py"], cwd=Path(__file__).parent)

    print("\n[Final] Inserting posts...")
    subprocess.run([sys.executable, "insert_posts.py"], cwd=Path(__file__).parent)

    print(f"\n{'=' * 50}")
    print("All topics processed")
    print(f"Succeeded: {len(topics) - len(failed)}/{len(topics)}")
    if failed:
        print(f"Failed: {failed}")
    print('=' * 50)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
