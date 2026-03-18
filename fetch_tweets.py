import os
import sys
import tomllib
import requests
import json
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import glob

load_dotenv()

print_lock = threading.Lock()
BATCHES_PER_FILE = 40


def load_config():
    config_path = Path(__file__).parent / "config.toml"
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def get_user_tweets(username: str, api_key: str, cursor: str = "", include_replies: bool = True) -> dict:
    url = "https://api.twitterapi.io/twitter/user/last_tweets"
    headers = {"X-API-Key": api_key}
    params = {"userName": username, "includeReplies": str(include_replies).lower()}
    if cursor:
        params["cursor"] = cursor
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    return {
        "tweets": data.get("data", {}).get("tweets", []),
        "has_next_page": data.get("has_next_page"),
        "next_cursor": data.get("next_cursor"),
    }


def extract_mentions(entities: dict) -> list[dict]:
    if not entities:
        return []
    mentions = entities.get("user_mentions", [])
    return [{"userName": m.get("screen_name"), "id_str": m.get("id_str"), "name": m.get("name")} for m in mentions if m.get("screen_name")]


def extract_tweet_data(tweet: dict) -> dict:
    author = tweet.get("author", {})
    entities = tweet.get("entities", {})
    return {
        "id": tweet.get("id"),
        "text": tweet.get("text"),
        "createdAt": tweet.get("createdAt"),
        "type": tweet.get("type"),
        "isReply": tweet.get("isReply"),
        "inReplyToUsername": tweet.get("inReplyToUsername"),
        "retweetCount": tweet.get("retweetCount"),
        "replyCount": tweet.get("replyCount"),
        "likeCount": tweet.get("likeCount"),
        "quoteCount": tweet.get("quoteCount"),
        "viewCount": tweet.get("viewCount"),
        "lang": tweet.get("lang"),
        "mentions": extract_mentions(entities),
        "author": {
            "id": author.get("id"),
            "userName": author.get("userName"),
            "name": author.get("name"),
            "followers": author.get("followers"),
            "following": author.get("following"),
            "isBlueVerified": author.get("isBlueVerified"),
        },
        "quoted_tweet": extract_nested_tweet(tweet.get("quoted_tweet")),
        "retweeted_tweet": extract_nested_tweet(tweet.get("retweeted_tweet")),
    }


def extract_nested_tweet(tweet) -> dict | None:
    if not tweet or not isinstance(tweet, dict):
        return None
    author = tweet.get("author", {})
    entities = tweet.get("entities", {})
    return {
        "id": tweet.get("id"),
        "text": tweet.get("text"),
        "createdAt": tweet.get("createdAt"),
        "type": tweet.get("type"),
        "isReply": tweet.get("isReply"),
        "inReplyToUsername": tweet.get("inReplyToUsername"),
        "retweetCount": tweet.get("retweetCount"),
        "replyCount": tweet.get("replyCount"),
        "likeCount": tweet.get("likeCount"),
        "quoteCount": tweet.get("quoteCount"),
        "viewCount": tweet.get("viewCount"),
        "lang": tweet.get("lang"),
        "mentions": extract_mentions(entities),
        "author": {
            "id": author.get("id"),
            "userName": author.get("userName"),
            "name": author.get("name"),
            "followers": author.get("followers"),
            "following": author.get("following"),
            "isBlueVerified": author.get("isBlueVerified"),
        },
        "quoted_tweet": extract_nested_tweet(tweet.get("quoted_tweet")),
        "retweeted_tweet": extract_nested_tweet(tweet.get("retweeted_tweet")),
    }


def fetch_tweets_until(username: str, api_key: str, cutoff_date: datetime, max_tweets: int) -> list[dict]:
    all_tweets = []
    cursor = ""
    page = 1
    while True:
        data = get_user_tweets(username, api_key, cursor)
        tweets = data.get("tweets", [])
        if not tweets:
            break

        reached_cutoff = False
        for tweet in tweets:
            created_at = datetime.strptime(tweet["createdAt"], "%a %b %d %H:%M:%S %z %Y")
            if created_at.replace(tzinfo=None) < cutoff_date:
                reached_cutoff = True
                break
            all_tweets.append(extract_tweet_data(tweet))
            if len(all_tweets) >= max_tweets:
                reached_cutoff = True
                break

        page += 1

        if reached_cutoff or not data.get("has_next_page"):
            break
        cursor = data.get("next_cursor", "")
        time.sleep(0.1)
    return all_tweets


def fetch_user_tweets(username: str, api_key: str, cutoff_date: datetime, max_tweets: int, index: int, total: int) -> tuple[str, list[dict], bool]:
    try:
        tweets = fetch_tweets_until(username, api_key, cutoff_date, max_tweets)
        with print_lock:
            print(f"[{index}/{total}] {username}: {len(tweets)} tweets")
        return username, tweets, True
    except Exception as e:
        with print_lock:
            print(f"[{index}/{total}] {username}: Error - {e}")
        return username, [], False


def process_batch(batch: list[tuple[str, int]], api_key: str, cutoff_date: datetime, max_tweets: int, total: int, parallel_requests: int) -> dict:
    results = {}
    queue = list(batch)
    retry_counts = {username: 0 for username, _ in batch}

    while queue:
        with ThreadPoolExecutor(max_workers=parallel_requests) as executor:
            futures = {
                executor.submit(fetch_user_tweets, username, api_key, cutoff_date, max_tweets, index, total): (username, index)
                for username, index in queue
            }

            failed = []
            for future in as_completed(futures):
                username, index = futures[future]
                _, tweets, success = future.result()

                if success:
                    results[username] = tweets
                else:
                    retry_counts[username] += 1
                    failed.append((username, index))
                    with print_lock:
                        print(f"[{index}/{total}] {username}: Retry {retry_counts[username]}")

            queue = failed
            if queue:
                time.sleep(1)

    return results


def get_checkpoint_path(raw_dir: Path, category: str, file_index: int) -> Path:
    start_batch = file_index * BATCHES_PER_FILE
    end_batch = start_batch + BATCHES_PER_FILE
    return raw_dir / f"{category}_tweets_checkpoint_{start_batch}_{end_batch}.json"


def load_processed_users(raw_dir: Path, category: str) -> set:
    processed = set()
    pattern = str(raw_dir / f"{category}_tweets_checkpoint_*.json")
    files = sorted(glob.glob(pattern))

    for filepath in files:
        print(f"Loading processed users from {filepath}...")
        with open(filepath, "r") as f:
            data = json.load(f)
        processed.update(data.get("processed", []))

    return processed


def load_checkpoint(checkpoint_path: Path) -> tuple[dict, set]:
    if checkpoint_path.exists():
        with open(checkpoint_path, "r") as f:
            data = json.load(f)
        return data.get("results", {}), set(data.get("processed", []))
    return {}, set()


def save_checkpoint(checkpoint_path: Path, results: dict, processed: set):
    with open(checkpoint_path, "w") as f:
        json.dump({"results": results, "processed": list(processed)}, f)


def main():
    if len(sys.argv) < 2:
        print("Usage: python fetch_tweets.py <category>")
        print("Example: python fetch_tweets.py crypto")
        sys.exit(1)

    category = sys.argv[1]

    api_key = os.environ.get("TWITTER_API_KEY")
    if not api_key:
        raise ValueError("TWITTER_API_KEY environment variable not set")

    config = load_config()

    look_back = config.get("look_back", {})
    year = look_back.get("year", 2026)
    month = look_back.get("month", 1)
    cutoff_date = datetime(year, month, 1)

    parallel_requests = look_back.get("parallel_requests", 100)
    max_tweets = look_back.get("max_tweets", 100)

    categories_list = config.get("categories", {}).get(category, [])
    seed_usernames = [p.lstrip("@") for p in categories_list]

    all_users = [u for u in seed_usernames if u]
    total = len(all_users)
    start_time = datetime.now()
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total unique users: {total}")
    print(f"Cutoff date: {cutoff_date}")
    print(f"Parallel requests: {parallel_requests}")
    print(f"Max tweets per user: {max_tweets}")

    raw_dir = Path(__file__).parent / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    processed = load_processed_users(raw_dir, category)
    print(f"Already processed users: {len(processed)}")

    users_to_fetch = [(u, i) for i, u in enumerate(all_users, 1) if u not in processed]
    print(f"Users to fetch: {len(users_to_fetch)}")

    batches = [users_to_fetch[i:i + parallel_requests] for i in range(0, len(users_to_fetch), parallel_requests)]
    total_batches = len(batches)

    completed_files = len(glob.glob(str(raw_dir / f"{category}_tweets_checkpoint_*.json")))
    current_file_index = completed_files
    current_file_results = {}
    current_file_processed = set()
    batches_in_current_file = 0

    for batch_num, batch in enumerate(batches, 1):
        print(f"\n=== Batch {batch_num}/{total_batches} ({len(batch)} users) ===")
        batch_results = process_batch(batch, api_key, cutoff_date, max_tweets, total, parallel_requests)

        current_file_results.update(batch_results)
        current_file_processed.update(batch_results.keys())
        batches_in_current_file += 1

        elapsed = datetime.now() - start_time

        checkpoint_path = get_checkpoint_path(raw_dir, category, current_file_index)
        print(f"Saving data file {checkpoint_path}... (elapsed: {elapsed})")
        save_checkpoint(checkpoint_path, current_file_results, current_file_processed)

        if batches_in_current_file >= BATCHES_PER_FILE:
            current_file_index += 1
            current_file_results = {}
            current_file_processed = set()
            batches_in_current_file = 0

    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total elapsed: {datetime.now() - start_time}")


if __name__ == "__main__":
    main()
