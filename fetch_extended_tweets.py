import os
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
    return raw_dir / f"{category}_extended_tweets_checkpoint_{start_batch}_{end_batch}.json"


def load_processed_users(raw_dir: Path, category: str) -> set:
    processed = set()
    pattern = str(raw_dir / f"{category}_extended_tweets_checkpoint_*.json")
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


def load_seed_tweets(raw_dir: Path, category: str) -> dict:
    all_results = {}
    pattern = str(raw_dir / f"{category}_tweets_checkpoint_*.json")
    files = sorted(glob.glob(pattern))

    for filepath in files:
        print(f"Loading seed tweets from {filepath}...")
        with open(filepath, "r") as f:
            data = json.load(f)
        all_results.update(data.get("results", {}))

    return all_results


def extract_interacting_users(tweets_data: dict) -> set:
    users = set()

    for username, tweets in tweets_data.items():
        for tweet in tweets:
            # Extract from mentions
            for mention in tweet.get("mentions", []):
                if mention.get("userName"):
                    users.add(mention["userName"])

            # Extract from inReplyToUsername
            if tweet.get("inReplyToUsername"):
                users.add(tweet["inReplyToUsername"])

            # Extract from quoted_tweet author
            quoted = tweet.get("quoted_tweet")
            if quoted and quoted.get("author", {}).get("userName"):
                users.add(quoted["author"]["userName"])

            # Extract from retweeted_tweet author
            retweeted = tweet.get("retweeted_tweet")
            if retweeted and retweeted.get("author", {}).get("userName"):
                users.add(retweeted["author"]["userName"])

    return users


def main():
    api_key = os.environ.get("TWITTER_API_KEY")
    if not api_key:
        raise ValueError("TWITTER_API_KEY environment variable not set")

    config = load_config()
    category = os.environ.get("CATEGORY", "crypto")

    look_back = config.get("look_back", {})
    year = look_back.get("year", 2026)
    month = look_back.get("month", 1)
    cutoff_date = datetime(year, month, 1)

    parallel_requests = look_back.get("parallel_requests", 100)
    max_tweets = look_back.get("max_tweets", 100)

    raw_dir = Path(__file__).parent / "raw"

    # Load seed tweets
    seed_tweets = load_seed_tweets(raw_dir, category)
    if not seed_tweets:
        print("No seed tweets found. Run fetch_tweets.py first.")
        return

    # Extract interacting users
    seed_usernames = set(seed_tweets.keys())
    interacting_users = extract_interacting_users(seed_tweets)
    extended_users = interacting_users - seed_usernames

    print(f"Seed users: {len(seed_usernames)}")
    print(f"Interacting users found: {len(interacting_users)}")
    print(f"Extended users (excluding seeds): {len(extended_users)}")

    all_users = sorted(extended_users)
    total = len(all_users)
    start_time = datetime.now()
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total unique users: {total}")
    print(f"Cutoff date: {cutoff_date}")
    print(f"Parallel requests: {parallel_requests}")
    print(f"Max tweets per user: {max_tweets}")

    processed = load_processed_users(raw_dir, category)
    print(f"Already processed users: {len(processed)}")

    users_to_fetch = [(u, i) for i, u in enumerate(all_users, 1) if u not in processed]
    print(f"Users to fetch: {len(users_to_fetch)}")

    batches = [users_to_fetch[i:i + parallel_requests] for i in range(0, len(users_to_fetch), parallel_requests)]
    total_batches = len(batches)

    completed_files = len(glob.glob(str(raw_dir / f"{category}_extended_tweets_checkpoint_*.json")))
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

        if batches_in_current_file >= BATCHES_PER_FILE or batch_num == total_batches:
            checkpoint_path = get_checkpoint_path(raw_dir, category, current_file_index)
            print(f"Saving data file {checkpoint_path}... (elapsed: {elapsed})")
            save_checkpoint(checkpoint_path, current_file_results, current_file_processed)

            current_file_index += 1
            current_file_results = {}
            current_file_processed = set()
            batches_in_current_file = 0
        else:
            print(f"Batch {batch_num} done (elapsed: {elapsed})")

    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total elapsed: {datetime.now() - start_time}")


if __name__ == "__main__":
    main()
