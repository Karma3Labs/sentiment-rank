import os
import tomllib
import requests
import json
import re
import glob
import time
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


API_REPLIES_URL = "https://api.twitterapi.io/twitter/tweet/replies/v2"
API_QUOTES_URL = "https://api.twitterapi.io/twitter/tweet/quotes"
API_RETWEETERS_URL = "https://api.twitterapi.io/twitter/tweet/retweeters"


def normalize_author(author: dict) -> dict:
    return {
        "id": author.get("id"),
        "userName": author.get("userName"),
        "name": author.get("name"),
        "followers": author.get("followers"),
        "isBlueVerified": author.get("isBlueVerified"),
    }


def normalize_tweet(tweet: dict) -> dict:
    author = tweet.get("author", {})
    return {
        "id": tweet.get("id"),
        "text": tweet.get("text"),
        "createdAt": tweet.get("createdAt"),
        "likeCount": tweet.get("likeCount"),
        "retweetCount": tweet.get("retweetCount"),
        "replyCount": tweet.get("replyCount"),
        "quoteCount": tweet.get("quoteCount"),
        "mediaUrls": [],
        "author": normalize_author(author),
    }


def api_request_with_retry(url: str, headers: dict, params: dict, max_retries: int = 3) -> dict | None:
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 500:
                time.sleep(1)
                continue
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                return None
    return None


def get_tweet_replies(tweet_id: str, api_key: str) -> list[dict]:
    headers = {"X-API-Key": api_key}
    params = {"tweetId": tweet_id}
    replies = []
    cursor = ""
    while True:
        if cursor:
            params["cursor"] = cursor
        data = api_request_with_retry(API_REPLIES_URL, headers, params)
        if data is None:
            break
        for tweet in data.get("tweets", []):
            if tweet.get("id") != tweet_id and tweet.get("isReply"):
                replies.append(normalize_tweet(tweet))
        if not data.get("has_next_page"):
            break
        cursor = data.get("next_cursor", "")
        if not cursor:
            break
    return replies


def get_tweet_quotes(tweet_id: str, api_key: str) -> list[dict]:
    headers = {"X-API-Key": api_key}
    params = {"tweetId": tweet_id}
    quotes = []
    cursor = ""
    while True:
        if cursor:
            params["cursor"] = cursor
        data = api_request_with_retry(API_QUOTES_URL, headers, params)
        if data is None:
            break
        for quote in data.get("tweets", []):
            quotes.append(normalize_tweet(quote))
        if not data.get("has_next_page"):
            break
        cursor = data.get("next_cursor", "")
        if not cursor:
            break
    return quotes


def get_tweet_retweeters(tweet_id: str, api_key: str) -> list[dict]:
    headers = {"X-API-Key": api_key}
    params = {"tweetId": tweet_id}
    retweeters = []
    cursor = ""
    while True:
        if cursor:
            params["cursor"] = cursor
        data = api_request_with_retry(API_RETWEETERS_URL, headers, params)
        if data is None:
            break
        for user in data.get("users", []):
            retweeters.append(normalize_author(user))
        if not data.get("has_next_page"):
            break
        cursor = data.get("next_cursor", "")
        if not cursor:
            break
    return retweeters


def load_config(config_path: str = "config.toml") -> dict:
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def load_local_tweets(raw_dir: Path, category: str) -> list[dict]:
    all_tweets = []

    patterns = [
        str(raw_dir / f"{category}_tweets_checkpoint_*.json"),
        str(raw_dir / f"{category}_extended_tweets_checkpoint_*.json"),
    ]

    for pattern in patterns:
        files = glob.glob(pattern)
        for filepath in files:
            with open(filepath, "r") as f:
                data = json.load(f)
            results = data.get("results", {})
            for username, tweets in results.items():
                all_tweets.extend(tweets)

    return all_tweets


def parse_query(query: str) -> tuple[list[str], list[str]]:
    terms = []
    excluded = []

    parts = re.findall(r'\([^)]+\)|-\w+:\w+|\S+', query)

    for part in parts:
        if part.startswith('-'):
            continue
        if part.endswith(':en'):
            continue
        if part.startswith('(') and part.endswith(')'):
            inner = part[1:-1]
            words = [w.strip().lower().lstrip('$') for w in inner.split(' OR ')]
            terms.append(words)
        else:
            terms.append([part.lower().lstrip('$')])

    return terms, excluded


def tweet_matches_query(tweet: dict, terms: list[list[str]]) -> bool:
    text = (tweet.get("text") or "").lower()

    for term_group in terms:
        if not any(term in text for term in term_group):
            return False

    return True


def score_tweet(tweet: dict) -> int:
    return (
        (tweet.get("likeCount") or 0) +
        (tweet.get("retweetCount") or 0) * 2 +
        (tweet.get("replyCount") or 0) +
        (tweet.get("quoteCount") or 0) * 2
    )


def search_local_tweets(tweets: list[dict], query: str, limit: int = 100, cutoff_date: datetime = None) -> list[dict]:
    terms, _ = parse_query(query)

    matching = []
    for tweet in tweets:
        if tweet.get("type") == "retweet":
            continue
        if tweet.get("retweeted_tweet"):
            continue
        if cutoff_date and tweet.get("createdAt"):
            try:
                created_at = datetime.strptime(tweet["createdAt"], "%a %b %d %H:%M:%S %z %Y")
                if created_at.replace(tzinfo=None) < cutoff_date:
                    continue
            except ValueError:
                pass
        if tweet_matches_query(tweet, terms):
            matching.append(tweet)

    matching.sort(key=score_tweet, reverse=True)
    return matching[:limit]


def get_topic(config: dict, topic_name: str) -> dict | None:
    for topic in config.get("topics", []):
        if topic.get("name") == topic_name:
            return topic
    return None


def list_topics(config: dict) -> list[str]:
    return [topic["name"] for topic in config.get("topics", [])]


def process_tweet(tweet: dict, api_key: str, index: int, total: int) -> dict:
    normalized = normalize_tweet(tweet)
    tweet_id = normalized["id"]
    normalized["replies"] = get_tweet_replies(tweet_id, api_key)
    normalized["quotes"] = get_tweet_quotes(tweet_id, api_key)
    normalized["retweeters"] = get_tweet_retweeters(tweet_id, api_key)
    print(f"[{index}/{total}] {tweet_id}: replies={len(normalized['replies'])} quotes={len(normalized['quotes'])} retweeters={len(normalized['retweeters'])}")
    return normalized


def search_topic(
    topic_name: str,
    config: dict,
    api_key: str,
    category: str,
) -> list[dict]:
    topic = get_topic(config, topic_name)
    if not topic:
        available = list_topics(config)
        raise ValueError(f"Topic '{topic_name}' not found. Available: {available}")

    query = topic["query"]
    raw_dir = Path(__file__).parent / "raw"

    search_look_back = config.get("search_look_back", {})
    look_back_days = search_look_back.get("days", 30)
    cutoff_date = datetime.now() - timedelta(days=look_back_days)

    print(f"Loading local tweets for category '{category}'...")
    all_tweets = load_local_tweets(raw_dir, category)
    print(f"Loaded {len(all_tweets)} tweets")

    print(f"Searching for matches (look back {look_back_days} days)...")
    matching = search_local_tweets(all_tweets, query, limit=100, cutoff_date=cutoff_date)
    print(f"Found {len(matching)} matching tweets")

    look_back = config.get("look_back", {})
    parallel_requests = look_back.get("parallel_requests", 100)

    results = [None] * len(matching)
    with ThreadPoolExecutor(max_workers=parallel_requests) as executor:
        futures = {
            executor.submit(process_tweet, tweet, api_key, i, len(matching)): i - 1
            for i, tweet in enumerate(matching, 1)
        }
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()

    return results


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Search local tweets")
    parser.add_argument("topic", nargs="?", help="Topic name from config.toml")
    parser.add_argument("--config", default="config.toml", help="Path to config file")
    parser.add_argument("--list", action="store_true", help="List available topics")
    args = parser.parse_args()

    config = load_config(args.config)

    if args.list:
        print("Available topics:")
        for topic in config.get("topics", []):
            print(f"  {topic['name']}: {topic.get('description', '')}")
        return 0

    if not args.topic:
        parser.error("topic is required (use --list to see available topics)")

    api_key = os.environ.get("TWITTER_API_KEY")
    if not api_key:
        print("Error: TWITTER_API_KEY environment variable not set")
        return 1

    category = os.environ.get("CATEGORY", "crypto")
    tweets = search_topic(args.topic, config, api_key, category)

    raw_dir = Path("raw")
    raw_dir.mkdir(exist_ok=True)
    output_path = raw_dir / f"{args.topic}.json"

    with open(output_path, "w") as f:
        json.dump(tweets, f, indent=2)
    print(f"Saved {len(tweets)} tweets to {output_path}")

    return 0


if __name__ == "__main__":
    exit(main())
