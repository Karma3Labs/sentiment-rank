import os
import tomllib
import requests
from datetime import datetime
from pathlib import Path
from typing import Iterator
from concurrent.futures import ThreadPoolExecutor


API_BASE_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"
API_REPLIES_URL = "https://api.twitterapi.io/twitter/tweet/replies"
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

    media_urls = []
    extended_entities = tweet.get("extendedEntities", {})
    for media in extended_entities.get("media", []):
        url = media.get("media_url_https")
        if url:
            media_urls.append(url)

    return {
        "id": tweet.get("id"),
        "text": tweet.get("text"),
        "createdAt": tweet.get("createdAt"),
        "likeCount": tweet.get("likeCount"),
        "retweetCount": tweet.get("retweetCount"),
        "replyCount": tweet.get("replyCount"),
        "quoteCount": tweet.get("quoteCount"),
        "mediaUrls": media_urls,
        "author": normalize_author(author),
    }


def get_tweet_replies(tweet_id: str, api_key: str, max_replies: int = 250, max_pages: int = 5) -> list[dict]:
    headers = {"X-API-Key": api_key}
    params = {"tweetId": tweet_id}
    replies = []
    cursor = ""
    page_count = 0
    while True:
        page_count += 1
        if cursor:
            params["cursor"] = cursor
        response = requests.get(API_REPLIES_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        for tweet in data.get("tweets", []):
            if tweet.get("id") != tweet_id:
                replies.append(normalize_tweet(tweet))
                if len(replies) >= max_replies:
                    return replies
        if page_count >= max_pages:
            break
        if not data.get("has_next_page"):
            break
        cursor = data.get("next_cursor", "")
        if not cursor:
            break
    return replies


def get_tweet_quotes(tweet_id: str, api_key: str, max_quotes: int = 250, max_pages: int = 5) -> list[dict]:
    headers = {"X-API-Key": api_key}
    params = {"tweetId": tweet_id}
    quotes = []
    cursor = ""
    page_count = 0
    while True:
        page_count += 1
        if cursor:
            params["cursor"] = cursor
        response = requests.get(API_QUOTES_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        for quote in data.get("tweets", []):
            quotes.append(normalize_tweet(quote))
            if len(quotes) >= max_quotes:
                return quotes
        if page_count >= max_pages:
            break
        if not data.get("has_next_page"):
            break
        cursor = data.get("next_cursor", "")
        if not cursor:
            break
    return quotes


def get_tweet_retweeters(tweet_id: str, api_key: str, max_retweeters: int = 250, max_pages: int = 5) -> list[dict]:
    headers = {"X-API-Key": api_key}
    params = {"tweetId": tweet_id}
    retweeters = []
    cursor = ""
    page_count = 0
    while True:
        page_count += 1
        if cursor:
            params["cursor"] = cursor
        response = requests.get(API_RETWEETERS_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        for user in data.get("users", []):
            retweeters.append(normalize_author(user))
            if len(retweeters) >= max_retweeters:
                return retweeters
        if page_count >= max_pages:
            break
        if not data.get("has_next_page"):
            break
        cursor = data.get("next_cursor", "")
        if not cursor:
            break
    return retweeters


def load_config(config_path: str = "config.toml") -> dict:
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def search_tweets(
    query: str,
    api_key: str,
    query_type: str = "Latest",
    cursor: str = "",
) -> dict:
    headers = {"X-API-Key": api_key}
    params = {
        "query": query,
        "queryType": query_type,
    }
    if cursor:
        params["cursor"] = cursor

    response = requests.get(API_BASE_URL, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def process_tweet(tweet: dict, api_key: str, max_replies: int, max_quotes: int, max_retweeters: int, max_pages: int) -> dict:
    normalized = normalize_tweet(tweet)
    tweet_id = normalized["id"]

    with ThreadPoolExecutor(max_workers=3) as executor:
        replies_future = executor.submit(get_tweet_replies, tweet_id, api_key, max_replies, max_pages)
        quotes_future = executor.submit(get_tweet_quotes, tweet_id, api_key, max_quotes, max_pages)
        retweeters_future = executor.submit(get_tweet_retweeters, tweet_id, api_key, max_retweeters, max_pages)

        normalized["replies"] = replies_future.result()
        normalized["quotes"] = quotes_future.result()
        normalized["retweeters"] = retweeters_future.result()

    return normalized


def search_all_tweets(
    query: str,
    api_key: str,
    query_type: str = "Latest",
    max_pages: int | None = None,
    max_tweets: int | None = None,
    fetch_limits: dict | None = None,
    parallel_requests: int = 100,
) -> list[dict]:
    limits = fetch_limits or {}
    max_replies = limits.get("max_replies", 250)
    max_quotes = limits.get("max_quotes", 250)
    max_retweeters = limits.get("max_retweeters", 250)
    sub_max_pages = max_pages or 5

    cursor = ""
    page_count = 0
    all_tweets = []

    while True:
        page_count += 1
        page_info = f"/{max_pages}" if max_pages else ""
        print(f"Fetching page {page_count}{page_info}...")
        result = search_tweets(query, api_key, query_type, cursor)
        tweets = result.get("tweets", [])
        print(f"  Got {len(tweets)} tweets")

        for tweet in tweets:
            all_tweets.append(tweet)
            if max_tweets and len(all_tweets) >= max_tweets:
                break

        if max_tweets and len(all_tweets) >= max_tweets:
            break

        if max_pages and page_count >= max_pages:
            break

        if not result.get("has_next_page"):
            break

        cursor = result.get("next_cursor", "")
        if not cursor:
            break

    print(f"Processing {len(all_tweets)} tweets with {parallel_requests} parallel requests...")
    results = []

    with ThreadPoolExecutor(max_workers=parallel_requests) as executor:
        futures = {
            executor.submit(process_tweet, tweet, api_key, max_replies, max_quotes, max_retweeters, sub_max_pages): i
            for i, tweet in enumerate(all_tweets)
        }

        for future in futures:
            idx = futures[future]
            try:
                normalized = future.result()
                results.append((idx, normalized))
                print(f"  [{len(results)}/{len(all_tweets)}] {normalized['id']}: r={len(normalized['replies'])} q={len(normalized['quotes'])} rt={len(normalized['retweeters'])}")
            except Exception as e:
                print(f"  [{len(results)}/{len(all_tweets)}] Error: {e}")

    results.sort(key=lambda x: x[0])
    return [r[1] for r in results]


def get_topic(config: dict, topic_name: str) -> dict | None:
    for topic in config.get("topics", []):
        if topic.get("name") == topic_name:
            return topic
    return None


def list_topics(config: dict) -> list[str]:
    return [topic["name"] for topic in config.get("topics", [])]


def search_topic(
    topic_name: str,
    config: dict,
    api_key: str,
    max_pages: int | None = None,
) -> list[dict]:
    topic = get_topic(config, topic_name)
    if not topic:
        available = list_topics(config)
        raise ValueError(f"Topic '{topic_name}' not found. Available: {available}")

    query = topic["query"]
    search_config = config.get("search", {})
    query_type = search_config.get("query_type", "Top")
    max_tweets = search_config.get("max_tweets", 100)
    fetch_limits = config.get("fetch_limits", {})

    if max_pages is None:
        max_pages = search_config.get("max_pages")

    parallel_requests = search_config.get("parallel_requests", 10)
    tweets = search_all_tweets(query, api_key, query_type, max_pages, max_tweets, fetch_limits, parallel_requests)
    return tweets


def main():
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Search tweets using TwitterAPI.io")
    parser.add_argument("topic", nargs="?", help="Topic name from config.toml")
    parser.add_argument("--config", default="config.toml", help="Path to config file")
    parser.add_argument("--max-pages", type=int, help="Maximum number of pages to fetch")
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

    tweets = search_topic(args.topic, config, api_key, args.max_pages)

    raw_dir = Path("raw")
    raw_dir.mkdir(exist_ok=True)
    output_path = raw_dir / f"{args.topic}.json"

    with open(output_path, "w") as f:
        json.dump(tweets, f, indent=2)
    print(f"Saved {len(tweets)} tweets to {output_path}")

    return 0


if __name__ == "__main__":
    exit(main())
