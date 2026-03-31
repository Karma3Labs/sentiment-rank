import os
import sys
import json
import glob
import tomllib
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"


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


def load_config(config_path: str = "config.toml") -> dict:
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def load_topics_from_raw() -> list[dict]:
    raw_dir = Path("raw")
    topic_files = glob.glob(str(raw_dir / "*_topics.json"))

    topics = []
    for topic_file in topic_files:
        category = os.path.basename(topic_file).replace("_topics.json", "")
        with open(topic_file, "r") as f:
            for topic in json.load(f):
                topic["category"] = category
                topics.append(topic)

    return topics


def get_topic(topic_slug: str) -> dict | None:
    topics = load_topics_from_raw()
    for topic in topics:
        if topic.get("slug") == topic_slug:
            return topic
    return None


def list_topics() -> list[str]:
    topics = load_topics_from_raw()
    return [topic["slug"] for topic in topics]


def search_tweets_page(query: str, api_key: str, query_type: str = "Latest", cursor: str = "") -> dict:
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


def search_tweets(
    topic: dict,
    twitter_api_key: str,
    config: dict,
) -> list[dict]:
    query = topic["query"]
    search_config = config.get("search", {})
    query_type = search_config.get("query_type", "Top")
    max_pages = search_config.get("max_pages", 20)

    look_back = config.get("look_back", {})
    year = look_back.get("year", 2026)
    month = look_back.get("month", 1)
    cutoff_date = datetime(year, month, 1)
    print(f"Cutoff date: {cutoff_date.strftime('%Y-%m-%d')}")

    all_tweets = []
    seen_ids = set()
    cursor = ""
    page_count = 0

    while page_count < max_pages:
        page_count += 1
        print(f"Fetching page {page_count}/{max_pages}...")

        try:
            result = search_tweets_page(query, twitter_api_key, query_type, cursor)
        except Exception as e:
            print(f"Error fetching page: {e}")
            break

        raw_tweets = result.get("tweets", [])
        print(f"  Got {len(raw_tweets)} tweets")

        if not raw_tweets:
            break

        reached_cutoff = False
        for tweet in raw_tweets:
            normalized = normalize_tweet(tweet)
            if normalized["id"] not in seen_ids:
                seen_ids.add(normalized["id"])
                try:
                    tweet_date = datetime.strptime(normalized["createdAt"], "%a %b %d %H:%M:%S %z %Y")
                    if tweet_date.replace(tzinfo=None) < cutoff_date:
                        print(f"  Reached cutoff date, stopping search")
                        reached_cutoff = True
                        break
                except (ValueError, TypeError):
                    pass
                all_tweets.append(normalized)

        if reached_cutoff:
            break

        if not result.get("has_next_page"):
            break

        cursor = result.get("next_cursor", "")
        if not cursor:
            break

    return all_tweets


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Search tweets")
    parser.add_argument("topic", nargs="?", help="Topic slug from raw/*_topics.json")
    parser.add_argument("--config", default="config.toml", help="Path to config file")
    parser.add_argument("--list", action="store_true", help="List available topics")
    args = parser.parse_args()

    config = load_config(args.config)

    if args.list:
        print("Available topics:")
        for topic in load_topics_from_raw():
            print(f"  {topic['slug']}: {topic.get('description', '')}")
        return 0

    if not args.topic:
        parser.error("topic is required (use --list to see available topics)")

    twitter_api_key = os.environ.get("TWITTER_API_KEY")
    if not twitter_api_key:
        print("Error: TWITTER_API_KEY environment variable not set")
        return 1

    topic = get_topic(args.topic)
    if not topic:
        available = list_topics()
        print(f"Topic '{args.topic}' not found. Available: {available}")
        return 1

    start_time = datetime.now()
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Topic: {args.topic}")
    print(f"Description: {topic.get('description')}")

    tweets = search_tweets(topic, twitter_api_key, config)

    raw_dir = Path("raw")
    raw_dir.mkdir(exist_ok=True)

    tweets_path = raw_dir / f"{args.topic}.json"
    with open(tweets_path, "w") as f:
        json.dump(tweets, f, indent=2)
    print(f"Saved {len(tweets)} tweets to {tweets_path}")

    elapsed = datetime.now() - start_time
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total elapsed: {elapsed}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
