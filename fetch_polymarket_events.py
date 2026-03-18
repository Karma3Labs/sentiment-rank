import os
import sys
import json
import tomllib
import requests
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def load_config():
    config_path = Path(__file__).parent / "config.toml"
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def load_config():
    config_path = Path(__file__).parent / "config.toml"
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def generate_query_and_hashtags(title: str, markets: list[dict], api_key: str) -> tuple[str, list[str]]:
    market_names = [m["name"] for m in markets[:10]]
    markets_str = ", ".join(market_names)

    prompt = f"""You are generating Twitter search parameters for a prediction market.

Event title: {title}
Market options: {markets_str}

Generate:
1. A Twitter advanced search query optimized to find relevant tweets about this topic. Use OR operators, include common variations, abbreviations, symbols (like $BTC), and relevant keywords. Include lang:en -filter:retweets at the end.
2. A list of 3-5 relevant hashtags.

Respond with ONLY valid JSON in this exact format:
{{"query": "your query here", "hashtags": ["#Tag1", "#Tag2", "#Tag3"]}}

Response:"""

    retries = 0
    while retries < 3:
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
                    "max_tokens": 500,
                    "messages": [{"role": "user", "content": prompt}]
                },
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            text = result["content"][0]["text"].strip()
            data = json.loads(text)
            return data.get("query", ""), data.get("hashtags", [])
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"Rate limited, waiting 60s...")
                time.sleep(60)
                continue
            retries += 1
        except Exception as e:
            print(f"Error generating query: {e}")
            retries += 1

    return f"{title} lang:en -filter:retweets", []


def fetch_events_for_category(category: str, api_key: str, limit: int = 25) -> list[dict]:
    response = requests.get(
        "https://gamma-api.polymarket.com/events",
        params={
            "tag_slug": category,
            "order": "volume",
            "ascending": "false",
            "limit": limit,
            "active": "true",
            "closed": "false"
        },
        timeout=30
    )
    response.raise_for_status()
    events = response.json()

    config = load_config()
    max_markets = config.get("search", {}).get("max_markets", 10)

    result = []
    for i, event in enumerate(events):
        event_markets = event.get("markets", [])
        event_markets.sort(key=lambda m: float(m.get("volume", 0) or 0), reverse=True)
        event_markets = event_markets[:max_markets]

        markets = []
        for market in event_markets:
            markets.append({
                "name": market.get("groupItemTitle") or market.get("question"),
                "id": int(market.get("id")) if market.get("id") else None
            })

        # Convert binary events (single market) to Yes/No
        if len(markets) == 1:
            market_id = markets[0]["id"]
            markets = [
                {"name": "Yes", "id": market_id},
                {"name": "No", "id": market_id}
            ]

        slug = event.get("slug", "")
        title = event.get("title", "")

        print(f"  [{i+1}/{len(events)}] Generating query for: {title[:50]}...")
        query, hashtags = generate_query_and_hashtags(title, markets, api_key)

        result.append({
            "event_id": int(event.get("id")) if event.get("id") else None,
            "slug": slug,
            "description": title,
            "markets": markets,
            "query": query,
            "hashtags": hashtags
        })

    return result


def main():
    api_key = os.environ.get("CLAUDE_API_KEY")
    if not api_key:
        print("CLAUDE_API_KEY environment variable not set")
        sys.exit(1)

    config = load_config()
    categories = list(config.get("categories", {}).keys())

    if not categories:
        print("No categories found in config.toml")
        sys.exit(1)

    raw_dir = Path(__file__).parent / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    for category in categories:
        print(f"Fetching events for category: {category}")
        events = fetch_events_for_category(category, api_key)
        output_path = raw_dir / f"{category}_topics.json"
        with open(output_path, "w") as f:
            json.dump(events, f, indent=2)
        print(f"Saved {len(events)} events to {output_path}")


if __name__ == "__main__":
    main()
