import os
import tomllib
import requests
import json
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def load_config():
    config_path = Path(__file__).parent / "config.toml"
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def get_followings(username: str, api_key: str, cursor: str = "") -> dict:
    url = "https://api.twitterapi.io/twitter/user/followings"
    headers = {"X-API-Key": api_key}
    params = {"userName": username.lstrip("@"), "pageSize": 200}
    if cursor:
        params["cursor"] = cursor
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def fetch_all_followings(username: str, api_key: str) -> list[dict]:
    all_followings = []
    cursor = ""
    page = 1
    while True:
        data = get_followings(username, api_key, cursor)
        followings = data.get("followings", [])
        all_followings.extend(followings)
        print(f"    Page {page}: {len(all_followings)} followings fetched", end="\r")
        if not data.get("has_next_page"):
            break
        cursor = data.get("next_cursor", "")
        page += 1
        time.sleep(0.5)
    print()
    return all_followings


def main():
    api_key = os.environ.get("TWITTER_API_KEY")
    if not api_key:
        raise ValueError("TWITTER_API_KEY environment variable not set")

    config = load_config()
    category = os.environ.get("CATEGORY", "crypto")
    seed_peers = config.get("seed_peers", {}).get(category, [])

    if not seed_peers:
        print(f"No seed peers found for category: {category}")
        return

    results = {}
    total = len(seed_peers)
    for i, peer in enumerate(seed_peers, 1):
        username = peer.lstrip("@")
        print(f"[{i}/{total}] Fetching followings for {username}...")
        try:
            followings = fetch_all_followings(username, api_key)
            results[username] = [f.get("userName") for f in followings]
            print(f"  Found {len(followings)} followings")
        except Exception as e:
            print(f"  Error: {e}")
            results[username] = []

    output_path = Path(__file__).parent / "raw" / f"{category}_followings.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {output_path}")


if __name__ == "__main__":
    main()
