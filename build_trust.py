import json
import tomllib
import glob
import sys
from pathlib import Path
from collections import defaultdict
import csv


def load_config():
    config_path = Path(__file__).parent / "config.toml"
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def load_checkpoints(raw_dir: Path, category: str) -> dict:
    patterns = [
        str(raw_dir / f"{category}_tweets_checkpoint_*.json"),
        str(raw_dir / f"{category}_extended_tweets_checkpoint_*.json"),
    ]
    all_results = {}
    for pattern in patterns:
        files = sorted(glob.glob(pattern))
        for filepath in files:
            with open(filepath, "r") as f:
                data = json.load(f)
            all_results.update(data.get("results", {}))
    return all_results


def load_followings(raw_dir: Path, category: str) -> dict:
    followings_path = raw_dir / f"{category}_followings.json"
    if followings_path.exists():
        with open(followings_path, "r") as f:
            return json.load(f)
    return {}


def build_username_to_id_map(results: dict) -> dict:
    mapping = {}
    for username, tweets in results.items():
        for tweet in tweets:
            author = tweet.get("author", {})
            if author.get("userName") and author.get("id"):
                mapping[author["userName"].lower()] = author["id"]
            for mention in tweet.get("mentions", []):
                if mention.get("userName") and mention.get("id_str"):
                    mapping[mention["userName"].lower()] = mention["id_str"]
            retweeted = tweet.get("retweeted_tweet")
            if retweeted:
                rt_author = retweeted.get("author", {})
                if rt_author.get("userName") and rt_author.get("id"):
                    mapping[rt_author["userName"].lower()] = rt_author["id"]
            quoted = tweet.get("quoted_tweet")
            if quoted:
                qt_author = quoted.get("author", {})
                if qt_author.get("userName") and qt_author.get("id"):
                    mapping[qt_author["userName"].lower()] = qt_author["id"]
    return mapping


def build_trust_arcs(results: dict, followings: dict, weights: dict, username_to_id: dict) -> dict:
    trust = defaultdict(float)

    follow_weight = weights.get("follow", 0)
    mention_weight = weights.get("mention", 0)
    reply_weight = weights.get("reply", 0)
    retweet_weight = weights.get("retweet", 0)
    quote_weight = weights.get("quote", 0)



    for username, tweets in results.items():
        for tweet in tweets:
            author = tweet.get("author", {})
            user_i_id = author.get("id")
            if not user_i_id:
                continue

            for mention in tweet.get("mentions", []):
                user_j_id = mention.get("id_str")
                if user_j_id:
                    trust[(user_i_id, user_j_id)] += mention_weight

            if tweet.get("isReply") and tweet.get("inReplyToUsername"):
                user_j_id = username_to_id.get(tweet["inReplyToUsername"].lower())
                if user_j_id:
                    trust[(user_i_id, user_j_id)] += reply_weight

            retweeted = tweet.get("retweeted_tweet")
            if retweeted:
                rt_author = retweeted.get("author", {})
                user_j_id = rt_author.get("id")
                if user_j_id:
                    trust[(user_i_id, user_j_id)] += retweet_weight

            quoted = tweet.get("quoted_tweet")
            if quoted:
                qt_author = quoted.get("author", {})
                user_j_id = qt_author.get("id")
                if user_j_id:
                    trust[(user_i_id, user_j_id)] += quote_weight

    return trust


def save_seed_csv(categories: list, username_to_id: dict, seed_dir: Path, category: str):
    seed_dir.mkdir(parents=True, exist_ok=True)
    output_path = seed_dir / f"{category}.csv"

    seed_ids = []
    for peer in categories:
        username = peer.lstrip("@").lower()
        user_id = username_to_id.get(username)
        if user_id:
            seed_ids.append(user_id)

    if not seed_ids:
        print("No seed peers found with IDs")
        return

    seed_value = 1.0 / len(seed_ids)

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["i", "v"])
        for user_id in seed_ids:
            writer.writerow([user_id, seed_value])
    print(f"Saved {len(seed_ids)} seed peers to {output_path}")


def save_trust_csv(trust: dict, trust_dir: Path, category: str):
    trust_dir.mkdir(parents=True, exist_ok=True)
    output_path = trust_dir / f"{category}.csv"
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["i", "j", "v"])
        for (user_i, user_j), value in sorted(trust.items()):
            if value > 0:
                writer.writerow([user_i, user_j, value])
    print(f"Saved {len(trust)} arcs to {output_path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python build_trust.py <category>")
        sys.exit(1)

    category = sys.argv[1]
    config = load_config()
    weights = config.get("trust_weights", {})

    categories_list = config.get("categories", {}).get(category, [])

    raw_dir = Path(__file__).parent / "raw"
    trust_dir = Path(__file__).parent / "trust"
    seed_dir = Path(__file__).parent / "seed"

    print(f"Loading checkpoints for {category}...")
    results = load_checkpoints(raw_dir, category)
    print(f"Loaded {len(results)} users")

    print(f"Loading followings for {category}...")
    followings = load_followings(raw_dir, category)
    print(f"Loaded followings for {len(followings)} users")

    print("Building username to ID mapping...")
    username_to_id = build_username_to_id_map(results)
    print(f"Mapped {len(username_to_id)} usernames to IDs")

    print("Building trust arcs...")
    trust = build_trust_arcs(results, followings, weights, username_to_id)
    print(f"Built {len(trust)} unique arcs")

    save_trust_csv(trust, trust_dir, category)
    save_seed_csv(categories_list, username_to_id, seed_dir, category)


if __name__ == "__main__":
    main()
