import json
from pathlib import Path

input_path = Path(__file__).parent / "raw" / "crypto_tweets_checkpoint_280_320.json"
output_path = Path(__file__).parent / "raw" / "crypto_tweets_checkpoint_280_320_processed.json"

with open(input_path, "r") as f:
    data = json.load(f)

results = data.get("results", {})
processed = data.get("processed", [])

empty_keys = [key for key, value in results.items() if value == []]

for key in empty_keys:
    del results[key]

processed = [p for p in processed if p not in empty_keys]

new_data = {
    "results": results,
    "processed": processed
}

with open(output_path, "w") as f:
    json.dump(new_data, f)

print(f"Removed {len(empty_keys)} empty entries")
print(f"Results: {len(results)}")
print(f"Processed: {len(processed)}")
print(f"Saved to {output_path}")
