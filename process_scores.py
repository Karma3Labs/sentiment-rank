import csv
import math
import glob
from pathlib import Path


def load_scores(scores_dir: Path) -> dict[str, list[tuple[str, float]]]:
    results = {}
    for filepath in glob.glob(str(scores_dir / "*.csv")):
        if "_processed" in filepath:
            continue
        category = Path(filepath).stem
        scores = []
        with open(filepath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                user_id = row["i"]
                value = float(row["v"])
                scores.append((user_id, value))
        results[category] = scores
    return results


def process_scores(scores: list[tuple[str, float]]) -> list[tuple[str, float]]:
    log_scores = []
    for user_id, value in scores:
        if value > 0:
            log_value = math.log2(value + 1)
            log_scores.append((user_id, log_value))
        else:
            log_scores.append((user_id, 0.0))

    if not log_scores:
        return log_scores

    min_val = min(v for _, v in log_scores)
    max_val = max(v for _, v in log_scores)

    if max_val == min_val:
        return [(user_id, 0.0) for user_id, _ in log_scores]

    normalized = []
    for user_id, value in log_scores:
        norm_value = 0.1 + 0.9 * (value - min_val) / (max_val - min_val)
        normalized.append((user_id, norm_value))

    return normalized


def save_scores(scores: list[tuple[str, float]], scores_dir: Path, category: str):
    output_path = scores_dir / f"{category}_processed.csv"
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["i", "v"])
        for user_id, value in scores:
            writer.writerow([user_id, value])
    print(f"Saved {len(scores)} scores to {output_path}")


def main():
    scores_dir = Path(__file__).parent / "scores"

    print("Loading scores...")
    all_scores = load_scores(scores_dir)
    print(f"Loaded {len(all_scores)} categories")

    for category, scores in all_scores.items():
        print(f"Processing {category} ({len(scores)} scores)...")
        processed = process_scores(scores)
        save_scores(processed, scores_dir, category)


if __name__ == "__main__":
    main()
