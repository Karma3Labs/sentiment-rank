#!/bin/bash

set -e

if [ -z "$1" ]; then
    echo "Usage: ./run_topic.sh <topic_name>"
    echo "Example: ./run_topic.sh ipos_2027"
    exit 1
fi

TOPIC=$1
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "Running pipeline for topic: $TOPIC"
echo "========================================"

echo ""
echo "[1/6] Searching local tweets..."
python search_local_tweets.py "$TOPIC"

echo ""
echo "[2/6] Predicting relevancy..."
python predict_relevancy.py "$TOPIC"

echo ""
echo "[3/6] Predicting sentiment..."
python predict_sentiment.py "$TOPIC"

echo ""
echo "[4/6] Weighting posts..."
python weight_posts.py "$TOPIC"

echo ""
echo "[5/6] Inserting topics..."
python insert_topics.py

echo ""
echo "[6/6] Inserting posts..."
python insert_posts.py

echo ""
echo "========================================"
echo "Pipeline complete for topic: $TOPIC"
echo "========================================"
