# Sentiment Rank

Aggregates social media sentiment to generate probability estimates for prediction market outcomes.

## Problem

Prediction markets allow trading on future events (elections, sports, crypto prices, etc.). Social media contains valuable signals about these events - people share opinions, analysis, and predictions. This project:

1. Collects relevant social media posts for prediction market events
2. Filters posts by relevancy using LLMs (OpenAI + Claude)
3. Extracts probability estimates from each post's sentiment
4. Weights posts by author credibility and engagement
5. Aggregates into final probability distributions for each market outcome

The goal is to turn unstructured social media discussion into structured probability estimates that can inform trading decisions or be compared against market prices.

## Setup

```bash
pip install -r requirements.txt
```

## Environment Variables

```
TWITTER_API_KEY=
OPENAI_API_KEY=
CLAUDE_API_KEY=
DATABASE_URL=
```

## Configuration

Edit `config.toml`:

```toml
[search]
query_type = "Top"  # "Latest" or "Top"
max_pages = 10
parallel_requests = 10

[look_back]
year = 2026
month = 1
```

## Usage

### Run full pipeline for all topics

```bash
python run_topics.py
```

This runs for each topic:
1. `search_tweets.py` - Search tweets
2. `predict_relevancy.py` - Score relevancy using OpenAI + Claude
3. `predict_sentiment.py` - Predict outcome probabilities (posts with relevancy > 0.5)
4. `weight_posts.py` - Calculate post weights

Then inserts results to database.

### Run individual scripts

```bash
# Search tweets for a topic
python search_tweets.py <topic_slug>

# Predict relevancy
python predict_relevancy.py <topic_slug>

# Predict sentiment
python predict_sentiment.py <topic_slug>

# Weight posts
python weight_posts.py <topic_slug>

# Insert to database
python insert_topics.py
python insert_posts.py
```

## Topics

Topics are defined in `raw/*_topics.json` files. Each topic has:
- `slug` - Unique identifier
- `description` - Event description
- `markets` - Available outcomes
- `query` - Twitter search query