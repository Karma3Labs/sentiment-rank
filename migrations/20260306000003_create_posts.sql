-- Migration: 003_create_posts
-- Description: Create posts table

CREATE TABLE IF NOT EXISTS twitter_sentiment.posts (
    id SERIAL PRIMARY KEY,
    event_id INTEGER NOT NULL REFERENCES twitter_sentiment.events(event_id),
    run_id INTEGER NOT NULL,
    post_id VARCHAR(255) NOT NULL,
    text TEXT NOT NULL,
    posted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    like_count INTEGER NOT NULL DEFAULT 0,
    retweet_count INTEGER NOT NULL DEFAULT 0,
    reply_count INTEGER NOT NULL DEFAULT 0,
    quote_count INTEGER NOT NULL DEFAULT 0,
    author_id VARCHAR(255) NOT NULL,
    author_username VARCHAR(255) NOT NULL,
    author_name VARCHAR(255) NOT NULL,
    author_followers INTEGER NOT NULL DEFAULT 0,
    author_is_verified BOOLEAN NOT NULL DEFAULT FALSE,
    relevancy_score REAL,
    weight REAL,
    predictions twitter_sentiment.prediction[],
    UNIQUE(event_id, run_id, post_id)
);

CREATE INDEX IF NOT EXISTS idx_posts_event_id ON twitter_sentiment.posts(event_id);
CREATE INDEX IF NOT EXISTS idx_posts_run_id ON twitter_sentiment.posts(event_id, run_id);
CREATE INDEX IF NOT EXISTS idx_posts_post_id ON twitter_sentiment.posts(post_id);
CREATE INDEX IF NOT EXISTS idx_posts_author_id ON twitter_sentiment.posts(author_id);
