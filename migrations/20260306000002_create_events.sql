-- Migration: 002_create_events
-- Description: Create events table

CREATE TABLE IF NOT EXISTS twitter_sentiment.events (
    event_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT NOT NULL,
    markets twitter_sentiment.market[] NOT NULL,
    query TEXT NOT NULL,
    hashtags TEXT[],
    category VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_name ON twitter_sentiment.events(name);
CREATE INDEX IF NOT EXISTS idx_events_category ON twitter_sentiment.events(category);
