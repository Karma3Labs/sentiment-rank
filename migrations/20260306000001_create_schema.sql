-- Migration: 001_create_schema
-- Description: Create twitter_sentiment schema and types

CREATE SCHEMA IF NOT EXISTS twitter_sentiment;

CREATE TYPE twitter_sentiment.market AS (
    id INTEGER,
    name TEXT
);

CREATE TYPE twitter_sentiment.prediction AS (
    market_id INTEGER,
    probability REAL
);
