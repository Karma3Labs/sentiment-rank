CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT NOT NULL,
    outcomes TEXT[] NOT NULL,
    query TEXT NOT NULL,
    hashtags TEXT[],
    category VARCHAR(100) NOT NULL,
    market_ids INTEGER[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_name ON events(name);
CREATE INDEX IF NOT EXISTS idx_events_category ON events(category);
