CREATE TABLE IF NOT EXISTS ecommerce_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL,
    user_id INTEGER,
    product_id INTEGER,
    event_type VARCHAR(50),
    event_timestamp TIMESTAMP,
    price DOUBLE PRECISION,
    quantity INTEGER,
    currency VARCHAR(10),
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS streaming_logs (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER,
    rows_written INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_event_id ON ecommerce_events(event_id);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON ecommerce_events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON ecommerce_events(event_timestamp);
