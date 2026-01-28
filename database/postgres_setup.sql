-- Drop existing tables if they exist (optional, for a clean start)
DROP TABLE IF EXISTS ecommerce_events CASCADE;
DROP TABLE IF EXISTS streaming_logs CASCADE;

-- Table to store ecommerce streaming events
CREATE TABLE ecommerce_events (
    id SERIAL PRIMARY KEY,                  -- Surrogate key
    event_id VARCHAR(255) NOT NULL UNIQUE,  -- Unique key for ON CONFLICT
    user_id INTEGER,
    product_id INTEGER,
    event_type VARCHAR(50),
    event_timestamp TIMESTAMP,
    price DOUBLE PRECISION,
    quantity INTEGER,
    currency VARCHAR(10),
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to log batch processing results
CREATE TABLE streaming_logs (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER,
    rows_written INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for faster queries
CREATE INDEX idx_events_user_id ON ecommerce_events(user_id);
CREATE INDEX idx_events_timestamp ON ecommerce_events(event_timestamp);


--docker-compose exec postgres psql -U spark_user -d ecommerce -c "ALTER TABLE ecommerce_events ADD CONSTRAINT ecommerce_events_event_id_key UNIQUE (event_id);"
--docker-compose exec postgres psql -U spark_user -d ecommerce -c "\d ecommerce_events"

