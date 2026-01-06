-- Create user safely
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_roles WHERE rolname = 'spark_user'
    ) THEN
        CREATE USER spark_user WITH PASSWORD 'spark_pass';
    END IF;
END
$$;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE ecommerce TO spark_user;
GRANT USAGE, CREATE ON SCHEMA public TO spark_user;

-- Tables
CREATE TABLE IF NOT EXISTS ecommerce_events (
    event_id UUID PRIMARY KEY,
    user_id INT,
    product_id INT,
    event_type VARCHAR(20),
    event_timestamp TIMESTAMP,
    price NUMERIC(10,2),
    quantity INT,
    currency VARCHAR(5),
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS streaming_logs (
    batch_id BIGINT,
    rows_written INT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
