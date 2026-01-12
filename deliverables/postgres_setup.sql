
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_roles WHERE rolname = 'spark_user'
    ) THEN
        CREATE USER spark_user WITH PASSWORD 'spark_pass';
    END IF;
END
$$;

-- Grant database privileges
GRANT ALL PRIVILEGES ON DATABASE ecommerce TO spark_user;

-- Connect to the database and grant schema privileges
\c ecommerce

-- Grant schema privileges
GRANT ALL PRIVILEGES ON SCHEMA public TO spark_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO spark_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO spark_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO spark_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO spark_user;

-- Create tables as spark_user
SET ROLE spark_user;

-- Tables
CREATE TABLE IF NOT EXISTS ecommerce_events (
    event_id VARCHAR(36) PRIMARY KEY,
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
    id SERIAL PRIMARY KEY,
    batch_id BIGINT,
    rows_written INT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_event_type ON ecommerce_events(event_type);
CREATE INDEX IF NOT EXISTS idx_event_timestamp ON ecommerce_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_user_id ON ecommerce_events(user_id);
CREATE INDEX IF NOT EXISTS idx_ingestion_time ON ecommerce_events(ingestion_time);

-- Reset to default user
RESET ROLE;

-- Grant ownership to spark_user for all created objects
ALTER TABLE ecommerce_events OWNER TO spark_user;
ALTER TABLE streaming_logs OWNER TO spark_user;
ALTER SEQUENCE streaming_logs_id_seq OWNER TO spark_user;