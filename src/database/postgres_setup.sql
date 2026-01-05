-- ecommerce_events
CREATE DATABASE ecommerce;
CREATE USER spark_user WITH PASSWORD 'spark_pass';
GRANT ALL PRIVILEGES ON DATABASE ecommerce TO spark_user;



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

-- streaming_logs
CREATE TABLE IF NOT EXISTS streaming_logs (
    batch_id BIGINT,
    rows_written INT,
    error_message TEXT,
    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
