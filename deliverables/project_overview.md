# Real-Time Data Ingestion Using Spark Structured Streaming & PostgreSQL

## Project Overview

This project implements a real-time data pipeline that simulates an e-commerce platform tracking user activity. The system continuously generates fake user events (product views and purchases), processes them using Apache Spark Structured Streaming, and stores the results in a PostgreSQL database.

## System Components

### 1. Data Generation Layer
**Component**: `data_generator.py`

This Python script continuously generates simulated e-commerce events and writes them as CSV files to a monitored directory. The generator creates realistic user activity including:
- Product views
- Purchase transactions with price, quantity, and currency information
- Unique event identifiers using UUIDs
- Timestamps for temporal tracking

The generator runs as a continuous service, producing batches of events at regular intervals with built-in retry logic for fault tolerance.

### 2. Streaming Processing Layer
**Component**: `spark_streaming_to_postgres.py`

The core processing component uses Apache Spark Structured Streaming to:
- Monitor the data directory for new CSV files
- Read incoming files automatically using schema enforcement
- Apply data transformations including type casting and timestamp conversion
- Add ingestion metadata for tracking
- Filter out invalid records

Spark processes data in micro-batches, ensuring efficient resource usage while maintaining near real-time latency.

### 3. Data Storage Layer
**Component**: PostgreSQL Database

A relational database stores processed events with the following structure:
- **ecommerce_events table**: Main storage for all processed events with indexed columns for fast querying
- **streaming_logs table**: Monitoring table that tracks batch processing metrics and errors
- **Views**: Pre-built aggregations for event statistics and performance monitoring

### 4. Infrastructure Layer
**Components**: Docker containers

The system uses containerized infrastructure:
- **PostgreSQL Container**: Isolated database instance with persistent storage
- **Spark Container**: Spark processing environment with necessary dependencies
- **Docker Compose**: Orchestrates multi-container deployment and networking

## Data Flow

1. The data generator creates CSV files with batches of events and writes them to `/data/raw`
2. Spark Structured Streaming detects new files automatically through directory monitoring
3. Each file is read, validated against the schema, and transformed
4. Processed records are written to PostgreSQL in micro-batches using JDBC
5. Batch metadata is logged to the streaming_logs table for monitoring
6. Application logs are written to `/logs` for debugging and auditing

## Key Features

- **Real-time processing**: Events are processed within seconds of generation
- **Fault tolerance**: Spark checkpointing ensures exactly-once processing semantics
- **Scalability**: The architecture can handle increased throughput by adjusting batch sizes and parallelism
- **Monitoring**: Comprehensive logging at both application and database levels
- **Data quality**: Schema enforcement and null filtering ensure clean data
- **Containerization**: Easy deployment and environment consistency

## Technology Stack

- **Apache Spark 3.x**: Distributed stream processing engine
- **PostgreSQL 13+**: Relational database for persistent storage
- **Python 3.8+**: Data generation and scripting
- **Docker & Docker Compose**: Containerization and orchestration
- **psycopg2**: PostgreSQL database adapter for Python
- **PySpark**: Python API for Apache Spark

## Performance Characteristics

The system is designed to handle:
- Continuous data ingestion at configurable rates
- Low latency processing (typically under 5 seconds end-to-end)
- Thousands of events per minute
- Automatic recovery from transient failures
- Efficient resource utilization through micro-batch processing

## Use Cases

This pipeline architecture is applicable to:
- E-commerce activity tracking
- Clickstream analytics
- IoT sensor data ingestion
- Financial transaction processing
- Log aggregation and monitoring
- Real-time recommendation systems