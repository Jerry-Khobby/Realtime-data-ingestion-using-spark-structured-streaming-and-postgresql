# User Guide: Real-Time Data Ingestion Pipeline

## Prerequisites

Before running this project, ensure you have the following installed:

- Docker Engine (version 20.10 or higher)
- Docker Compose (version 1.29 or higher)
- Git (for cloning the repository)
- At least 4GB of available RAM
- 10GB of free disk space

## Project Structure

```
project-root/
├── docker/
│   ├── postgres/
│   │   └── Dockerfile
│   └── spark/
│       └── Dockerfile
├── data/
│   ├── raw/              (created automatically)
│   └── checkpoints/      (created automatically)
├── deliverables/
│   ├── project_overview.md
│   ├── user_guide.md
│   ├── test_cases.md
│   ├── performance_metrics.md
│   ├── system_architecture.png
│   └── postgres_setup.sql
├── etl/
│   ├── extract/
│   │   └── data_generation.py
│   └── transform_load/
│       └── spark_streaming_to_postgres.py
├── logs/                 (created automatically)
│   ├── data_generator.log
│   └── spark_streaming.log
├── docker-compose.yml
├── postgres_connection_details.txt
└── requirements.txt
```

## Step-by-Step Setup Instructions

### Step 1: Environment Configuration

Create a `.env` file in the project root directory with the following contents:

```
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce_streaming
POSTGRES_USER=spark_user
POSTGRES_PASSWORD=your_secure_password
```

Replace `your_secure_password` with a strong password of your choice.

### Step 2: Build Docker Containers

Navigate to the project root directory and build the Docker images:

```bash
docker-compose build
```

This process may take 5-10 minutes depending on your internet connection.

### Step 3: Initialize the Database

Start only the PostgreSQL container first:

```bash
docker-compose up -d postgres
```

Wait 10-15 seconds for PostgreSQL to initialize, then create the database schema:

```bash
docker exec -i <postgres_container_name> psql -U spark_user -d postgres < deliverables/postgres_setup.sql
```

To find your postgres container name, run:
```bash
docker ps
```

### Step 4: Start All Services

Start all containers in the background:

```bash
docker-compose up -d
```

Verify all containers are running:

```bash
docker-compose ps
```

You should see containers for PostgreSQL, Spark, and the data generator all in "Up" status.

### Step 5: Monitor the Pipeline

Check the data generator logs:

```bash
tail -f logs/data_generator.log
```

Check the Spark streaming logs:

```bash
tail -f logs/spark_streaming.log
```

View generated CSV files:

```bash
ls -lh data/raw/
```

### Step 6: Verify Data in PostgreSQL

Connect to the PostgreSQL database:

```bash
docker exec -it <postgres_container_name> psql -U spark_user -d ecommerce_streaming
```

Run verification queries:

```sql
-- Check total events ingested
SELECT COUNT(*) FROM ecommerce_events;

-- View recent events
SELECT * FROM ecommerce_events ORDER BY ingestion_time DESC LIMIT 10;

-- Check event statistics
SELECT * FROM event_statistics;

-- Monitor streaming performance
SELECT * FROM streaming_performance LIMIT 20;

-- View events by type
SELECT event_type, COUNT(*) FROM ecommerce_events GROUP BY event_type;
```

Exit psql with `\q`

## Common Operations

### Stopping the Pipeline

Stop all services:

```bash
docker-compose down
```

Stop and remove all data (including database):

```bash
docker-compose down -v
```

### Restarting After Failure

If a container fails, restart it:

```bash
docker-compose restart <service_name>
```

For example:
```bash
docker-compose restart spark
```

### Viewing Logs

View logs for a specific service:

```bash
docker-compose logs -f <service_name>
```

Example:
```bash
docker-compose logs -f spark
```

### Adjusting Generation Rate

Edit the `data_generation.py` file and modify these constants:

```python
EVENTS_PER_FILE = 10        # Number of events per CSV file
SLEEP_SECONDS = 3           # Seconds between file generation
```

After changes, rebuild and restart:

```bash
docker-compose build
docker-compose restart data-generator
```

### Clearing Old Data

To clear generated CSV files:

```bash
rm -f data/raw/*.csv
```

To clear Spark checkpoints (forces reprocessing):

```bash
rm -rf data/checkpoints/*
```

To truncate database tables:

```sql
TRUNCATE TABLE ecommerce_events;
TRUNCATE TABLE streaming_logs;
```

## Troubleshooting

### Issue: PostgreSQL Connection Failed

**Symptoms**: Spark logs show connection errors

**Solution**:
1. Verify PostgreSQL is running: `docker-compose ps postgres`
2. Check credentials in `.env` file match `postgres_connection_details.txt`
3. Ensure PostgreSQL container has fully started (wait 15-20 seconds)
4. Test connection manually: `docker exec -it <postgres_container> psql -U spark_user -d ecommerce_streaming`

### Issue: No Files Being Generated

**Symptoms**: `data/raw/` directory is empty

**Solution**:
1. Check data generator logs: `docker-compose logs data-generator`
2. Verify the container is running: `docker-compose ps`
3. Check directory permissions: `ls -la data/`
4. Restart the generator: `docker-compose restart data-generator`

### Issue: Spark Not Processing Files

**Symptoms**: Files accumulate in `data/raw/` but database remains empty

**Solution**:
1. Check Spark logs: `docker-compose logs spark`
2. Verify schema matches the CSV structure
3. Check for checkpoint corruption: clear `data/checkpoints/`
4. Ensure PostgreSQL table exists: run `postgres_setup.sql` again
5. Restart Spark: `docker-compose restart spark`

### Issue: High Memory Usage

**Symptoms**: System becomes slow or containers crash

**Solution**:
1. Reduce batch size in `data_generation.py` (lower `EVENTS_PER_FILE`)
2. Increase sleep interval between generations
3. Add memory limits to `docker-compose.yml`
4. Clear old checkpoint data regularly

### Issue: Disk Space Running Low

**Symptoms**: Containers fail to write files

**Solution**:
1. Archive or delete old CSV files from `data/raw/`
2. Truncate database tables if data is no longer needed
3. Clean Docker system: `docker system prune -a`
4. Remove old logs: `truncate -s 0 logs/*.log`

## Performance Tuning

### For Higher Throughput

1. Increase `EVENTS_PER_FILE` in data_generation.py
2. Decrease `SLEEP_SECONDS` for faster generation
3. Add more Spark executor memory in docker-compose.yml
4. Increase PostgreSQL connection pool size

### For Lower Latency

1. Reduce Spark trigger interval (add `.trigger(processingTime='1 second')`)
2. Use smaller batch sizes
3. Optimize PostgreSQL indexes
4. Enable write-ahead logging in PostgreSQL

## Shutdown Procedure

To gracefully shut down the pipeline:

1. Stop the data generator first:
   ```bash
   docker-compose stop data-generator
   ```

2. Wait for Spark to process remaining files (check logs)

3. Stop Spark:
   ```bash
   docker-compose stop spark
   ```

4. Stop PostgreSQL:
   ```bash
   docker-compose stop postgres
   ```

5. Or stop everything at once:
   ```bash
   docker-compose down
   ```

## Getting Help

If you encounter issues not covered in this guide:

1. Check all log files in the `logs/` directory
2. Review Docker container logs: `docker-compose logs`
3. Verify environment variables are set correctly
4. Ensure all prerequisites are installed and up to date
5. Try a clean restart: `docker-compose down -v && docker-compose up -d`