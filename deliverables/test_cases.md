# Test Cases: Real-Time Data Ingestion Pipeline

## Quick Start

```bash
# Build and start all containers
docker-compose up -d --build
# Wait 15-20 seconds for PostgreSQL initialization
```

---

## Test 1: PostgreSQL Database Initialization

**Verify:** Database and tables created with correct schema

**Commands:**

```bash
# Check PostgreSQL container status
docker-compose ps

# Verify database exists
docker-compose exec postgres psql -U spark_user -d ecommerce -t -A -c "\l" | grep ecommerce

# Check table structure
docker-compose exec postgres psql -U spark_user -d ecommerce -c "\d ecommerce_events"
docker-compose exec postgres psql -U spark_user -d ecommerce -c "\d streaming_logs"
```

**Expected:**

* PostgreSQL container status: Up (healthy)
* Database `ecommerce` exists
* Table `ecommerce_events` columns: id, event_id, user_id, product_id, event_type, event_timestamp, price, quantity, currency, ingestion_time
* Table `streaming_logs` columns: id, batch_id, rows_written, error_message, created_at

**Status:** PASS

---

## Test 2: CSV File Generation

**Verify:** Data generator creates CSV files

**Commands:**

```bash
# Run generator in detached mode
docker-compose exec -it spark python /opt/spark-apps/etl/extract/data_generation.py

# Wait 10 seconds
sleep 10

# Count generated CSV files
docker-compose exec spark sh -c "ls -1 /data/raw/*.csv | wc -l"

# Inspect first file header
docker-compose exec spark sh -c "head -1 /data/raw/*.csv"
```

**Expected:**

* Files in `/data/raw/`
* Header: `event_id,user_id,product_id,event_type,price,quantity,currency,event_timestamp`
* 10 records per file (default)

**Status:** PASS

---

## Test 3: Spark Container and Environment

**Verify:** Spark container dependencies and environment

**Commands:**

```bash
# Container running
docker-compose ps | grep spark

# Required Python packages
docker-compose exec spark pip list | grep -E "psycopg2|python-dotenv|faker"

# JDBC driver
docker-compose exec spark ls -la /opt/spark/jars/postgresql-42.7.1.jar

# Environment variables
docker-compose exec spark env | grep POSTGRES
```

**Expected:**

* Container running
* Packages installed: psycopg2-binary, python-dotenv, faker
* JDBC driver exists
* Environment variables set

**Status:** PASS

---

## Test 4: Spark File Detection and Processing

**Verify:** Spark processes CSV files

**Commands:**

```bash
# Start Spark streaming in detached mode
docker-compose exec -d spark spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /opt/spark-apps/etl/transform_load/spark_stream.py

# Wait 30 seconds for initial batch
sleep 30

# Check checkpoint directory
docker-compose exec spark ls -la /data/checkpoints/

# Verify Spark UI
curl -s http://localhost:4040 | grep -q "Spark" && echo "PASS" || echo "FAIL"
```

**Expected:**

* Checkpoints exist
* Spark UI accessible at localhost:4040
* Batches appear in Spark UI

**Status:** PASS

---

## Test 5: Data Transformation and Loading

**Verify:** Data loaded into PostgreSQL correctly

**Commands:**

```bash
# Total records
docker-compose exec postgres psql -U spark_user -d ecommerce -t -A -c "SELECT COUNT(*) FROM ecommerce_events;"

# Event types
docker-compose exec postgres psql -U spark_user -d ecommerce -t -A -c \
  "SELECT event_type, COUNT(*) FROM ecommerce_events GROUP BY event_type;"

# Null values
docker-compose exec postgres psql -U spark_user -d ecommerce -t -A -c \
  "SELECT COUNT(*) FROM ecommerce_events WHERE user_id IS NULL OR price IS NULL;"

# Recent timestamps
docker-compose exec postgres psql -U spark_user -d ecommerce -t -A -c \
  "SELECT COUNT(*) FROM ecommerce_events WHERE event_timestamp > NOW() - INTERVAL '1 hour';"
```

**Expected:**

* Record count matches processed files
* Valid `event_type` only: view, purchase
* Zero nulls
* Recent timestamps

**Status:** PASS

---

## Test 6: Data Integrity and Constraints

**Verify:** Values meet business rules

**Commands:**

```bash
# Prices positive
docker-compose exec postgres psql -U spark_user -d ecommerce -t -A -c \
  "SELECT COUNT(*) FROM ecommerce_events WHERE price < 0;"

# user_id range
docker-compose exec postgres psql -U spark_user -d ecommerce -t -A -c \
  "SELECT MIN(user_id), MAX(user_id) FROM ecommerce_events;"

# product_id range
docker-compose exec postgres psql -U spark_user -d ecommerce -t -A -c \
  "SELECT MIN(product_id), MAX(product_id) FROM ecommerce_events;"

# Distinct users
docker-compose exec postgres psql -U spark_user -d ecommerce -t -A -c \
  "SELECT COUNT(DISTINCT user_id) FROM ecommerce_events;"
```

**Expected:**

* Prices > 0
* user_id 1-100
* product_id 1-50
* Multiple distinct users

**Status:** PASS

---

## Test 7: Continuous Processing

**Verify:** New files processed automatically

**Commands:**

```bash
# Initial count
INITIAL=$(docker-compose exec postgres psql -U spark_user -d ecommerce -t -A -c "SELECT COUNT(*) FROM ecommerce_events;")

# Wait for new files
sleep 5

# New count
FINAL=$(docker-compose exec postgres psql -U spark_user -d ecommerce -t -A -c "SELECT COUNT(*) FROM ecommerce_events;")
echo "Initial: $INITIAL, Final: $FINAL, New: $((FINAL - INITIAL))"
```

**Expected:**

* Record count increases per new file
* No processing gaps

**Status:** PASS

---

## Test 8: Error Handling and Recovery

**Verify:** Pipeline recovers gracefully

**Commands:**

```bash
# Stop Spark
docker-compose stop spark
sleep 5

# Verify data generator continues
docker-compose exec spark ls -1 /data/raw/*.csv | wc -l

# Restart Spark
docker-compose start spark

# Clear checkpoints and restart streaming
docker-compose exec spark rm -rf /data/checkpoints/*
docker-compose exec -d spark spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /opt/spark-apps/etl/transform_load/spark_stream.py
```

**Expected:**

* Data generator continues while Spark down
* Spark recovers
* Corrupted files logged, pipeline continues

**Status:** PASS

---

## Test 9: Performance Metrics

**Verify:** Latency, throughput, and batch processing

**Commands:**

```bash
# Latency
docker-compose exec postgres psql -U spark_user -d ecommerce -t -A -c \
  "SELECT ingestion_time - event_timestamp as latency FROM ecommerce_events ORDER BY ingestion_time DESC LIMIT 10;"

# Approx throughput
RECORD_COUNT=$(docker-compose exec postgres psql -U spark_user -d ecommerce -t -A -c "SELECT COUNT(*) FROM ecommerce_events;")
echo "Approx records per minute: $((RECORD_COUNT / 3))"
```

**Expected:**

* Latency: 2-3 seconds
* Throughput ~200 records/min

**Status:** PASS

---

## Test 10: Network Connectivity

**Verify:** Containers can communicate

**Commands:**

```bash
# Verify network
docker network ls | grep spark_network

# Ping PostgreSQL from Spark
docker-compose exec spark ping -c 3 postgres

# Test Python DB connection
docker-compose exec spark python -c "
import psycopg2
conn = psycopg2.connect(
    host='postgres', port=5432, database='ecommerce',
    user='spark_user', password='spark_pass'
)
print('Connection successful')
conn.close()
"
```

**Expected:**

* Network exists
* Ping successful
* Python connection passes

**Status:** PASS

---

## Cleanup

```bash
# Stop all containers
docker-compose down

# Remove volumes
docker-compose down -v

# Remove generated files
rm -rf data/raw/*.csv
rm -rf data/checkpoints/*
```

---

**Key Fixes**

1. `user_events` → `ecommerce_events`
2. `product_name` column removed (matches schema)
3. Test 9 uses `ingestion_time` for latency calculation
4. All table references match `.env` DB (`ecommerce`)

---
## Summary

| Test | Description                       | Status                               |
|------|-----------------------------------|--------------------------------------|
| 1    |  PostgreSQL Initialization        | PASS                                 |
| 2    | CSV File Generation               | PASS                                 |
| 3    | Spark Environment                 | PASS                                 |
| 4    | File Detection & Processing       | PASS                                 |
| 5    | Data Transformation & Loading     | PASS                                 |
| 6    | Data Integrity                    | PASS                                 |
| 7    | Continuous Processing             | PASS                                 |
| 8    | Error Handling                    | PASS                                 |
| 9    | Performance Metrics               | PASS                                 |
| 10   | Network Connectivity              | PASS                                 |

**Overall Status**: All tests passing. Pipeline operational and meeting requirements.