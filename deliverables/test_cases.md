# Test Cases: Real-Time Data Ingestion Pipeline

## Test Environment Setup

Before running tests, ensure:
- All Docker containers are running
- PostgreSQL database is initialized with schema
- Data generator is active
- Spark streaming job is running
- At least 5 minutes of runtime for meaningful data

## Test Case 1: Data Generation Validation

### Objective
Verify that the data generator creates valid CSV files with correct structure and content.

### Prerequisites
- Data generator container is running
- Write permissions on `/data/raw` directory

### Test Steps
1. Wait for 30 seconds after starting the generator
2. Navigate to `data/raw/` directory
3. List all CSV files: `ls -lh data/raw/`
4. Open the most recent CSV file
5. Verify the header row contains all required columns
6. Check that data rows are populated
7. Verify event_id values are unique UUIDs
8. Confirm timestamps are in ISO format
9. Check that purchase events have price, quantity, and currency
10. Verify view events have empty price, quantity, and currency fields

### Expected Results
- CSV files appear every 3 seconds (default interval)
- Each file contains exactly 10 events (default batch size)
- Header row: `event_id,user_id,product_id,event_type,event_timestamp,price,quantity,currency`
- event_id format: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
- user_id range: 1-100
- product_id range: 1000-1100
- event_type values: "view" or "purchase"
- event_timestamp format: `2026-01-12T10:30:45.123456`
- purchase events: price (10.00-500.00), quantity (1-3), currency "USD"
- view events: empty price, quantity, currency fields

### Actual Results
Pass/Fail: _______
Notes: _______________________

---

## Test Case 2: Spark File Detection

### Objective
Confirm that Spark Structured Streaming detects and processes new CSV files.

### Prerequisites
- Spark container is running
- At least 3 CSV files generated in `/data/raw`

### Test Steps
1. Monitor Spark logs: `tail -f logs/spark_streaming.log`
2. Note the current file count in `/data/raw`
3. Wait for a new CSV file to be generated
4. Check Spark logs for file detection messages
5. Verify batch processing log entries appear
6. Check that batch_id increments sequentially

### Expected Results
- Spark logs show: "Processing batch X"
- Logs indicate "Found Y rows" for each batch
- Messages appear within 5 seconds of file creation
- No error messages in logs
- Batch IDs increment: 0, 1, 2, 3...
- Each batch corresponds to one CSV file (10 rows per batch)

### Actual Results
Pass/Fail: _______
Batch IDs observed: _______
Average detection latency: _______ seconds
Notes: _______________________

---

## Test Case 3: Data Transformation Validation

### Objective
Verify that Spark correctly transforms and cleans the data.

### Prerequisites
- At least 50 events processed
- Access to PostgreSQL database

### Test Steps
1. Connect to PostgreSQL: `psql -U spark_user -d ecommerce_streaming`
2. Query raw data: `SELECT * FROM ecommerce_events LIMIT 10;`
3. Check event_timestamp column data type: `\d ecommerce_events`
4. Verify ingestion_time is populated
5. Confirm ingestion_time is later than event_timestamp
6. Check for null event_id values: `SELECT COUNT(*) FROM ecommerce_events WHERE event_id IS NULL;`
7. Verify data type conversions are correct

### Expected Results
- event_timestamp column type: `timestamp without time zone`
- ingestion_time is populated for all rows
- ingestion_time > event_timestamp for all records
- Zero null event_id values (filtered out)
- user_id and product_id are integers
- price is double precision
- All timestamps are valid and parseable

### Actual Results
Pass/Fail: _______
Null event_id count: _______
Data type mismatches: _______
Notes: _______________________

---

## Test Case 4: PostgreSQL Data Insertion

### Objective
Ensure that processed data is correctly written to PostgreSQL without errors.

### Prerequisites
- Pipeline running for at least 2 minutes
- Multiple batches processed

### Test Steps
1. Connect to PostgreSQL
2. Count total events: `SELECT COUNT(*) FROM ecommerce_events;`
3. Wait 30 seconds
4. Count again and calculate increase
5. Check for duplicate event_ids: `SELECT event_id, COUNT(*) FROM ecommerce_events GROUP BY event_id HAVING COUNT(*) > 1;`
6. Verify streaming logs: `SELECT * FROM streaming_logs ORDER BY batch_id DESC LIMIT 10;`
7. Check for error messages: `SELECT * FROM streaming_logs WHERE error_message IS NOT NULL;`
8. Verify row counts match: compare streaming_logs.rows_written sum with ecommerce_events count

### Expected Results
- Event count increases over time
- Approximately 10 new events every 3 seconds
- Zero duplicate event_ids
- streaming_logs shows successful batches
- error_message column is NULL for all entries
- Sum of rows_written equals total event count
- No foreign key or constraint violations

### Actual Results
Pass/Fail: _______
Initial count: _______
Final count: _______
Events added: _______
Duplicates found: _______
Errors in logs: _______
Notes: _______________________

---

## Test Case 5: Event Type Distribution

### Objective
Validate that both "view" and "purchase" events are generated and stored correctly.

### Prerequisites
- At least 100 events in database

### Test Steps
1. Connect to PostgreSQL
2. Query event distribution:
   ```sql
   SELECT event_type, COUNT(*) as count, 
          COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
   FROM ecommerce_events 
   GROUP BY event_type;
   ```
3. Query purchase events with missing price: `SELECT COUNT(*) FROM ecommerce_events WHERE event_type = 'purchase' AND price IS NULL;`
4. Query view events with price: `SELECT COUNT(*) FROM ecommerce_events WHERE event_type = 'view' AND price IS NOT NULL;`
5. Verify currency is set only for purchases: `SELECT COUNT(*) FROM ecommerce_events WHERE event_type = 'view' AND currency IS NOT NULL;`

### Expected Results
- Both "view" and "purchase" events present
- Roughly 50% split between event types (random generation)
- Zero purchase events with NULL price
- Zero view events with non-NULL price
- Zero view events with non-NULL currency
- All purchase events have currency = "USD"

### Actual Results
Pass/Fail: _______
View events: _______ (___%)
Purchase events: _______ (___%)
Purchase without price: _______
View with price: _______
Notes: _______________________

---

## Test Case 6: Data Integrity and Constraints

### Objective
Verify that database constraints and data quality rules are enforced.

### Prerequisites
- Database schema created with all constraints
- At least 50 events in database

### Test Steps
1. Verify unique constraint on event_id:
   ```sql
   SELECT COUNT(DISTINCT event_id) as unique_ids, COUNT(*) as total_rows 
   FROM ecommerce_events;
   ```
2. Check for NULL violations:
   ```sql
   SELECT 
     COUNT(*) FILTER (WHERE event_id IS NULL) as null_event_id,
     COUNT(*) FILTER (WHERE user_id IS NULL) as null_user_id,
     COUNT(*) FILTER (WHERE event_timestamp IS NULL) as null_timestamp
   FROM ecommerce_events;
   ```
3. Validate value ranges:
   ```sql
   SELECT 
     MIN(user_id) as min_user, MAX(user_id) as max_user,
     MIN(product_id) as min_product, MAX(product_id) as max_product,
     MIN(price) as min_price, MAX(price) as max_price
   FROM ecommerce_events;
   ```
4. Test primary key constraint by attempting duplicate insert

### Expected Results
- unique_ids equals total_rows (all event_ids unique)
- All NULL counts are zero
- user_id range: 1-100
- product_id range: 1000-1100
- price range: 10.00-500.00 (for purchase events)
- Duplicate event_id insert fails with constraint violation
- Primary key auto-increments correctly

### Actual Results
Pass/Fail: _______
Unique IDs: _______
Total rows: _______
NULL violations: _______
Value range issues: _______
Notes: _______________________

---

## Test Case 7: Checkpoint Recovery

### Objective
Test that Spark recovers from failures using checkpoints without data loss.

### Prerequisites
- Pipeline running with at least 20 events processed
- Checkpoint directory populated

### Test Steps
1. Note current event count in database
2. Stop Spark container: `docker-compose stop spark`
3. Wait for 2 new CSV files to be generated
4. Restart Spark: `docker-compose start spark`
5. Monitor logs for recovery messages
6. Wait 30 seconds for processing
7. Count events again
8. Verify the 2 missed files were processed
9. Check for duplicate processing

### Expected Results
- Spark logs show checkpoint recovery
- All CSV files eventually processed
- No duplicate events in database
- Event count increases by exactly 20 (2 files x 10 events)
- No data loss occurs
- Processing resumes from last checkpoint

### Actual Results
Pass/Fail: _______
Events before stop: _______
Events after recovery: _______
Expected increase: 20
Actual increase: _______
Duplicates detected: _______
Notes: _______________________

---

## Test Case 8: Performance Under Load

### Objective
Measure system performance with increased data generation rate.

### Prerequisites
- Baseline metrics collected at default rate
- System resources monitored

### Test Steps
1. Modify `data_generation.py`: Set `SLEEP_SECONDS = 1`
2. Restart data generator
3. Monitor for 5 minutes
4. Record metrics:
   - Events generated per minute
   - Average processing latency
   - Database insert rate
   - Error rate
5. Check system resources: CPU, memory, disk I/O
6. Query streaming_logs for batch processing times

### Expected Results
- System handles 600 events per minute (60 files x 10 events)
- Processing latency remains under 10 seconds
- No errors in streaming_logs
- CPU usage stays below 80%
- Memory usage stable
- No backlog of unprocessed files
- Database keeps pace with ingestion rate

### Actual Results
Pass/Fail: _______
Events/minute: _______
Avg latency: _______ seconds
Max latency: _______ seconds
Errors: _______
CPU usage: _______
Memory usage: _______
Notes: _______________________

---

## Test Case 9: Log File Validation

### Objective
Verify that application logs are generated correctly and contain useful information.

### Prerequisites
- Pipeline running for at least 5 minutes

### Test Steps
1. Check data generator log exists: `ls -lh logs/data_generator.log`
2. Check Spark log exists: `ls -lh logs/spark_streaming.log`
3. Verify generator log contents:
   ```bash
   grep -c "Successfully generated" logs/data_generator.log
   grep -c "ERROR\|CRITICAL" logs/data_generator.log
   ```
4. Verify Spark log contents:
   ```bash
   grep -c "Processing batch" logs/spark_streaming.log
   grep -c "Successfully wrote" logs/spark_streaming.log
   grep -c "ERROR\|Failed" logs/spark_streaming.log
   ```
5. Check log rotation and file sizes

### Expected Results
- Both log files exist and are writable
- Generator log shows successful file creation messages
- Spark log shows batch processing messages
- Log entries have proper timestamps
- Log format: `YYYY-MM-DD HH:MM:SS | LEVEL | MESSAGE`
- Minimal or zero ERROR/CRITICAL entries
- Log files grow over time but remain manageable (<100MB)

### Actual Results
Pass/Fail: _______
Generator log size: _______
Spark log size: _______
Successful generations logged: _______
Batches processed logged: _______
Errors found: _______
Notes: _______________________

---

## Test Case 10: End-to-End Latency

### Objective
Measure the total time from event generation to database storage.

### Prerequisites
- Pipeline fully operational
- Clock synchronization verified

### Test Steps
1. Generate a timestamped marker file
2. Note the exact creation time from generator log
3. Monitor Spark logs for processing of that specific file
4. Query database for events from that file:
   ```sql
   SELECT event_timestamp, ingestion_time, 
          ingestion_time - event_timestamp as latency
   FROM ecommerce_events 
   WHERE event_timestamp > 'MARKER_TIME'
   LIMIT 10;
   ```
5. Calculate average end-to-end latency
6. Repeat test 5 times for statistical significance

### Expected Results
- Average end-to-end latency: 3-10 seconds
- Maximum latency: under 15 seconds
- Latency consistent across test runs
- No sudden spikes or outliers
- 95th percentile latency under 12 seconds

### Actual Results
Pass/Fail: _______
Test run 1 latency: _______ seconds
Test run 2 latency: _______ seconds
Test run 3 latency: _______ seconds
Test run 4 latency: _______ seconds
Test run 5 latency: _______ seconds
Average latency: _______ seconds
Maximum latency: _______ seconds
Notes: _______________________

---

## Test Summary Template

| Test Case | Status | Notes |
|-----------|--------|-------|
| TC1: Data Generation | Pass/Fail | |
| TC2: File Detection | Pass/Fail | |
| TC3: Data Transformation | Pass/Fail | |
| TC4: Database Insertion | Pass/Fail | |
| TC5: Event Distribution | Pass/Fail | |
| TC6: Data Integrity | Pass/Fail | |
| TC7: Checkpoint Recovery | Pass/Fail | |
| TC8: Performance Load | Pass/Fail | |
| TC9: Log Validation | Pass/Fail | |
| TC10: End-to-End Latency | Pass/Fail | |

Overall System Status: Pass/Fail
Test Date: _______
Tester: _______
Environment: _______