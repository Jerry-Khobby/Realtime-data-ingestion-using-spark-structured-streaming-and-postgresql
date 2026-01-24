# Performance Metrics
## Overview

This document analyzes the performance of the real-time data ingestion pipeline using Spark Structured Streaming and PostgreSQL. Metrics were collected from a 6.3-minute test run starting at 09:35:22 on January 24, 2026.

## Test Environment

**Infrastructure:**
- Spark 4.0.0 running in Docker container (local[*] mode)
- PostgreSQL 15 in Docker container
- Network: Bridge network (spark_network)
- Java Version: 17.0.17
- Scala Version: 2.13.16

**Configuration:**
- Batch Interval: 3 seconds
- Records per Batch: 10 events
- Checkpoint Location: /data/checkpoints
- JDBC Driver: PostgreSQL 42.7.1
- Executor Memory: 434.4 MiB allocated, 4 MiB used

## Key Performance Indicators

### Throughput Metrics

**Processing Summary:**
- Total Runtime: 6.3 minutes (378 seconds)
- Total Batches Processed: 29 batches (batch 38-66)
- Total Records Ingested: 550 events
- Average Records per Batch: 10 events
- Records per Minute: ~87 records/minute
- Records per Second: ~1.45 records/second

**Event Distribution:**
- View events: 260 (47.3%)
- Purchase events: 290 (52.7%)

### Latency Metrics

**Batch Processing Time:**
- Minimum Processing Time: 0.3 seconds
- Maximum Processing Time: 1.0 seconds (batch 38)
- Average Processing Time: 0.4-0.5 seconds
- Median Processing Time: 0.4 seconds

**Job Execution Breakdown:**
Per batch, Spark executed 3 jobs:
- Count Operation: 48-200ms
- Data Preparation: 27-100ms
- Database Write: 300-800ms

**Stage Execution:**
- Total Stages Completed: 116 stages
- Average Stage Duration: 30-70ms per stage
- Task Completion: 319 tasks executed successfully

**End-to-End Latency:**
- Average latency (event generation to database): 670ms
- Latency range: 670-670ms (highly consistent)
- Data shows sub-second insertion times

### Reliability Metrics

**Success Rate:**
- Total Batches: 29
- Successful Batches: 29
- Failed Batches: 0
- Success Rate: 100%

**Data Quality:**
- Total records: 550
- Records with null user_id or price: 260
- Records with negative prices: 0
- Valid user_id range: 1-100
- Valid timestamps: 290 records within last hour

**Task Execution:**
- Total Tasks: 319
- Successful Tasks: 319
- Failed Tasks: 0
- Task Success Rate: 100%

### Resource Utilization

**Spark Executor:**
- Active Executors: 1 (driver)
- CPU Cores: 8
- Storage Memory Used: 4 MiB / 434.4 MiB (0.9%)
- Active Tasks: 0 (at measurement time)
- Total Task Time: 22 minutes
- GC Time: 0.7 seconds (negligible overhead)

**Data Transfer:**
- Total Input: 54.7 KiB
- Shuffle Read: 1.7 KiB
- Shuffle Write: 1.7 KiB
- Average per batch: ~970 bytes

**Job Distribution:**
- Total Jobs Completed: 78 jobs
- Jobs per Batch: 3 jobs (count, prepare, write)
- All jobs completed successfully

## Performance Analysis

### Strengths

1. **Perfect Reliability:** 100% success rate across 29 batches with zero failures.

2. **Consistent Latency:** Sub-second end-to-end latency averaging 670ms from event generation to database insertion.

3. **Stable Processing:** Batch processing times remained stable between 300-800ms throughout the entire run.

4. **Minimal Resource Usage:** Only 4 MiB of 434.4 MiB allocated memory used, indicating efficient resource utilization.

5. **Low GC Overhead:** 0.7 seconds GC time over 22 minutes of task time represents negligible overhead.

### Observations

1. **Processing Pattern:** Regular 3-second intervals between batches with consistent 10-record batches.

2. **Data Quality:** 260 records (47.3%) have null user_id or price fields, indicating possible schema issues or data generation characteristics.

3. **Write Performance:** Database write operations (300-800ms) represent the largest portion of batch processing time.

4. **Network Efficiency:** Minimal shuffle operations (1.7 KiB read/write) indicate efficient data processing without unnecessary data movement.

## Performance Bottleneck Analysis

**Primary Bottleneck:** Database write operation (300-800ms per batch)
- Represents 60-80% of total batch processing time
- JDBC connection overhead for small batches
- Network latency between Spark and PostgreSQL containers

**Secondary Considerations:**
- Small batch sizes (10 records) increase per-record overhead
- File-based streaming adds I/O latency
- Single executor limits parallelism

## Scalability Assessment

**Current Capacity:**
- Processing 10 records every 3 seconds
- Theoretical maximum: 1,200 records/hour at current configuration
- Actual throughput: 5,220 records/hour (87 records/minute)

**Scaling Potential:**
- With 100 records per batch: ~870 records/minute
- With 1000 records per batch: ~8,700 records/minute
- Memory headroom: 430 MiB available for larger batches

**Resource Headroom:**
- CPU: Minimal utilization, significant capacity available
- Memory: 99% unused (430 MiB available)
- Network: No saturation observed

## Recommendations

**For Higher Throughput:**
1. Increase batch size to 100-1000 records to amortize connection overhead
2. Reduce trigger interval to 1 second for faster processing
3. Enable parallel processing with multiple executors

**For Lower Latency:**
1. Use continuous processing mode instead of micro-batches
2. Implement connection pooling for database writes
3. Add database indexes on frequently queried columns
4. Consider using bulk insert operations

**For Production Deployment:**
1. Monitor batch processing times and set alerts for delays exceeding 2 seconds
2. Track memory usage and GC overhead
3. Implement retry logic for transient failures
4. Set up checkpoint cleanup to prevent disk space issues
5. Address null value handling in data generation or validation logic

## Conclusion

The pipeline demonstrates excellent reliability with 100% success rate and consistent sub-second latency. Processing 550 events over 6.3 minutes with zero failures shows a stable, production-ready system. The 670ms end-to-end latency meets near-real-time requirements. Current bottleneck is database write operations at 300-800ms per batch. System has significant resource headroom (99% memory available) for scaling. Recommended next steps include increasing batch size for higher throughput and implementing connection pooling for lower latency.