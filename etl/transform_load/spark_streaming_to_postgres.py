import sys
from pathlib import Path
import os
import logging
from contextlib import contextmanager

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, current_timestamp
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv

# Project path setup
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from etl.config import RAW_DATA_DIR, CHECKPOINT_DIR, LOG_DIR, SPARK_LOG_FILE

# Load environment variables
load_dotenv()

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Remove any pre-existing handlers (from previous runs or basicConfig)
if logger.hasHandlers():
    logger.handlers.clear()

# File handler only
file_handler = logging.FileHandler(SPARK_LOG_FILE)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
file_handler.setFormatter(file_formatter)

logger.addHandler(file_handler)

# Test logging
logger.info("=" * 60)
logger.info("Starting Spark Streaming Application (file-only logs)")
logger.info(f"Data directory: {RAW_DATA_DIR}")
logger.info(f"Checkpoint directory: {CHECKPOINT_DIR}")
logger.info(f"Log file: {SPARK_LOG_FILE}")
logger.info("=" * 60)


# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("EcommerceStreamingToPostgres")
    .config("spark.ui.port", "4050")
    .config("spark.sql.streaming.checkpointLocation", str(CHECKPOINT_DIR))
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
logger.info("Spark session initialized")

# Define schema
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("currency", StringType(), True),
])

# Read streaming data
events_df = (
    spark.readStream
    .schema(schema)
    .option("header", True)
    .option("maxFilesPerTrigger", 5)
    .csv(str(RAW_DATA_DIR))
)

clean_df = (
    events_df
    .withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))
    .withColumn("ingestion_time", current_timestamp())
    .filter(col("event_id").isNotNull())
)

# PostgreSQL connection
def read_postgres_config():
    config = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }
    if not config["user"] or not config["password"]:
        raise ValueError("POSTGRES_USER and POSTGRES_PASSWORD must be set")
    logger.info(f"Connecting to PostgreSQL at {config['host']}:{config['port']}/{config['database']}")
    return config

pg_config = read_postgres_config()

# Connection pool
connection_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=5,
    host=pg_config["host"],
    port=pg_config["port"],
    dbname=pg_config["database"],
    user=pg_config["user"],
    password=pg_config["password"]
)

@contextmanager
def get_db_connection():
    conn = connection_pool.getconn()
    try:
        yield conn
    finally:
        connection_pool.putconn(conn)

# Write batch to Postgres
def write_to_postgres(batch_df, batch_id):
    logger.info(f"Processing batch {batch_id}")
    rows_written = 0
    error_msg = None

    try:
        rows_written = batch_df.count()
        logger.info(f"Batch {batch_id}: Found {rows_written} rows")

        if rows_written > 0:
            rows = batch_df.select(
                "event_id", "user_id", "product_id", "event_type",
                "event_timestamp", "price", "quantity", "currency", "ingestion_time"
            ).toLocalIterator()  # avoids loading full batch

            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    import psycopg2.extras
                    psycopg2.extras.execute_batch(cursor, """
                        INSERT INTO ecommerce_events (
                            event_id, user_id, product_id, event_type,
                            event_timestamp, price, quantity, currency, ingestion_time
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO UPDATE SET
                            user_id = EXCLUDED.user_id,
                            product_id = EXCLUDED.product_id,
                            event_type = EXCLUDED.event_type,
                            event_timestamp = EXCLUDED.event_timestamp,
                            price = EXCLUDED.price,
                            quantity = EXCLUDED.quantity,
                            currency = EXCLUDED.currency,
                            ingestion_time = CURRENT_TIMESTAMP
                    """, rows)
                conn.commit()

        logger.info(f"Batch {batch_id}: Successfully wrote {rows_written} rows")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Batch {batch_id} failed: {error_msg}", exc_info=True)

    finally:
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO streaming_logs (batch_id, rows_written, error_message)
                        VALUES (%s, %s, %s)
                    """, (batch_id, rows_written, error_msg))
                conn.commit()
        except Exception as log_error:
            logger.error(f"Failed to write log for batch {batch_id}: {log_error}")


# Start streaming
logger.info("Starting streaming query...")
query = (
    clean_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", str(CHECKPOINT_DIR))
    .trigger(processingTime="3 seconds")
    .start()
)
logger.info(f"Monitoring directory: {RAW_DATA_DIR}")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Received shutdown signal")
    query.stop()
    connection_pool.closeall()
    logger.info("Streaming stopped gracefully")
