from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DoubleType, TimestampType
)
from pyspark.sql.functions import col, current_timestamp
import os
import logging
import psycopg2

# Directories
DATA_DIR = "/data/raw"
CHECKPOINT_DIR = "/data/checkpoints"
LOG_DIR = "/logs"

# Create necessary directories
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "spark_streaming.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("EcommerceStreamingToPostgres")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

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
    .csv(DATA_DIR)
)

clean_df = (
    events_df
    .withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))
    .withColumn("ingestion_time", current_timestamp())
    .filter(col("event_id").isNotNull())
)

def read_postgres_config():
    """Read PostgreSQL config from environment variables"""
    config = {
        'host': os.getenv('POSTGRES_HOST', 'postgres'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB', 'ecommerce'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD')
    }

    if not config['user'] or not config['password']:
        raise ValueError("POSTGRES_USER and POSTGRES_PASSWORD must be set")
    
    logger.info(f"Connecting to PostgreSQL at {config['host']}:{config['port']}/{config['database']}")
    return config

config = read_postgres_config()



# Function to write each batch using psycopg2
def write_to_postgres(batch_df, batch_id):
    rows_written = 0
    error_msg = None

    try:
        rows_written = batch_df.count()
        if rows_written > 0:
            rows = [tuple(row) for row in batch_df.collect()]
            conn = psycopg2.connect(
                host=config['host'],
                port=config['port'],
                dbname=config['database'],
                user=config['user'],
                password=config['password']
            )
            cursor = conn.cursor()
            insert_sql = """
                INSERT INTO ecommerce_events
                (event_id, user_id, product_id, event_type, event_timestamp, price, quantity, currency, ingestion_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_sql, rows)
            conn.commit()
            cursor.close()
            conn.close()

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Batch {batch_id} failed: {error_msg}", exc_info=True)

    finally:
        # Use explicit schema for log_df
        log_schema = StructType([
            StructField("batch_id", IntegerType(), True),
            StructField("rows_written", IntegerType(), True),
            StructField("error_message", StringType(), True)
        ])
        log_df = spark.createDataFrame([(batch_id, rows_written, error_msg)], schema=log_schema)

        try:
            log_rows = [tuple(row) for row in log_df.collect()]
            conn = psycopg2.connect(
                host=config['host'],
                port=config['port'],
                dbname=config['database'],
                user=config['user'],
                password=config['password']
            )
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT INTO streaming_logs
                (batch_id, rows_written, error_message)
                VALUES (%s, %s, %s)
            """, log_rows)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as log_error:
            logger.error(f"Failed to write log for batch {batch_id}: {log_error}")

# Start streaming query
logger.info("Starting streaming query...")
query = (
    clean_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .start()
)

logger.info("Streaming query started. Waiting for termination...")
query.awaitTermination()
