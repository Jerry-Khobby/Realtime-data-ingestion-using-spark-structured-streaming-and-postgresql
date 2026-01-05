from pyspark.sql import SparkSession,Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DoubleType, TimestampType
)
from pyspark.sql.functions import col, current_timestamp
import os 
import logging 


LOG_DIR = "/logs"
os.makedirs(LOG_DIR,exist_ok=True)


logging.basicConfig(
    filename=os.path.join(LOG_DIR, "spark_streaming.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


spark = (
    SparkSession.builder
    .appName("EcommerceStreamingToPostgres")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

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


events_df = (
    spark.readStream
    .schema(schema)
    .option("header", True)
    .csv("/data/raw")
)


clean_df = (
    events_df
    .withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))
    .withColumn("ingestion_time", current_timestamp())
    .filter(col("event_id").isNotNull())
)


def read_postgres_config(path="../postgres_connection_details.txt"):
    config = {}
    with open(path, "r") as f:
        for line in f:
            key, val = line.strip().split("=")
            config[key] = val
    return config

config = read_postgres_config()
jdbc_url = f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"
jdbc_properties = {
    "user": config["user"],
    "password": config["password"],
    "driver": "org.postgresql.Driver"
}



def write_to_postgres(batch_df, batch_id):
    rows_written = 0
    error_msg = None
    try:
        rows_written = batch_df.count()
        if rows_written > 0:
            batch_df.write.jdbc(
                url=jdbc_url,
                table="ecommerce_events",
                mode="append",
                properties=jdbc_properties
            )
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Batch {batch_id}: {error_msg}", exc_info=True)
    finally:
        # Write batch log to PostgreSQL
        log_df = spark.createDataFrame([
            Row(batch_id=batch_id, rows_written=rows_written, error_message=error_msg)
        ])
        log_df.write.jdbc(
            url=jdbc_url,
            table="streaming_logs",
            mode="append",
            properties=jdbc_properties
        )



query = (
    clean_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", "/data/checkpoints")
    .start()
)

query.awaitTermination()
