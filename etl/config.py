from pathlib import Path
import os



PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = Path("/data")
RAW_DATA_DIR = DATA_DIR / "raw"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"

LOG_DIR = Path("/logs")



EVENTS_PER_FILE = 10
SLEEP_SECONDS = 3

MAX_WRITE_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

EVENT_TYPES = ["view", "purchase"]

DATA_GENERATOR_LOG_FILE = LOG_DIR / "data_generator.log"



SPARK_APP_NAME = "EcommerceStreamingToPostgres"
SPARK_LOG_FILE = LOG_DIR / "spark_streaming.log"



ECOMMERCE_EVENTS_TABLE = "ecommerce_events"
STREAMING_LOGS_TABLE = "streaming_logs"


POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}
