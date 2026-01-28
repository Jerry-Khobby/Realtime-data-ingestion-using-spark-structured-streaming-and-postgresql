import csv
import time
import uuid
import random
import logging
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from etl.config import (
    RAW_DATA_DIR,
    LOG_DIR,
    DATA_GENERATOR_LOG_FILE,
    EVENTS_PER_FILE,
    SLEEP_SECONDS,
    MAX_WRITE_RETRIES,
    RETRY_BACKOFF_SECONDS,
    EVENT_TYPES
)

OUTPUT_DIR: Path = RAW_DATA_DIR
LOG_FILE: Path = DATA_GENERATOR_LOG_FILE

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Graceful shutdown handler
shutdown_flag = False

def signal_handler(sig, frame):
    global shutdown_flag
    logger.info("Shutdown signal received. Finishing current file...")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def generate_event() -> Dict[str, str]:
    event_type = random.choice(EVENT_TYPES)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 100),
        "product_id": random.randint(1000, 1100),
        "event_type": event_type,
        "event_timestamp": datetime.utcnow().isoformat(),
        "price": round(random.uniform(10, 500), 2) if event_type == "purchase" else "",
        "quantity": random.randint(1, 3) if event_type == "purchase" else "",
        "currency": "USD" if event_type == "purchase" else ""
    }


def write_events_file(filepath: Path) -> bool:
    """Returns True if successful, False otherwise"""
    temp_filepath = filepath.with_suffix('.tmp')
    attempt = 1

    while attempt <= MAX_WRITE_RETRIES:
        try:
            # Write to temporary file first
            with open(temp_filepath, mode="w", newline="") as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=[
                        "event_id",
                        "user_id",
                        "product_id",
                        "event_type",
                        "event_timestamp",
                        "price",
                        "quantity",
                        "currency"
                    ]
                )
                writer.writeheader()

                for _ in range(EVENTS_PER_FILE):
                    writer.writerow(generate_event())
            
            # Atomic rename - prevents Spark from reading partial files
            temp_filepath.rename(filepath)

            logger.info(
                f"Successfully generated {filepath.name} "
                f"({EVENTS_PER_FILE} events)"
            )
            return True

        except (OSError, IOError) as e:
            logger.warning(
                f"File write failed (attempt {attempt}/{MAX_WRITE_RETRIES}): "
                f"{filepath} | {e}"
            )

            # Cleanup temp file if exists
            if temp_filepath.exists():
                try:
                    temp_filepath.unlink()
                except Exception:
                    pass

            if attempt == MAX_WRITE_RETRIES:
                logger.error(
                    f"Exceeded max retries for file write: {filepath}",
                    exc_info=True
                )
                return False

            backoff = RETRY_BACKOFF_SECONDS ** attempt
            logger.info(f"Retrying in {backoff}s...")
            time.sleep(backoff)
            attempt += 1

        except Exception:
            logger.critical(
                f"Fatal error while writing file: {filepath}",
                exc_info=True
            )
            # Cleanup temp file
            if temp_filepath.exists():
                try:
                    temp_filepath.unlink()
                except Exception:
                    pass
            raise

    return False


logger.info("Starting data generator service")
logger.info(f"Output directory: {OUTPUT_DIR}")
logger.info(f"Log file: {LOG_FILE}")
logger.info(f"Events per file: {EVENTS_PER_FILE}")
logger.info(f"Sleep interval: {SLEEP_SECONDS}s")

file_count = 0

while not shutdown_flag:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")  # Added microseconds for uniqueness
    filename = f"events_{timestamp}.csv"
    filepath = OUTPUT_DIR / filename

    try:
        if write_events_file(filepath):
            file_count += 1
            if file_count % 10 == 0:  # Log progress every 10 files
                logger.info(f"Progress: {file_count} files generated")
    except Exception:
        logger.critical("Generator stopped due to unrecoverable error")
        break

    time.sleep(SLEEP_SECONDS)

logger.info(f"Data generator stopped gracefully. Total files generated: {file_count}")