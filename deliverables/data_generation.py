import csv
import time
import uuid
import random
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict

OUTPUT_DIR = Path("/data/raw")
LOG_DIR = Path("/logs")
LOG_FILE = LOG_DIR / "data_generator.log"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)



EVENTS_PER_FILE = 10
SLEEP_SECONDS = 3

MAX_WRITE_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2  

EVENT_TYPES = ["view", "purchase"]


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


def write_events_file(filepath: Path) -> None:
    attempt = 1

    while attempt <= MAX_WRITE_RETRIES:
        try:
            with open(filepath, mode="w", newline="") as file:
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

            logger.info(
                f"Successfully generated {filepath.name} "
                f"({EVENTS_PER_FILE} events)"
            )
            return

        except (OSError, IOError) as e:
            logger.warning(
                f"File write failed (attempt {attempt}/{MAX_WRITE_RETRIES}): "
                f"{filepath} | {e}"
            )

            if attempt == MAX_WRITE_RETRIES:
                logger.error(
                    f"Exceeded max retries for file write: {filepath}",
                    exc_info=True
                )
                return

            backoff = RETRY_BACKOFF_SECONDS ** attempt
            logger.info(f"Retrying in {backoff}s...")
            time.sleep(backoff)
            attempt += 1

        except Exception:
            logger.critical(
                f"Fatal error while writing file: {filepath}",
                exc_info=True
            )
            raise


logger.info("Starting data generator service")
logger.info(f"Output directory: {OUTPUT_DIR}")
logger.info(f"Log directory: {LOG_DIR}")

while True:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"events_{timestamp}.csv"
    filepath = OUTPUT_DIR / filename

    try:
        write_events_file(filepath)

    except Exception:
        logger.critical("Generator stopped due to unrecoverable error")
        break

    time.sleep(SLEEP_SECONDS)
