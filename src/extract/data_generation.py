import csv
import os
import time
import uuid
import random
import logging
from datetime import datetime


OUTPUT_DIR = "../../data/raw"
LOG_DIR = "../../logs"
LOG_FILE = os.path.join(LOG_DIR, "data_generator.log")

EVENTS_PER_FILE = 10
SLEEP_SECONDS = 3

EVENT_TYPES = ["view", "purchase"]
CURRENCIES = ["USD"]


os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()  
    ]
)

logger = logging.getLogger(__name__)

def generate_event():
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


logger.info("Starting data generator service")

while True:
    try:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"events_{timestamp}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)

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

        logger.info(f"Generated file: {filename} with {EVENTS_PER_FILE} events")

        time.sleep(SLEEP_SECONDS)

    except Exception as e:
        logger.error("Error while generating events", exc_info=True)
        time.sleep(SLEEP_SECONDS)
