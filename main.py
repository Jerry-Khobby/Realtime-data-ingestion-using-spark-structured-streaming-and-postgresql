import subprocess
import logging
import sys
import signal
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
LOG_DIR = Path("/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
MAIN_LOG_FILE = LOG_DIR / "main.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(MAIN_LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

processes = []

def start_spark_job(script_path):
    """Run Spark job directly (already inside container)."""
    try:
        logger.info(f"Starting Spark job: {script_path}")
        cmd = ["spark-submit", str(script_path)]
        proc = subprocess.Popen(cmd)
        processes.append(proc)
        return proc
    except Exception as e:
        logger.error(f"Failed to start Spark job: {e}", exc_info=True)
        raise

def shutdown(signum, frame):
    logger.info("Shutting down orchestrator and child processes...")
    for proc in processes:
        proc.terminate()
    for proc in processes:
        proc.wait()
    logger.info("All processes terminated. Exiting.")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        spark_script_path = "/opt/spark-apps/transform_load/spark_streaming_to_postgres.py"
        spark_proc = start_spark_job(spark_script_path)

        logger.info("Spark streaming job started.")

        while True:
            for proc in processes:
                retcode = proc.poll()
                if retcode is not None:
                    logger.error(f"Process exited with code {retcode}. Shutting down.")
                    shutdown(None, None)
            time.sleep(2)

    except Exception as e:
        logger.error(f"Orchestrator encountered an error: {e}", exc_info=True)
        shutdown(None, None)
