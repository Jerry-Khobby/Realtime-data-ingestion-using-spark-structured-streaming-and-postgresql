import subprocess
import logging
import sys
import signal
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).parent.resolve()
ETL_DIR = SCRIPT_DIR / "etl"
EXTRACT_DIR = ETL_DIR / "extract"

LOG_DIR = SCRIPT_DIR / "logs"
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


def start_local_process(script_path, name):
    """Run a Python script locally."""
    try:
        logger.info(f"Starting {name}: {script_path}")
        proc = subprocess.Popen([sys.executable, str(script_path)])
        processes.append(proc)
        return proc
    except Exception as e:
        logger.error(f"Failed to start {name}: {e}", exc_info=True)
        raise

def start_spark_in_docker(container_name, script_inside_container):
    """Run Spark ETL inside Docker."""
    try:
        logger.info(f"Starting Spark ETL inside Docker container '{container_name}'")
        cmd = [
            "docker", "exec", "-i", container_name,
            "spark-submit", script_inside_container
        ]
        proc = subprocess.Popen(cmd)
        processes.append(proc)
        return proc
    except Exception as e:
        logger.error(f"Failed to start Spark ETL in Docker: {e}", exc_info=True)
        raise

def shutdown(signum, frame):
    """Terminate all child processes gracefully."""
    logger.info("Shutting down orchestrator and child processes...")
    for proc in processes:
        proc.terminate()
    for proc in processes:
        proc.wait()
    logger.info("All processes terminated. Exiting.")
    sys.exit(0)


if __name__ == "__main__":
    # Catch termination signals
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        logger.info("Ensure Docker containers (Postgres, Spark) are running first.")
        logger.info("Sleeping 5 seconds to allow services to start...")
        time.sleep(5)

        # --------------------------
        # Start Spark inside Docker
        # --------------------------
        spark_container_name = "spark"  # replace with your container name if different
        spark_script_path = "/opt/spark-apps/etl/transform_load/spark_streaming_to_postgres.py"
        spark_proc = start_spark_in_docker(spark_container_name, spark_script_path)

        # --------------------------
        # Start local data generator
        # --------------------------
        data_gen_script = EXTRACT_DIR / "data_generation.py"
        data_gen_proc = start_local_process(data_gen_script, "Data Generator")

        logger.info("Orchestrator running. Spark monitoring CSVs, Data Generator producing events.")

        # Monitor processes
        while True:
            for proc in processes:
                retcode = proc.poll()
                if retcode is not None:
                    logger.error(f"Process {proc.args} exited with code {retcode}. Shutting down.")
                    shutdown(None, None)
            time.sleep(2)

    except Exception as e:
        logger.error(f"Orchestrator encountered an error: {e}", exc_info=True)
        shutdown(None, None)
