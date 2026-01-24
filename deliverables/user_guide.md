# User Guide: Real-Time Data Ingestion Pipeline

## Prerequisites

* Docker Engine ≥ 20.10
* Docker Compose ≥ 2.0
* ≥ 4GB RAM, ≥ 5GB free disk

---

## Project Structure

```
project-root/
├── data/                 (raw CSVs & Spark checkpoints)
├── database/
│   └── postgres_setup.sql
├── deliverables/         (*.md, system architecture)
├── etl/
│   ├── extract/data_generation.py
│   └── transform_load/spark_stream.py
├── logs/
├── Dockerfile
├── docker-compose.yml
├── main.py
└── .env
```

---

## Quick Start

### 1. Configure Environment

Create `.env`:

```bash
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce
POSTGRES_USER=spark_user
POSTGRES_PASSWORD=spark_pass
```
---

### 2. Build and Start Services

```bash
docker-compose up -d --build
docker-compose ps
```

Check containers `postgres` and `spark` are running.

---

### 3. Initialize Database

```bash
docker exec -it postgres psql -U spark_user -d ecommerce -f database/postgres_setup.sql
```

Verify tables:

```sql
\dt
```

---

### 4. Run Data Generator

```bash
docker exec -it spark bash
python /opt/spark-apps/etl/extract/data_generation.py
```

---

### 5. Start Spark Streaming

```bash
docker exec -it spark bash
spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /opt/spark-apps/etl/transform_load/spark_stream.py
```

---

### 6. Monitor Pipeline

* Spark UI: `http://localhost:4040`
* Generated CSVs:

```bash
docker exec -it spark ls -ltr /data/raw
```

* Verify database data:

```bash
docker exec -it postgres psql -U spark_user -d ecommerce -c "SELECT COUNT(*) FROM user_events;"
```

---

## Pipeline Operations

### Start / Restart

* **Fresh start (reset data):**

```bash
docker-compose down -v
docker-compose up -d --build
```

* **Normal restart:**

```bash
docker-compose down
docker-compose up -d
```

* **Restart specific service:**

```bash
docker-compose restart spark
```

### Stop

```bash
docker-compose down
```

Stop data generator or Spark manually via Ctrl+C in their terminals.

---

## Data Management

* Clear generated CSVs:

```bash
docker exec -it spark rm -f /data/raw/*.csv
```

* Clear Spark checkpoints:

```bash
docker exec -it spark rm -rf /data/checkpoints/*
```

* Truncate database table:

```bash
docker exec -it postgres psql -U spark_user -d ecommerce -c "TRUNCATE TABLE user_events;"
```

---

## Configuration

* Adjust generation rate in `data_generation.py`:

```python
EVENTS_PER_FILE = 10
SLEEP_SECONDS = 3
```

* Modify Spark settings in `spark_stream.py` (batch interval, schema, checkpoint path).
* Update database credentials in `.env` and `postgres_setup.sql` if needed.
