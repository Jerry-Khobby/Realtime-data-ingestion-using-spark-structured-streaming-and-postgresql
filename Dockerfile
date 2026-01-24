FROM octoenergy/pyspark:4.0.0

USER root

# Install required Python packages
RUN pip install --no-cache-dir python-dotenv psycopg2-binary faker

# Download PostgreSQL JDBC driver
RUN wget -P /opt/spark/jars/ https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

# Create necessary directories
RUN mkdir -p \
  /opt/spark-apps/etl/extract \
  /opt/spark-apps/etl/transform_load \
  /data/raw \
  /data/checkpoints \
  /logs && \
  chown -R 1000:1000 /opt/spark-apps /data /logs

# Copy application files
COPY etl/ /opt/spark-apps/etl/
COPY main.py /opt/spark-apps/main.py

WORKDIR /opt/spark-apps

USER 1000

# Keep container running
CMD ["tail", "-f", "/dev/null"]
