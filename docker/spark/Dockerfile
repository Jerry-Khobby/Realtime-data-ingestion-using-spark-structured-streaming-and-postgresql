FROM octoenergy/pyspark:4.0.0

USER root

# Install psycopg2
RUN pip install --no-cache-dir psycopg2-binary

# Create necessary directories
RUN mkdir -p /opt/spark-apps /data/checkpoints /logs && \
  chown -R 1000:1000 /opt/spark-apps /data /logs

WORKDIR /opt/spark-apps

USER 1000

# Keep container running
CMD ["tail", "-f", "/dev/null"]
