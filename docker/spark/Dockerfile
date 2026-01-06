FROM octoenergy/pyspark:4.0.0

USER root

# Add PostgreSQL JDBC driver
COPY jars/postgresql-42.7.8.jar /opt/spark/jars/

# Create necessary directories
RUN mkdir -p /opt/spark-apps /data/checkpoints /logs && \
  chown -R 1000:1000 /opt/spark-apps /data /logs

# Set working directory
WORKDIR /opt/spark-apps

USER 1000

# Keep container running
CMD ["tail", "-f", "/dev/null"]