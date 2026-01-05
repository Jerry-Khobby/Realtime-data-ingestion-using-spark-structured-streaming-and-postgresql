FROM postgres:15

# Automatically executed on first container startup
COPY src/database/postgres_setup.sql /docker-entrypoint-initdb.d/
