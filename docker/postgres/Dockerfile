FROM postgres:15

# Automatically executed on first container startup
COPY database/postgres_setup.sql /docker-entrypoint-initdb.d/
