#Use the official PostgreSQL image as the base
FROM postgres:15

# Copy initialization scripts to the Docker entrypoint directory
COPY init-db.sql /docker-entrypoint-initdb.d/

# Expose PostgreSQL port
EXPOSE 5432