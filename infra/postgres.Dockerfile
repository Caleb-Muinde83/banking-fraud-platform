FROM debezium/postgres:15-alpine

# Copy the initialization script into the image so Swarm nodes don't need a host bind
COPY postgres-init.sh /docker-entrypoint-initdb.d/postgres-init.sh

RUN chmod +x /docker-entrypoint-initdb.d/postgres-init.sh

# Keep the base image entrypoint. The compose file overrides entrypoint to execute the
# startup wrapper which will in turn launch the server and apply role updates.
