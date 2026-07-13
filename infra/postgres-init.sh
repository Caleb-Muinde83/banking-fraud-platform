#!/bin/bash
set -euo pipefail

TARGET_ROLE="${POSTGRES_USER:-postgres}"
TARGET_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
TARGET_DB="${POSTGRES_DB:-postgres}"

export POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB

echo "Starting PostgreSQL and applying role configuration..."
docker-entrypoint.sh postgres "$@" &
postgres_pid=$!

cleanup() {
  if kill -0 "$postgres_pid" 2>/dev/null; then
    kill -TERM "$postgres_pid" 2>/dev/null || true
    wait "$postgres_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

for attempt in $(seq 1 60); do
  if pg_isready -U postgres -d "$TARGET_DB" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if pg_isready -U postgres -d "$TARGET_DB" >/dev/null 2>&1; then
  echo "Ensuring role $TARGET_ROLE exists with the configured password..."
  psql -v ON_ERROR_STOP=1 --username postgres --dbname "$TARGET_DB" <<EOSQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$TARGET_ROLE') THEN
    CREATE ROLE $TARGET_ROLE WITH LOGIN PASSWORD '$TARGET_PASSWORD';
  ELSE
    ALTER ROLE $TARGET_ROLE WITH LOGIN PASSWORD '$TARGET_PASSWORD';
  END IF;
END
\$\$;
EOSQL
fi

wait "$postgres_pid"
