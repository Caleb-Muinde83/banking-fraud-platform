#!/bin/bash
set -euo pipefail

TARGET_ROLE="${POSTGRES_USER:-postgres}"
TARGET_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
TARGET_DB="${POSTGRES_DB:-postgres}"

/usr/local/bin/docker-entrypoint.sh postgres "$@" &
pid=$!

cleanup() {
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

until pg_isready -U "$TARGET_ROLE" -d "$TARGET_DB" >/dev/null 2>&1; do
  sleep 2
done

echo "Ensuring role $TARGET_ROLE has the configured password..."
psql -v ON_ERROR_STOP=1 --username "$TARGET_ROLE" --dbname "$TARGET_DB" <<EOSQL
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

wait "$pid"
