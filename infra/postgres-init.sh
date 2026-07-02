#!/bin/bash
set -euo pipefail

TARGET_ROLE="${POSTGRES_USER:-postgres}"
TARGET_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
TARGET_DB="${POSTGRES_DB:-postgres}"

export POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB

if [ -f /docker-entrypoint-initdb.d/00-init-user.sh ]; then
  /docker-entrypoint-initdb.d/00-init-user.sh
fi

exec docker-entrypoint.sh postgres "$@"
