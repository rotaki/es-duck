#!/usr/bin/env bash
set -euo pipefail

# Resets the local PostgreSQL database to a clean state
# by stopping the server and removing the data directory

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POSTGRES_DIR="${SCRIPT_DIR}/postgres"
POSTGRES_DATA_DIR="${SCRIPT_DIR}/postgres-data"
POSTGRES_LOG="${SCRIPT_DIR}/postgres.log"

echo "Resetting local PostgreSQL database..."

# Stop PostgreSQL if it's running
if [[ -x "${POSTGRES_DIR}/bin/pg_ctl" ]]; then
  echo "Stopping PostgreSQL server..."
  "${POSTGRES_DIR}/bin/pg_ctl" -D "${POSTGRES_DATA_DIR}" stop 2>/dev/null || {
    echo "PostgreSQL was not running or already stopped."
  }
else
  echo "PostgreSQL binaries not found. Nothing to stop."
fi

# Remove data directory
if [[ -d "${POSTGRES_DATA_DIR}" ]]; then
  echo "Removing data directory..."
  rm -rf "${POSTGRES_DATA_DIR}"
  echo "Data directory removed."
else
  echo "Data directory does not exist."
fi

# Remove log file
if [[ -f "${POSTGRES_LOG}" ]]; then
  echo "Removing log file..."
  rm -f "${POSTGRES_LOG}"
  echo "Log file removed."
fi

echo
echo "PostgreSQL reset complete!"
echo "Run './scripts/run_postgres_local_test.sh' to start fresh."
