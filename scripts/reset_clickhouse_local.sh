#!/usr/bin/env bash
set -euo pipefail

# Resets the local ClickHouse database to a clean state
# by stopping the server and removing the data directory

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLICKHOUSE_BIN_DIR="${SCRIPT_DIR}/clickhouse-bin"
CLICKHOUSE_DATA_DIR="${SCRIPT_DIR}/clickhouse-data"
CLICKHOUSE_LOG="${SCRIPT_DIR}/clickhouse.log"
CLICKHOUSE_PID="${SCRIPT_DIR}/clickhouse.pid"

echo "Resetting local ClickHouse database..."

# Stop ClickHouse if it's running
if [[ -f "${CLICKHOUSE_PID}" ]]; then
  PID=$(cat "${CLICKHOUSE_PID}")
  echo "Stopping ClickHouse server (PID: ${PID})..."
  kill "${PID}" 2>/dev/null || {
    echo "ClickHouse was not running or already stopped."
  }
  # Wait a moment for graceful shutdown
  sleep 2
  # Force kill if still running
  kill -9 "${PID}" 2>/dev/null || true
else
  echo "No PID file found. Checking for running ClickHouse processes..."
  pkill -f "clickhouse server" 2>/dev/null || {
    echo "No running ClickHouse processes found."
  }
fi

# Remove data directory
if [[ -d "${CLICKHOUSE_DATA_DIR}" ]]; then
  echo "Removing data directory..."
  rm -rf "${CLICKHOUSE_DATA_DIR}"
  echo "Data directory removed."
else
  echo "Data directory does not exist."
fi

# Remove log file
if [[ -f "${CLICKHOUSE_LOG}" ]]; then
  echo "Removing log file..."
  rm -f "${CLICKHOUSE_LOG}"
  echo "Log file removed."
fi

# Remove PID file
if [[ -f "${CLICKHOUSE_PID}" ]]; then
  echo "Removing PID file..."
  rm -f "${CLICKHOUSE_PID}"
  echo "PID file removed."
fi

echo
echo "ClickHouse reset complete!"
echo "Run './scripts/run_clickhouse_local_test.sh' to start fresh."
