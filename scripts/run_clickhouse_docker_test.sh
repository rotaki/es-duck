#!/usr/bin/env bash
set -euo pipefail

# Simple helper to start a local ClickHouse instance in Docker
# suitable for running high-performance ingestion benchmarks.
#
# Usage:
#   ./scripts/run_clickhouse_docker_test.sh
# Then, in another terminal:
#   export CLICKHOUSE_URL=http://localhost:8123
#   cargo run -- --format gensort --input data.bin --threads 8
#
# When you're done:
#   docker stop es-duck-clickhouse-test

CONTAINER_NAME="${CONTAINER_NAME:-es-duck-clickhouse-test}"
HTTP_PORT="${HTTP_PORT:-8123}"
NATIVE_PORT="${NATIVE_PORT:-9000}"
USER="${CLICKHOUSE_USER:-default}"
PASSWORD="${CLICKHOUSE_PASSWORD:-}" # Default is empty for testing

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not installed or not on PATH." >&2
  exit 1
fi

echo "Starting ClickHouse Docker container '${CONTAINER_NAME}'..."

# Optimization notes:
# --ulimit: High-performance ClickHouse needs high open file limits (nofile) and processes (nproc).
# -v /tmp:/tmp: Essential for local file exports/imports.
docker run --rm -d \
  --name "${CONTAINER_NAME}" \
  --ulimit nofile=262144:262144 \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  -p "${HTTP_PORT}:8123" \
  -p "${NATIVE_PORT}:9000" \
  -v /tmp:/tmp \
  clickhouse/clickhouse-server:latest

# Wait for ClickHouse to accept connections
echo "Waiting for ClickHouse to be ready..."
until curl -s "http://localhost:${HTTP_PORT}/ping" > /dev/null; do
  sleep 1
done

CLICKHOUSE_URL="http://localhost:${HTTP_PORT}"

echo
echo "ClickHouse is running in Docker."
echo "Native Protocol Port: ${NATIVE_PORT} (use this for clickhouse-rs/tcp)"
echo "HTTP Protocol Port: ${HTTP_PORT} (use this for reqwest/http)"
echo
echo "Use this in another terminal:"
echo "  export CLICKHOUSE_URL=\"${CLICKHOUSE_URL}\""
echo "  cargo run -- --url \$CLICKHOUSE_URL --threads \$(nproc)"
echo
echo "When finished, stop the container with:"
echo "  docker stop ${CONTAINER_NAME}"