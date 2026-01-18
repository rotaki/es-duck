#!/usr/bin/env bash
set -euo pipefail

# Simple helper to start a local Postgres instance in Docker
# suitable for running the postgres_integration_test.
#
# Usage:
#   ./scripts/run_postgres_docker_test.sh
# Then, in another terminal:
#   export POSTGRES_TEST_URL=postgres://postgres:postgres@localhost:5432/es_duck_test
#   cargo test --test postgres_integration_test
#
# When you're done:
#   docker stop es-duck-postgres-test

CONTAINER_NAME="${CONTAINER_NAME:-es-duck-postgres-test}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-es_duck_test}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not installed or not on PATH." >&2
  exit 1
fi

echo "Starting Postgres Docker container '${CONTAINER_NAME}' on port ${POSTGRES_PORT}..."

# Mount /tmp so PostgreSQL can write test output files that are accessible from the host
docker run --rm -d \
  --name "${CONTAINER_NAME}" \
  -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
  -e POSTGRES_DB="${POSTGRES_DB}" \
  -p "${POSTGRES_PORT}:5432" \
  -v /tmp:/tmp \
  postgres:18

POSTGRES_TEST_URL="postgres://postgres:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"

echo
echo "Postgres is starting in Docker."
echo "Use this in another terminal:"
echo "  export POSTGRES_TEST_URL=\"${POSTGRES_TEST_URL}\""
echo "  cargo test --test postgres_integration_test"
echo
echo "When finished, stop the container with:"
echo "  docker stop ${CONTAINER_NAME}"

