#!/usr/bin/env bash
set -euo pipefail

# Simple helper to install DuckDB CLI and open it against a database file.
# Follows the same pattern as run_clickhouse_local_test.sh / run_postgres_local_test.sh.
#
# Usage:
#   ./scripts/run_duckdb_local_test.sh [path_to_db]
#
# Environment variables:
#   DUCKDB_VERSION  - version to install (default: 1.4.4)
#   DB_FILE         - path to the DuckDB database file (default: ./duckdb_bench.db)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKDB_DIR="${SCRIPT_DIR}/duckdb"
DUCKDB_VERSION="${DUCKDB_VERSION:-1.4.4}"

# DB_FILE can be passed as $1, env var, or defaults
DB_FILE="${1:-${DB_FILE:-./duckdb_bench.db}}"

VERSIONED_BINARY="${DUCKDB_DIR}/duckdb-${DUCKDB_VERSION}"

# 1. Install DuckDB if this version is not present
if [[ ! -x "${VERSIONED_BINARY}" ]]; then
  echo "DuckDB ${DUCKDB_VERSION} not found. Installing..."
  DUCKDB_VERSION="${DUCKDB_VERSION}" "${SCRIPT_DIR}/install_duckdb.sh"
fi

# Verify binary exists after install
if [[ ! -x "${VERSIONED_BINARY}" ]]; then
  echo "Error: DuckDB binary not found at ${VERSIONED_BINARY}" >&2
  exit 1
fi

echo
echo "DuckDB CLI: ${VERSIONED_BINARY}"
echo "  Version: $("${VERSIONED_BINARY}" --version 2>&1 | head -1)"
echo "  Database: ${DB_FILE}"
echo

# 2. Check that the database file exists
if [[ ! -f "${DB_FILE}" ]]; then
  echo "Warning: Database file '${DB_FILE}' does not exist yet."
  echo "You can create one with the load-duckdb binary, or DuckDB will create an empty database."
  echo
fi

# 3. Print helpful commands
cat <<'COMMANDS'
=== Useful commands once inside the DuckDB shell ===

-- Check table contents
SELECT COUNT(*) FROM bench_data;
DESCRIBE bench_data;
SELECT * FROM bench_data LIMIT 5;

-- Show database size
CALL pragma_database_size();

-- Configure for sort benchmarking
SET threads = 4;
SET memory_limit = '1GB';
SET temp_directory = './duckdb_temp';

-- Verify current settings
SELECT name, value, description FROM duckdb_settings()
  WHERE name IN ('threads', 'memory_limit', 'temp_directory');

-- Run the sort query (same as sort-duckdb binary)
EXPLAIN SELECT sort_key, payload FROM bench_data ORDER BY sort_key;

-- Time the sort (analyze mode)
EXPLAIN ANALYZE SELECT sort_key, payload FROM bench_data ORDER BY sort_key;

-- Export sorted data to parquet
COPY (SELECT sort_key, payload FROM bench_data ORDER BY sort_key)
  TO 'sorted_output.parquet' (FORMAT PARQUET, PRESERVE_ORDER true);

-- Check DuckDB temp spill usage while running
-- (run in another terminal)
--   du -sh ./duckdb_temp

=== End of commands ===
COMMANDS

echo
echo "Starting DuckDB shell..."
echo

exec "${VERSIONED_BINARY}" "${DB_FILE}"
