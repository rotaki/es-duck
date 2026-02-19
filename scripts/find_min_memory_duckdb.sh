#!/bin/bash
# Binary search for the minimum MEMORY_LIMIT (GiB) at which DuckDB sort
# completes without being OOM-killed by the cgroup (which is set to
# CGROUP_OVERHEAD_PCT% of MEMORY_LIMIT, matching run_all_benchmarks.sh).
#
# Required env vars:
#   DB_FILE       - path to DuckDB database file
#
# Optional env vars:
#   TABLE              - table name (default: bench_data)
#   TEMP_DIR           - temp directory for DuckDB (default: ./duckdb_temp_memsearch)
#   THREADS            - thread count to test with (default: 4)
#   TIMEOUT_SECONDS    - per-run timeout (default: 7200)
#   LOW_GIB            - lower bound of search, assumed to fail (default: 10)
#   HIGH_GIB           - upper bound of search, assumed to succeed (default: 128)
#   CGROUP_OVERHEAD_PCT - cgroup cap as % of MEMORY_LIMIT (default: 120)
#   CLEAR_CACHE_SCRIPT - path to cache-clear script (default: /usr/local/sbin/clearcache3.sh)
#   LOG_DIR            - directory for per-run logs (default: ./logs/duckdb_min_memory_<timestamp>)

set -euo pipefail

DB_FILE="${DB_FILE:?DB_FILE must be set}"
TABLE="${TABLE:-bench_data}"
THREADS="${THREADS:-4}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-7200}"
LOW_GIB="${LOW_GIB:-10}"
HIGH_GIB="${HIGH_GIB:-128}"
CGROUP_OVERHEAD_PCT="${CGROUP_OVERHEAD_PCT:-120}"
CLEAR_CACHE_SCRIPT="${CLEAR_CACHE_SCRIPT:-/usr/local/sbin/clearcache3.sh}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SEARCH_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="${LOG_DIR:-${SCRIPT_DIR}/logs/duckdb_min_memory_${SEARCH_TIMESTAMP}}"
TEMP_DIR="${TEMP_DIR:-${SCRIPT_DIR}/duckdb_temp_memsearch}"

mkdir -p "$LOG_DIR" "$TEMP_DIR"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

clear_caches() {
    if [[ -f "$CLEAR_CACHE_SCRIPT" ]]; then
        echo "[cache] Clearing system caches..."
        sudo "$CLEAR_CACHE_SCRIPT" || echo "[cache] Warning: cache clear failed"
    else
        echo "[cache] Skipping cache clear (script not found: $CLEAR_CACHE_SCRIPT)"
    fi
}

# Run DuckDB sort under a cgroup scoped at CGROUP_OVERHEAD_PCT% of mem_gib.
# Returns the exit code of the sort process (0=ok, 137=OOM, 124=timeout).
run_trial() {
    local mem_gib="$1"
    local mem_bytes=$(( mem_gib * 1024 * 1024 * 1024 ))
    local cgroup_bytes=$(( mem_bytes * CGROUP_OVERHEAD_PCT / 100 ))
    local cgroup_gib=$(( cgroup_bytes / 1024 / 1024 / 1024 ))
    local log_file="${LOG_DIR}/${mem_gib}GiB_${THREADS}threads.log"
    local unit_name="es-duck-memsearch-${mem_gib}gib-$$-$(date +%s)"

    echo ""
    echo "========================================="
    echo "[search] Testing MEMORY_LIMIT=${mem_gib}GiB  cgroup cap=${cgroup_gib}GiB"
    echo "[search] Start: $(date +"%Y-%m-%d %H:%M:%S")"
    echo "========================================="

    clear_caches

    rm -rf "${TEMP_DIR:?}"/*
    mkdir -p "$TEMP_DIR"

    # Probe cgroup scope creation first (mirrors run_with_cgroup_limits).
    if ! systemd-run --user --scope --quiet --collect \
            --unit "${unit_name}-probe" \
            --property "MemoryAccounting=yes" \
            --property "MemoryHigh=${cgroup_bytes}" \
            --property "MemoryMax=${cgroup_bytes}" \
            --property "MemorySwapMax=0" \
            -- true 2>/dev/null; then
        echo "[cgroup] ERROR: failed to create cgroup scope." >&2
        return 1
    fi

    local exit_code=0
    set +e
    systemd-run --user --scope --quiet --collect \
        --unit "${unit_name}" \
        --property "MemoryAccounting=yes" \
        --property "MemoryHigh=${cgroup_bytes}" \
        --property "MemoryMax=${cgroup_bytes}" \
        --property "MemorySwapMax=0" \
        -- timeout "$TIMEOUT_SECONDS" \
            bash -c "cd '${PROJECT_DIR}' && cargo run --release --bin sort-duckdb --features db-duckdb -- \
                --db '${DB_FILE}' \
                --table '${TABLE}' \
                --memory-limit '${mem_gib}GiB' \
                --temp-dir '${TEMP_DIR}' \
                --threads '${THREADS}'" 2>&1 | tee "$log_file"
    exit_code=${PIPESTATUS[0]}
    set -e

    local status
    case $exit_code in
        0)   status="SUCCESS" ;;
        137) status="OOM_KILLED" ;;
        124) status="TIMEOUT" ;;
        *)   status="FAILED (exit ${exit_code})" ;;
    esac

    echo "[search] ${mem_gib}GiB => ${status}"
    echo "[search] Log: $log_file"

    return $exit_code
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

echo "=== DuckDB Minimum Memory Binary Search ==="
echo "Database:      $DB_FILE"
echo "Table:         $TABLE"
echo "Threads:       $THREADS"
echo "Search range:  [${LOW_GIB}GiB, ${HIGH_GIB}GiB]"
echo "Cgroup overhead: ${CGROUP_OVERHEAD_PCT}%"
echo "Log directory: $LOG_DIR"
echo ""

if [[ $LOW_GIB -ge $HIGH_GIB ]]; then
    echo "ERROR: LOW_GIB ($LOW_GIB) must be less than HIGH_GIB ($HIGH_GIB)" >&2
    exit 1
fi

# Verify the upper bound actually succeeds before committing to a long search.
echo "[search] Verifying upper bound (${HIGH_GIB}GiB)..."
upper_exit=0
run_trial "$HIGH_GIB" || upper_exit=$?
if [[ $upper_exit -ne 0 ]]; then
    echo ""
    echo "ERROR: Upper bound ${HIGH_GIB}GiB also failed (exit ${upper_exit})." >&2
    echo "       Increase HIGH_GIB and try again." >&2
    exit 1
fi

# Binary search: invariant — low fails, high succeeds.
low=$LOW_GIB
high=$HIGH_GIB

while [[ $(( high - low )) -gt 1 ]]; do
    mid=$(( (low + high) / 2 ))
    echo ""
    echo "[search] Range: [${low}GiB, ${high}GiB]  trying mid=${mid}GiB..."

    trial_exit=0
    run_trial "$mid" || trial_exit=$?

    if [[ $trial_exit -eq 0 ]]; then
        high=$mid
    else
        low=$mid
    fi
done

echo ""
echo "==========================================="
echo "=== Binary Search Complete              ==="
echo "==========================================="
echo "  Minimum MEMORY_LIMIT: ${high}GiB"
echo "  Cgroup cap at ${high}GiB: $(( high * CGROUP_OVERHEAD_PCT / 100 ))GiB  (${CGROUP_OVERHEAD_PCT}%)"
echo "  Threads tested: $THREADS"
echo "  Logs: $LOG_DIR"
echo "==========================================="
