#!/bin/bash
# ClickHouse memory sweep: vary memory limit at fixed thread count
# FIXED VERSION: Shows real-time output and has timeout protection

set -e

# Generate timestamp for this sweep run
SWEEP_TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Configuration
INPUT_FILE="${INPUT_FILE:-testdata/test_gensort_5gb.dat}"
FORMAT="${FORMAT:-gensort}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
DATABASE="${DATABASE:-default}"
TABLE="${TABLE:-bench_data}"
THREADS="${THREADS:-16}"
# MEMORY_LIMITS="${MEMORY_LIMITS:-2GiB 4GiB 6GiB 8GiB 16GiB 24GiB 32GiB 48GiB}"
MEMORY_LIMITS="${MEMORY_LIMITS:-48GiB 32GiB 24GiB 16GiB 8GiB 4GiB 2GiB}"
LOG_DIR="${LOG_DIR:-./logs/clickhouse_memory_sweep_${SWEEP_TIMESTAMP}}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-7200}"  # 2 hour default timeout
BENCHMARK_RUNS="${BENCHMARK_RUNS:-1}"  # Number of times to run each configuration
CLEAR_CACHE_SCRIPT="/usr/local/sbin/clearcache3.sh"
OUTPUT="${OUTPUT:-}"  # Optional output path for binary mode
RCLONE_REMOTE="${RCLONE_REMOTE:-}"
CGROUP_MODE="${CGROUP_MODE:-on}"  # on | off

# ── Helper Functions ──────────────────────────────────────────────────────────

parse_memory_to_bytes() {
    local raw="${1//[[:space:]]/}"
    local mem="${raw^^}"
    local value

    # Accepts GiB/MiB/KiB (binary units), or shorthand G/M/K.
    if [[ "$mem" =~ ^([0-9]+([.][0-9]+)?)(GIB|G)$ ]]; then
        value="${BASH_REMATCH[1]}"
        awk -v v="$value" 'BEGIN { printf "%.0f\n", v * 1024 * 1024 * 1024 }'
    elif [[ "$mem" =~ ^([0-9]+([.][0-9]+)?)(MIB|M)$ ]]; then
        value="${BASH_REMATCH[1]}"
        awk -v v="$value" 'BEGIN { printf "%.0f\n", v * 1024 * 1024 }'
    elif [[ "$mem" =~ ^([0-9]+([.][0-9]+)?)(KIB|K)$ ]]; then
        value="${BASH_REMATCH[1]}"
        awk -v v="$value" 'BEGIN { printf "%.0f\n", v * 1024 }'
    elif [[ "$mem" =~ ^([0-9]+([.][0-9]+)?)B?$ ]]; then
        value="${BASH_REMATCH[1]}"
        awk -v v="$value" 'BEGIN { printf "%.0f\n", v }'
    else
        return 1
    fi
}

run_with_cgroup_limits() {
    local memory_limit="$1"
    shift

    if [[ "$CGROUP_MODE" == "off" ]]; then
        "$@"
        return $?
    fi

    if [[ "$CGROUP_MODE" != "on" ]]; then
        echo "[cgroup] ERROR: invalid CGROUP_MODE='$CGROUP_MODE' (expected on|off)." >&2
        return 1
    fi

    local limit_bytes
    if ! limit_bytes=$(parse_memory_to_bytes "$memory_limit"); then
        echo "[cgroup] ERROR: cannot parse memory limit '$memory_limit'." >&2
        return 1
    fi

    if [[ "$(uname -s)" != "Linux" ]]; then
        echo "[cgroup] ERROR: CGROUP_MODE=on requires Linux." >&2
        return 1
    fi
    if ! command -v systemd-run >/dev/null 2>&1; then
        echo "[cgroup] ERROR: CGROUP_MODE=on requires systemd-run." >&2
        return 1
    fi

    local unit_name="es-duck-clickhouse-sweep-$$-$(date +%s)"
    echo "[cgroup] Applying limits: memory.high=${limit_bytes}, memory.max=${limit_bytes}, memory.swap.max=0"

    # Probe cgroup scope creation with a no-op
    if ! systemd-run --user --scope --quiet --collect \
        --unit "${unit_name}-probe" \
        --property "MemoryAccounting=yes" \
        --property "MemoryHigh=${limit_bytes}" \
        --property "MemoryMax=${limit_bytes}" \
        --property "MemorySwapMax=0" \
        -- true 2>/dev/null; then
        echo "[cgroup] ERROR: failed to create cgroup scope (setup error)." >&2
        return 1
    fi

    # Run the actual command
    systemd-run --user --scope --quiet --collect \
        --unit "${unit_name}" \
        --property "MemoryAccounting=yes" \
        --property "MemoryHigh=${limit_bytes}" \
        --property "MemoryMax=${limit_bytes}" \
        --property "MemorySwapMax=0" \
        -- "$@"
}

upload_log_file() {
    local log_file="$1"

    if [ -z "$RCLONE_REMOTE" ]; then
        return 0
    fi
    if ! command -v rclone >/dev/null 2>&1; then
        echo "[upload] rclone not found, skipping per-run upload."
        return 0
    fi

    local remote_dir="${RCLONE_REMOTE}/$(basename "$LOG_DIR")"
    local remote_file="${remote_dir}/$(basename "$log_file")"

    echo "[upload] Uploading log file $log_file -> $remote_file ..."
    if rclone copyto "$log_file" "$remote_file" --progress; then
        echo "[upload] Upload complete: $remote_file"
    else
        echo "[upload] Warning: per-run upload failed (exit $?)" >&2
    fi
}

echo "=== ClickHouse Memory Sweep ==="
echo "Input: $INPUT_FILE"
echo "Format: $FORMAT"
echo "ClickHouse URL: $CLICKHOUSE_URL"
echo "Database: $DATABASE"
echo "Table: $TABLE"
echo "Threads: $THREADS"
echo "Memory limits: $MEMORY_LIMITS"
echo "Cgroup mode: $CGROUP_MODE"
echo "Timeout: ${TIMEOUT_SECONDS}s"
echo "Benchmark runs per config: $BENCHMARK_RUNS"
echo "Log directory: $LOG_DIR"
if [ -n "$OUTPUT" ]; then
    echo "Mode: Binary output to $OUTPUT"
else
    echo "Mode: Count (no output)"
fi
echo ""

# Create log directory
mkdir -p "$LOG_DIR"

# Check if table exists by querying system.tables
TABLE_EXISTS=$(curl -sS "${CLICKHOUSE_URL}/?query=SELECT%20count(*)%20FROM%20system.tables%20WHERE%20database%20=%20'${DATABASE}'%20AND%20name%20=%20'${TABLE}'" 2>/dev/null || echo "0")

# Load database if table doesn't exist
if [ "$TABLE_EXISTS" = "0" ]; then
    echo "Loading data into ClickHouse..."
    echo "NOTE: This will show output in real-time..."

    timeout $TIMEOUT_SECONDS cargo run --release --bin load-clickhouse --features db-clickhouse -- \
        --format "$FORMAT" \
        --input "$INPUT_FILE" \
        --url "$CLICKHOUSE_URL" \
        --database "$DATABASE" \
        --table "$TABLE" \
        --threads "$THREADS" || {
        echo "ERROR: Database loading failed or timed out"
        exit 1
    }

    echo "Syncing..."
    sync
    echo ""
fi

# Run sort for each memory limit
SKIP_ALL_REMAINING=false
for MEM in $MEMORY_LIMITS; do
  # Skip remaining memory limits if previous configuration failed
  if [ "$SKIP_ALL_REMAINING" = true ]; then
    echo "Skipping $MEM - previous memory limit failed"
    continue
  fi

  for RUN in $(seq 1 $BENCHMARK_RUNS); do
    RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    # Create individual log file for this configuration with run number suffix
    LOG_FILE="${LOG_DIR}/${THREADS}threads_${MEM}_run${RUN}_${RUN_TIMESTAMP}.log"
    TEMP_OUTPUT="/tmp/clickhouse_sweep_${MEM}_run${RUN}_${RUN_TIMESTAMP}.log"

    echo "========================================="
    echo "Running with $MEM memory limit... (Run $RUN of $BENCHMARK_RUNS)"
    echo "Start time: $(date +"%Y-%m-%d %H:%M:%S")"
    echo "========================================="
    echo "Log file: $LOG_FILE"
    echo "NOTE: Output will appear in real-time below..."
    echo ""

    # Run with timeout and show output in real-time using tee
    # Note: When CGROUP_MODE=on, applies OS-level memory limits to the client process
    set +e
    if [ -n "$OUTPUT" ]; then
        # Binary output mode: truncate output file first in case it exists
        if [ -f "$OUTPUT" ]; then
            echo "Truncating existing output file..."
            > "$OUTPUT"
            sync
        fi

        run_with_cgroup_limits "$MEM" \
            timeout $TIMEOUT_SECONDS cargo run --release --bin sort-clickhouse --features db-clickhouse -- \
                --url "$CLICKHOUSE_URL" \
                --database "$DATABASE" \
                --table "$TABLE" \
                --threads "$THREADS" \
                --memory-limit "$MEM" \
                --output "$OUTPUT" 2>&1 | tee "$TEMP_OUTPUT"
    else
        # Count mode
        run_with_cgroup_limits "$MEM" \
            timeout $TIMEOUT_SECONDS cargo run --release --bin sort-clickhouse --features db-clickhouse -- \
                --url "$CLICKHOUSE_URL" \
                --database "$DATABASE" \
                --table "$TABLE" \
                --threads "$THREADS" \
                --memory-limit "$MEM" 2>&1 | tee "$TEMP_OUTPUT"
    fi

    EXIT_CODE=${PIPESTATUS[0]}
    set -e

    # Read captured output
    COMMAND_OUTPUT=$(cat "$TEMP_OUTPUT")

    # Check timeout or OOM kill - skip remaining runs for this configuration
    # Also check for ClickHouse memory limit errors in output
    if [ $EXIT_CODE -eq 124 ]; then
        echo ""
        echo "WARNING: Process timed out after ${TIMEOUT_SECONDS}s"
        echo "Skipping remaining benchmark runs for $MEM memory limit..."
        SKIP_REMAINING=true
    elif [ $EXIT_CODE -eq 137 ]; then
        echo ""
        echo "WARNING: Process killed by memory manager (OOM kill, exit 137)"
        echo "Skipping remaining benchmark runs for $MEM memory limit..."
        SKIP_REMAINING=true
    elif echo "$COMMAND_OUTPUT" | grep -qiE "(MEMORY_LIMIT_EXCEEDED|memory limit exceeded|out of memory)"; then
        echo ""
        echo "WARNING: ClickHouse encountered memory limit error (detected in output)"
        echo "Skipping remaining benchmark runs for $MEM memory limit..."
        SKIP_REMAINING=true
        # Treat as OOM for logging purposes
        if [ $EXIT_CODE -eq 0 ]; then
            EXIT_CODE=137
        fi
    else
        SKIP_REMAINING=false
    fi

    # Clear ClickHouse's internal caches using SYSTEM DROP commands
    echo "Clearing ClickHouse caches..."
    curl -sS "${CLICKHOUSE_URL}/?query=SYSTEM%20DROP%20MARK%20CACHE" >/dev/null 2>&1 || true
    curl -sS "${CLICKHOUSE_URL}/?query=SYSTEM%20DROP%20UNCOMPRESSED%20CACHE" >/dev/null 2>&1 || true
    curl -sS "${CLICKHOUSE_URL}/?query=SYSTEM%20DROP%20COMPILED%20EXPRESSION%20CACHE" >/dev/null 2>&1 || true

    # Extract timing from output
    DURATION=$(echo "$COMMAND_OUTPUT" | grep "TIMING:" | awk '{print $2}' || true)

    # Write detailed log to individual file
    {
        echo "========================================="
        echo "ClickHouse Memory Sweep - Configuration Log"
        echo "========================================="
        echo "Configuration: memory_limit=$MEM, threads=$THREADS, run=$RUN/$BENCHMARK_RUNS"
        echo "Input: $INPUT_FILE"
        echo "ClickHouse URL: $CLICKHOUSE_URL"
        echo "Database: $DATABASE"
        echo "Table: $TABLE"
        echo "Timeout: ${TIMEOUT_SECONDS}s"
        echo "Start time: $(date +"%Y-%m-%d %H:%M:%S")"
        echo ""
        echo "Exit code: $EXIT_CODE"
        if [ $EXIT_CODE -eq 0 ]; then
            echo "Status: SUCCESS"
        elif [ $EXIT_CODE -eq 124 ]; then
            echo "Status: TIMEOUT"
        elif [ $EXIT_CODE -eq 137 ]; then
            echo "Status: OOM_KILLED"
        else
            echo "Status: FAILED"
        fi
        echo ""
        echo "========================================="
        echo "Full output:"
        echo "========================================="
        echo "$COMMAND_OUTPUT"
        echo ""
        echo "========================================="
        echo "Summary:"
        echo "========================================="
        if [ -n "$DURATION" ]; then
            echo "Duration: ${DURATION}s"
            echo "Result: $MEM,$THREADS,$RUN,$DURATION"
        else
            echo "WARNING: Could not extract timing information"
        fi
        echo "End time: $(date +"%Y-%m-%d %H:%M:%S")"
        echo "========================================="
    } > "$LOG_FILE"

    upload_log_file "$LOG_FILE"

    # Clean up temp output file
    rm -f "$TEMP_OUTPUT"

    # Report results
    echo ""
    echo "========================================="
    if [ -n "$DURATION" ]; then
        echo "✓ Result logged: memory_limit=$MEM, threads=$THREADS, run=$RUN/$BENCHMARK_RUNS, duration=${DURATION}s"
    else
        echo "✗ Warning: Could not extract timing information"
    fi
    echo "End time: $(date +"%Y-%m-%d %H:%M:%S")"
    echo "========================================="

    # Clean up binary output file if it exists
    if [ -n "$OUTPUT" ] && [ -f "$OUTPUT" ]; then
        echo "Cleaning up output file..."
        OUTPUT_SIZE=$(du -sh "$OUTPUT" 2>/dev/null | cut -f1 || echo "unknown")
        echo "Output file size: $OUTPUT_SIZE"
        # Truncate first in case file is still open
        > "$OUTPUT"
        rm -f "$OUTPUT"
        sync
        echo "Output file removed and synced."
    fi

    # Clear system caches
    if [ -f "$CLEAR_CACHE_SCRIPT" ]; then
        echo "Clearing system caches..."
        if ! sudo "$CLEAR_CACHE_SCRIPT" 2>&1; then
            echo "ERROR: Failed to clear caches (exit code: $?)" >&2
            echo "This may impact benchmark accuracy" >&2
        fi
    else
        echo "Warning: Cache clear script not found: $CLEAR_CACHE_SCRIPT"
    fi

    # Skip remaining benchmark runs for this configuration if timeout occurred
    if [ "$SKIP_REMAINING" = true ]; then
        SKIP_ALL_REMAINING=true
        echo "Configuration failed - skipping all remaining lower memory limits..."
        break
    fi

    echo ""
    echo "Waiting 30 seconds before next run..."
    sync
    sleep 30
    echo ""
  done
done

echo "=== Sweep Complete ==="
echo "Results saved to logs in: $LOG_DIR"
echo ""
echo "Summary of results:"
grep "Result:" "$LOG_DIR"/*.log 2>/dev/null || echo "No successful results found"
