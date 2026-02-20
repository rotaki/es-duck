#!/bin/bash
# PostgreSQL memory sweep: vary work_mem at fixed parallel worker count
# FIXED VERSION: Shows real-time output and has timeout protection

set -e

# Generate timestamp for this sweep run
SWEEP_TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Configuration
INPUT_FILE="${INPUT_FILE:-testdata/test_gensort_5gb.dat}"
FORMAT="${FORMAT:-gensort}"
DB_CONNECTION="${DB_CONNECTION:-postgres://postgres@localhost:5433/bench}"
TABLE="${TABLE:-bench_data}"
PARALLEL_WORKERS="${PARALLEL_WORKERS:-15}"  # Default: 16 threads - 1 leader = 15 workers
# MEMORY_LIMITS="${MEMORY_LIMITS:-2GiB 4GiB 6GiB 8GiB 16GiB 24GiB 32GiB 48GiB}"
MEMORY_LIMITS="${MEMORY_LIMITS:-48GiB 32GiB 24GiB 16GiB 8GiB 4GiB 2GiB}"
LOG_DIR="${LOG_DIR:-./logs/postgres_memory_sweep_${SWEEP_TIMESTAMP}}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-7200}"  # 2 hour default timeout
BENCHMARK_RUNS="${BENCHMARK_RUNS:-1}"  # Number of times to run each configuration
CLEAR_CACHE_SCRIPT="/usr/local/sbin/clearcache3.sh"
TEMP_TABLESPACE="${TEMP_TABLESPACE:-}"  # Optional temp tablespace for spilling
OUTPUT="${OUTPUT:-}"  # Optional output path for binary mode
RCLONE_REMOTE="${RCLONE_REMOTE:-}"

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

echo "=== PostgreSQL Memory Sweep ==="
echo "Input: $INPUT_FILE"
echo "Format: $FORMAT"
echo "Database: $DB_CONNECTION"
echo "Table: $TABLE"
echo "Parallel workers: $PARALLEL_WORKERS (Total processes: $((PARALLEL_WORKERS + 1)))"
echo "Memory limits: $MEMORY_LIMITS"
echo "Timeout: ${TIMEOUT_SECONDS}s"
echo "Benchmark runs per config: $BENCHMARK_RUNS"
echo "Log directory: $LOG_DIR"
if [ -n "$TEMP_TABLESPACE" ]; then
    echo "Temp tablespace: $TEMP_TABLESPACE"
fi
if [ -n "$OUTPUT" ]; then
    echo "Mode: Binary output to $OUTPUT"
else
    echo "Mode: Count (no output)"
fi
echo ""

# Create log directory
mkdir -p "$LOG_DIR"

# Extract database name from connection string
DB_NAME=$(echo "$DB_CONNECTION" | sed -n 's|.*://.*@.*/\([^?]*\).*|\1|p' || echo "$DB_CONNECTION" | sed -n 's|.*://[^/]*/\([^?]*\).*|\1|p')
# Extract connection string without database name for creating database
DB_CONN_BASE=$(echo "$DB_CONNECTION" | sed 's|/[^/]*$|/postgres|')

# Check if database exists, create if needed
DB_EXISTS=$(psql "$DB_CONN_BASE" -tAc "SELECT EXISTS (SELECT FROM pg_database WHERE datname = '$DB_NAME')" 2>/dev/null || echo "f")
if [ "$DB_EXISTS" = "f" ]; then
    echo "Creating database '$DB_NAME'..."
    psql "$DB_CONN_BASE" -c "CREATE DATABASE $DB_NAME" >/dev/null
    echo "Database created."
fi

# Check if table exists
TABLE_EXISTS=$(psql "$DB_CONNECTION" -tAc "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '$TABLE')" 2>/dev/null || echo "f")

# Load database if table doesn't exist
if [ "$TABLE_EXISTS" = "f" ]; then
    echo "Loading data into PostgreSQL..."
    cargo run --release --bin load-postgres --features db-postgres -- \
        --format "$FORMAT" \
        --input "$INPUT_FILE" \
        --db "$DB_CONNECTION" \
        --table "$TABLE" \
        --threads 14

    echo "Running CHECKPOINT..."
    psql "$DB_CONNECTION" -c "CHECKPOINT" >/dev/null
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
    LOG_FILE="${LOG_DIR}/${PARALLEL_WORKERS}workers_${MEM}_run${RUN}_${RUN_TIMESTAMP}.log"
    TEMP_OUTPUT="/tmp/postgres_sweep_${MEM}_run${RUN}_${RUN_TIMESTAMP}.log"

    echo "========================================="
    echo "Running with $MEM work_mem... (Run $RUN of $BENCHMARK_RUNS)"
    echo "Start time: $(date +"%Y-%m-%d %H:%M:%S")"
    echo "========================================="
    echo "Log file: $LOG_FILE"
    echo "NOTE: Output will appear in real-time below..."
    echo ""

    # Run with timeout and show output in real-time using tee
    set +e
    if [ -n "$OUTPUT" ]; then
        # Binary output mode: truncate output file first in case it exists
        if [ -f "$OUTPUT" ]; then
            echo "Truncating existing output file..."
            > "$OUTPUT"
            sync
        fi

        if [ -n "$TEMP_TABLESPACE" ]; then
            timeout $TIMEOUT_SECONDS cargo run --release --bin sort-postgres --features db-postgres -- \
                --db "$DB_CONNECTION" \
                --table "$TABLE" \
                --total-memory "$MEM" \
                --parallel-workers "$PARALLEL_WORKERS" \
                --temp-tablespace "$TEMP_TABLESPACE" \
                --output "$OUTPUT" 2>&1 | tee "$TEMP_OUTPUT"
        else
            timeout $TIMEOUT_SECONDS cargo run --release --bin sort-postgres --features db-postgres -- \
                --db "$DB_CONNECTION" \
                --table "$TABLE" \
                --total-memory "$MEM" \
                --parallel-workers "$PARALLEL_WORKERS" \
                --output "$OUTPUT" 2>&1 | tee "$TEMP_OUTPUT"
        fi
    else
        # Count mode
        if [ -n "$TEMP_TABLESPACE" ]; then
            timeout $TIMEOUT_SECONDS cargo run --release --bin sort-postgres --features db-postgres -- \
                --db "$DB_CONNECTION" \
                --table "$TABLE" \
                --total-memory "$MEM" \
                --parallel-workers "$PARALLEL_WORKERS" \
                --temp-tablespace "$TEMP_TABLESPACE" 2>&1 | tee "$TEMP_OUTPUT"
        else
            timeout $TIMEOUT_SECONDS cargo run --release --bin sort-postgres --features db-postgres -- \
                --db "$DB_CONNECTION" \
                --table "$TABLE" \
                --total-memory "$MEM" \
                --parallel-workers "$PARALLEL_WORKERS" 2>&1 | tee "$TEMP_OUTPUT"
        fi
    fi

    EXIT_CODE=${PIPESTATUS[0]}
    set -e

    # Read captured output
    COMMAND_OUTPUT=$(cat "$TEMP_OUTPUT")

    # Check timeout or OOM kill - skip remaining runs for this configuration
    # Also check for memory errors in output (PostgreSQL connection closed, out of memory, etc)
    if [ $EXIT_CODE -eq 124 ]; then
        echo ""
        echo "WARNING: Process timed out after ${TIMEOUT_SECONDS}s"
        echo "Skipping remaining benchmark runs for $MEM work_mem..."
        SKIP_REMAINING=true
    elif [ $EXIT_CODE -eq 137 ]; then
        echo ""
        echo "WARNING: Process killed by memory manager (OOM kill, exit 137)"
        echo "Skipping remaining benchmark runs for $MEM work_mem..."
        SKIP_REMAINING=true
    elif echo "$COMMAND_OUTPUT" | grep -qiE "(out of memory|memory limit|FATAL.*memory|could not allocate|Error.*Closed)"; then
        echo ""
        echo "WARNING: Process encountered memory error (detected in output)"
        echo "Skipping remaining benchmark runs for $MEM work_mem..."
        SKIP_REMAINING=true
        # Treat as OOM for logging purposes
        if [ $EXIT_CODE -eq 0 ]; then
            EXIT_CODE=137
        fi
    else
        SKIP_REMAINING=false
    fi

    # Clear PostgreSQL's internal caches
    echo "Clearing PostgreSQL caches..."
    psql "$DB_CONNECTION" -c "DISCARD ALL" >/dev/null 2>&1 || true

    # Extract timing from output
    DURATION=$(echo "$COMMAND_OUTPUT" | grep "TIMING:" | awk '{print $2}' || true)

    # Write detailed log to individual file
    {
        echo "========================================="
        echo "PostgreSQL Memory Sweep - Configuration Log"
        echo "========================================="
        echo "Configuration: work_mem=$MEM, parallel_workers=$PARALLEL_WORKERS, run=$RUN/$BENCHMARK_RUNS"
        echo "Input: $INPUT_FILE"
        echo "Database: $DB_CONNECTION"
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
            echo "Result: $MEM,$PARALLEL_WORKERS,$RUN,$DURATION"
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
        echo "✓ Result logged: work_mem=$MEM, parallel_workers=$PARALLEL_WORKERS, run=$RUN/$BENCHMARK_RUNS, duration=${DURATION}s"
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
