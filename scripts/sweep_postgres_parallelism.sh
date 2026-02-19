#!/bin/bash
# PostgreSQL parallelism sweep: vary parallel workers at fixed memory budget
# FIXED VERSION: Shows real-time output and has timeout protection

set -e

# Generate timestamp for this sweep run
SWEEP_TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Configuration
INPUT_FILE="${INPUT_FILE:-testdata/test_gensort.dat}"
FORMAT="${FORMAT:-gensort}"
DB_CONNECTION="${DB_CONNECTION:-postgres://postgres@localhost:5433/bench}"
TABLE="${TABLE:-bench_data}"
# Support both TOTAL_MEMORY and WORK_MEM (backward compatibility)
TOTAL_MEMORY="${TOTAL_MEMORY:-${WORK_MEM:-2GB}}"
# WORKER_COUNTS="${WORKER_COUNTS:-4 8 16 24 32 40 44}"
WORKER_COUNTS="${WORKER_COUNTS:-4}"
LOG_DIR="${LOG_DIR:-./logs/postgres_parallelism_sweep_${SWEEP_TIMESTAMP}}"
OUTPUT="${OUTPUT:-}"  # Optional output path for binary mode
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-7200}"  # 2 hour default timeout
BENCHMARK_RUNS="${BENCHMARK_RUNS:-1}"  # Number of times to run each configuration
CLEAR_CACHE_SCRIPT="/usr/local/sbin/clearcache3.sh"

echo "=== PostgreSQL Parallelism Sweep ==="
echo "Input: $INPUT_FILE"
echo "Format: $FORMAT"
echo "Database: $DB_CONNECTION"
echo "Table: $TABLE"
echo "Total memory budget: $TOTAL_MEMORY"
echo "Worker counts: $WORKER_COUNTS"
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

# Run sort for each worker count
for W in $WORKER_COUNTS; do
  for RUN in $(seq 1 $BENCHMARK_RUNS); do
    RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    # Create individual log file for this configuration with run number suffix
    LOG_FILE="${LOG_DIR}/${TOTAL_MEMORY}_${W}workers_run${RUN}_${RUN_TIMESTAMP}.log"
    TEMP_OUTPUT="/tmp/postgres_sweep_${W}_run${RUN}_${RUN_TIMESTAMP}.log"

    echo "========================================="
    echo "Running with $W parallel workers... (Run $RUN of $BENCHMARK_RUNS)"
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

        timeout $TIMEOUT_SECONDS cargo run --release --bin sort-postgres --features db-postgres -- \
            --db "$DB_CONNECTION" \
            --table "$TABLE" \
            --total-memory "$TOTAL_MEMORY" \
            --parallel-workers "$W" \
            --output "$OUTPUT" 2>&1 | tee "$TEMP_OUTPUT"
    else
        # Count mode
        timeout $TIMEOUT_SECONDS cargo run --release --bin sort-postgres --features db-postgres -- \
            --db "$DB_CONNECTION" \
            --table "$TABLE" \
            --total-memory "$TOTAL_MEMORY" \
            --parallel-workers "$W" 2>&1 | tee "$TEMP_OUTPUT"
    fi

    EXIT_CODE=${PIPESTATUS[0]}
    set -e

    # Read captured output
    COMMAND_OUTPUT=$(cat "$TEMP_OUTPUT")

    # Check timeout - skip remaining runs for this configuration
    if [ $EXIT_CODE -eq 124 ]; then
        echo ""
        echo "WARNING: Process timed out after ${TIMEOUT_SECONDS}s"
        echo "Skipping remaining benchmark runs for $W workers..."
        SKIP_REMAINING=true
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
        echo "PostgreSQL Parallelism Sweep - Configuration Log"
        echo "========================================="
        echo "Configuration: total_memory=$TOTAL_MEMORY, parallel_workers=$W, run=$RUN/$BENCHMARK_RUNS"
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
            echo "Result: $TOTAL_MEMORY,$W,$RUN,$DURATION"
        else
            echo "WARNING: Could not extract timing information"
        fi
        echo "End time: $(date +"%Y-%m-%d %H:%M:%S")"
        echo "========================================="
    } > "$LOG_FILE"

    # Clean up temp output file
    rm -f "$TEMP_OUTPUT"

    # Report results
    echo ""
    echo "========================================="
    if [ -n "$DURATION" ]; then
        echo "✓ Result logged: total_memory=$TOTAL_MEMORY, parallel_workers=$W, run=$RUN/$BENCHMARK_RUNS, duration=${DURATION}s"
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
    if [ -x "$CLEAR_CACHE_SCRIPT" ]; then
        echo "Clearing system caches..."
        sudo "$CLEAR_CACHE_SCRIPT" || echo "Warning: Failed to clear caches"
    else
        echo "Warning: Cache clear script not found or not executable: $CLEAR_CACHE_SCRIPT"
    fi

    # Skip remaining benchmark runs for this configuration if timeout occurred
    if [ "$SKIP_REMAINING" = true ]; then
        echo "Moving to next configuration..."
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
