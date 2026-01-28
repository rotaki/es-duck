#!/bin/bash
# ClickHouse parallelism sweep: vary thread count at fixed memory budget

set -e

# Generate timestamp for this sweep run
SWEEP_TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Configuration
INPUT_FILE="${INPUT_FILE:-testdata/test_gensort.dat}"
FORMAT="${FORMAT:-gensort}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
DATABASE="${DATABASE:-default}"
TABLE="${TABLE:-bench_data}"
# Support both TOTAL_MEMORY and MEMORY_LIMIT (backward compatibility)
TOTAL_MEMORY="${TOTAL_MEMORY:-${MEMORY_LIMIT:-2GB}}"
# THREAD_COUNTS="${THREAD_COUNTS:-4 8 16 24 32 40 44}"
THREAD_COUNTS="${THREAD_COUNTS:-4}"
LOG_DIR="${LOG_DIR:-./logs/clickhouse_parallelism_sweep_${SWEEP_TIMESTAMP}}"
OUTPUT="${OUTPUT:-}"  # Optional output path for binary mode
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-7200}"  # 2 hour default timeout

echo "=== ClickHouse Parallelism Sweep ==="
echo "Input: $INPUT_FILE"
echo "Format: $FORMAT"
echo "ClickHouse URL: $CLICKHOUSE_URL"
echo "Database: $DATABASE"
echo "Table: $TABLE"
echo "Total memory budget: $TOTAL_MEMORY"
echo "Thread counts: $THREAD_COUNTS"
echo "Timeout: ${TIMEOUT_SECONDS}s"
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
    cargo run --release --bin load-clickhouse --features db-clickhouse -- \
        --format "$FORMAT" \
        --input "$INPUT_FILE" \
        --url "$CLICKHOUSE_URL" \
        --database "$DATABASE" \
        --table "$TABLE" \
        --threads 14

    echo "Syncing..."
    sync
    echo ""
fi

# Run sort for each thread count
for THREADS in $THREAD_COUNTS; do
    RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    # Create individual log file for this configuration
    LOG_FILE="${LOG_DIR}/${TOTAL_MEMORY}_${THREADS}threads_${RUN_TIMESTAMP}.log"

    echo "========================================="
    echo "Running with $THREADS threads..."
    echo "========================================="
    echo "Log file: $LOG_FILE"

    # Capture start time before running command
    START_TIME=$(date +"%Y-%m-%d %H:%M:%S")

    # Run and capture output and exit code
    set +e
    if [ -n "$OUTPUT" ]; then
        # Binary output mode: truncate output file first in case it exists
        if [ -f "$OUTPUT" ]; then
            echo "Truncating existing output file..."
            > "$OUTPUT"
            sync
        fi

        COMMAND_OUTPUT=$(timeout $TIMEOUT_SECONDS cargo run --release --bin sort-clickhouse --features db-clickhouse -- \
            --url "$CLICKHOUSE_URL" \
            --database "$DATABASE" \
            --table "$TABLE" \
            --memory-limit "$TOTAL_MEMORY" \
            --threads "$THREADS" \
            --output "$OUTPUT" 2>&1)
    else
        # Count mode
        COMMAND_OUTPUT=$(timeout $TIMEOUT_SECONDS cargo run --release --bin sort-clickhouse --features db-clickhouse -- \
            --url "$CLICKHOUSE_URL" \
            --database "$DATABASE" \
            --table "$TABLE" \
            --memory-limit "$TOTAL_MEMORY" \
            --threads "$THREADS" 2>&1)
    fi
    EXIT_CODE=$?
    set -e

    echo "$COMMAND_OUTPUT"

    # Check timeout
    if [ $EXIT_CODE -eq 124 ]; then
        echo ""
        echo "WARNING: Process timed out after ${TIMEOUT_SECONDS}s"
    fi

    # Clear ClickHouse's internal caches using SYSTEM DROP commands
    echo "Clearing ClickHouse caches..."
    curl -sS "${CLICKHOUSE_URL}/?query=SYSTEM%20DROP%20MARK%20CACHE" >/dev/null 2>&1 || true
    curl -sS "${CLICKHOUSE_URL}/?query=SYSTEM%20DROP%20UNCOMPRESSED%20CACHE" >/dev/null 2>&1 || true
    curl -sS "${CLICKHOUSE_URL}/?query=SYSTEM%20DROP%20COMPILED%20EXPRESSION%20CACHE" >/dev/null 2>&1 || true

    # Extract timing from output
    DURATION=$(echo "$COMMAND_OUTPUT" | grep "TIMING:" | awk '{print $2}')

    # Write detailed log to individual file
    {
        echo "========================================="
        echo "ClickHouse Parallelism Sweep - Configuration Log"
        echo "========================================="
        echo "Configuration: memory_limit=$TOTAL_MEMORY, threads=$THREADS"
        echo "Input: $INPUT_FILE"
        echo "ClickHouse URL: $CLICKHOUSE_URL"
        echo "Database: $DATABASE"
        echo "Table: $TABLE"
        echo "Timeout: ${TIMEOUT_SECONDS}s"
        echo "Start time: $START_TIME"
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
            echo "Result: $TOTAL_MEMORY,$THREADS,$DURATION"
        else
            echo "WARNING: Could not extract timing information"
        fi
        echo "End time: $(date +"%Y-%m-%d %H:%M:%S")"
        echo "========================================="
    } > "$LOG_FILE"

    # Report results
    if [ -n "$DURATION" ]; then
        echo "Result logged: memory_limit=$TOTAL_MEMORY, threads=$THREADS, duration=${DURATION}s"
    else
        echo "Warning: Could not extract timing information"
    fi

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

    echo ""
    echo "Waiting 30 seconds before next run..."
    sync
    sleep 30
    echo ""
done

echo "=== Sweep Complete ==="
echo "Results saved to logs in: $LOG_DIR"
echo ""
echo "Summary of results:"
find "$LOG_DIR" -name "*.log" -exec grep "Result:" {} + 2>/dev/null || echo "No successful results found"
