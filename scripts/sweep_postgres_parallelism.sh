#!/bin/bash
# PostgreSQL parallelism sweep: vary parallel workers at fixed memory budget

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

echo "=== PostgreSQL Parallelism Sweep ==="
echo "Input: $INPUT_FILE"
echo "Format: $FORMAT"
echo "Database: $DB_CONNECTION"
echo "Table: $TABLE"
echo "Total memory budget: $TOTAL_MEMORY"
echo "Worker counts: $WORKER_COUNTS"
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
    cargo run --release --bin load-postgres -- \
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
    RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    # Create individual log file for this configuration
    LOG_FILE="${LOG_DIR}/${TOTAL_MEMORY}_${W}workers_${RUN_TIMESTAMP}.log"

    echo "========================================="
    echo "Running with $W parallel workers..."
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

        COMMAND_OUTPUT=$(timeout $TIMEOUT_SECONDS cargo run --release --bin sort-postgres -- \
            --db "$DB_CONNECTION" \
            --table "$TABLE" \
            --total-memory "$TOTAL_MEMORY" \
            --parallel-workers "$W" \
            --output "$OUTPUT" 2>&1)
    else
        # Count mode
        COMMAND_OUTPUT=$(timeout $TIMEOUT_SECONDS cargo run --release --bin sort-postgres -- \
            --db "$DB_CONNECTION" \
            --table "$TABLE" \
            --total-memory "$TOTAL_MEMORY" \
            --parallel-workers "$W" 2>&1)
    fi
    EXIT_CODE=$?
    set -e

    echo "$COMMAND_OUTPUT"

    # Check timeout
    if [ $EXIT_CODE -eq 124 ]; then
        echo ""
        echo "WARNING: Process timed out after ${TIMEOUT_SECONDS}s"
    fi

    # Clear PostgreSQL's internal caches
    echo "Clearing PostgreSQL caches..."
    psql "$DB_CONNECTION" -c "DISCARD ALL" >/dev/null 2>&1 || true

    # Extract timing from output
    DURATION=$(echo "$COMMAND_OUTPUT" | grep "TIMING:" | awk '{print $2}')

    # Write detailed log to individual file
    {
        echo "========================================="
        echo "PostgreSQL Parallelism Sweep - Configuration Log"
        echo "========================================="
        echo "Configuration: total_memory=$TOTAL_MEMORY, parallel_workers=$W"
        echo "Input: $INPUT_FILE"
        echo "Database: $DB_CONNECTION"
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
            echo "Result: $TOTAL_MEMORY,$W,$DURATION"
        else
            echo "WARNING: Could not extract timing information"
        fi
        echo "End time: $(date +"%Y-%m-%d %H:%M:%S")"
        echo "========================================="
    } > "$LOG_FILE"

    # Report results
    if [ -n "$DURATION" ]; then
        echo "Result logged: total_memory=$TOTAL_MEMORY, parallel_workers=$W, duration=${DURATION}s"
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
