#!/bin/bash
# PostgreSQL parallelism sweep: vary parallel workers at fixed memory budget

set -e

# Configuration
INPUT_FILE="${INPUT_FILE:-testdata/test_gensort.dat}"
FORMAT="${FORMAT:-gensort}"
DB_CONNECTION="${DB_CONNECTION:-postgres://postgres@localhost:5433/bench}"
TABLE="${TABLE:-bench_data}"
WORK_MEM="${WORK_MEM:-2GB}"
WORKER_COUNTS="${WORKER_COUNTS:-4 8 16 24 32 40 44}"
LOG_DIR="${LOG_DIR:-./logs/postgres_parallelism_sweep}"

echo "=== PostgreSQL Parallelism Sweep ==="
echo "Input: $INPUT_FILE"
echo "Format: $FORMAT"
echo "Database: $DB_CONNECTION"
echo "Table: $TABLE"
echo "work_mem: $WORK_MEM"
echo "Worker counts: $WORKER_COUNTS"
echo "Log directory: $LOG_DIR"
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
    LOG_FILE="${LOG_DIR}/${WORK_MEM}_${W}workers_${RUN_TIMESTAMP}.log"

    echo "========================================="
    echo "Running with $W parallel workers..."
    echo "========================================="
    echo "Log file: $LOG_FILE"

    # Run and capture output and exit code
    set +e
    OUTPUT=$(cargo run --release --bin sort-postgres -- \
        --db "$DB_CONNECTION" \
        --table "$TABLE" \
        --total-memory "$WORK_MEM" \
        --parallel-workers "$W" 2>&1)
    EXIT_CODE=$?
    set -e

    echo "$OUTPUT"

    # Extract timing from output
    DURATION=$(echo "$OUTPUT" | grep "TIMING:" | awk '{print $2}')

    # Write detailed log to individual file
    {
        echo "========================================="
        echo "PostgreSQL Parallelism Sweep - Configuration Log"
        echo "========================================="
        echo "Configuration: work_mem=$WORK_MEM, parallel_workers=$W"
        echo "Input: $INPUT_FILE"
        echo "Database: $DB_CONNECTION"
        echo "Table: $TABLE"
        echo "Start time: $(date +"%Y-%m-%d %H:%M:%S")"
        echo ""
        echo "Exit code: $EXIT_CODE"
        if [ $EXIT_CODE -eq 0 ]; then
            echo "Status: SUCCESS"
        else
            echo "Status: FAILED"
        fi
        echo ""
        echo "========================================="
        echo "Full output:"
        echo "========================================="
        echo "$OUTPUT"
        echo ""
        echo "========================================="
        echo "Summary:"
        echo "========================================="
        if [ -n "$DURATION" ]; then
            echo "Duration: ${DURATION}s"
            echo "Result: $WORK_MEM,$W,$DURATION"
        else
            echo "WARNING: Could not extract timing information"
        fi
        echo "End time: $(date +"%Y-%m-%d %H:%M:%S")"
        echo "========================================="
    } > "$LOG_FILE"

    # Report results
    if [ -n "$DURATION" ]; then
        echo "Result logged: work_mem=$WORK_MEM, parallel_workers=$W, duration=${DURATION}s"
    else
        echo "Warning: Could not extract timing information"
    fi

    echo ""
done

echo "=== Sweep Complete ==="
echo "Results saved to logs in: $LOG_DIR"
