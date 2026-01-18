#!/bin/bash
# PostgreSQL memory sweep: vary work_mem at fixed parallel worker count

set -e

# Configuration
INPUT_FILE="${INPUT_FILE:-testdata/test_gensort_5gb.dat}"
FORMAT="${FORMAT:-gensort}"
DB_CONNECTION="${DB_CONNECTION:-postgres://postgres@localhost:5433/bench}"
TABLE="${TABLE:-bench_data}"
OUTPUT_BASE="${OUTPUT_BASE:-./postgres_sorted/result}"
PARALLEL_WORKERS="${PARALLEL_WORKERS:-40}"
# MEMORY_LIMITS="${MEMORY_LIMITS:-1GB 4GB 6GB 8GB 16GB 24GB 32GB}"
MEMORY_LIMITS="${MEMORY_LIMITS:-2GB}"
RESULTS_FILE="${RESULTS_FILE:-postgres_memory_sweep_results.csv}"
LOG_DIR="${LOG_DIR:-./logs/postgres_memory_sweep}"

echo "=== PostgreSQL Memory Sweep ==="
echo "Input: $INPUT_FILE"
echo "Format: $FORMAT"
echo "Database: $DB_CONNECTION"
echo "Table: $TABLE"
echo "Parallel workers: $PARALLEL_WORKERS"
echo "Memory limits: $MEMORY_LIMITS"
echo "Results file: $RESULTS_FILE"
echo "Log directory: $LOG_DIR"
echo ""

# Create log directory
mkdir -p "$LOG_DIR"

# Initialize results file with header
echo "work_mem,parallel_workers,duration_seconds" > "$RESULTS_FILE"

# Create output directory
mkdir -p "$(dirname "$OUTPUT_BASE")"

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
        --table "$TABLE"

    echo "Running CHECKPOINT..."
    psql "$DB_CONNECTION" -c "CHECKPOINT" >/dev/null
    sync
    echo ""
fi

# Run sort for each memory limit
for MEM in $MEMORY_LIMITS; do
    RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    # Create individual log file for this configuration
    LOG_FILE="${LOG_DIR}/${PARALLEL_WORKERS}workers_${MEM}_${RUN_TIMESTAMP}.log"

    echo "========================================="
    echo "Running with $MEM work_mem..."
    echo "========================================="
    echo "Log file: $LOG_FILE"

    OUTPUT_FILE="${OUTPUT_BASE}_${MEM}.bin"
    # Convert to absolute path for PostgreSQL COPY command
    if [[ "$OUTPUT_FILE" = /* ]]; then
        OUTPUT_FILE_ABS="$OUTPUT_FILE"
    else
        OUTPUT_FILE_ABS="$(pwd)/$OUTPUT_FILE"
    fi

    # Remove old output
    rm -f "$OUTPUT_FILE"

    # Run and capture output and exit code
    set +e
    OUTPUT=$(cargo run --release --bin sort-postgres -- \
        --db "$DB_CONNECTION" \
        --table "$TABLE" \
        --output "$OUTPUT_FILE_ABS" \
        --total-memory "$MEM" \
        --parallel-workers "$PARALLEL_WORKERS" 2>&1)
    EXIT_CODE=$?
    set -e

    echo "$OUTPUT"

    # Extract timing from output
    DURATION=$(echo "$OUTPUT" | grep "TIMING:" | awk '{print $2}')

    # Write detailed log to individual file
    {
        echo "========================================="
        echo "PostgreSQL Memory Sweep - Configuration Log"
        echo "========================================="
        echo "Configuration: work_mem=$MEM, parallel_workers=$PARALLEL_WORKERS"
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
            echo "Result: $MEM,$PARALLEL_WORKERS,$DURATION"
        else
            echo "WARNING: Could not extract timing information"
        fi
        echo "End time: $(date +"%Y-%m-%d %H:%M:%S")"
        echo "========================================="
    } > "$LOG_FILE"

    # Log results to CSV
    if [ -n "$DURATION" ]; then
        echo "$MEM,$PARALLEL_WORKERS,$DURATION" >> "$RESULTS_FILE"
        echo "Result logged: work_mem=$MEM, parallel_workers=$PARALLEL_WORKERS, duration=${DURATION}s"
    else
        echo "Warning: Could not extract timing information"
    fi

    # Clean up output file to save SSD space
    if [ -f "$OUTPUT_FILE_ABS" ]; then
        echo "Cleaning up output file..."
        truncate -s 0 "$OUTPUT_FILE_ABS"
        sync
        rm -f "$OUTPUT_FILE_ABS"
        echo "Output file removed."
    fi

    echo ""
done

echo "=== Sweep Complete ==="
echo "All output files have been cleaned up to save SSD space."
echo "Timing results saved to: $RESULTS_FILE"
echo "Detailed logs saved to: $LOG_DIR"
