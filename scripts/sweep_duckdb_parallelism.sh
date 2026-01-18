#!/bin/bash
# DuckDB parallelism sweep: vary threads from 4 to 44 at fixed memory budget

set -e

# Configuration
INPUT_FILE="${INPUT_FILE:-testdata/test_gensort.dat}"
FORMAT="${FORMAT:-gensort}"
DB_FILE="${DB_FILE:-./duckdb_bench.db}"
TABLE="${TABLE:-bench_data}"
OUTPUT_BASE="${OUTPUT_BASE:-./duckdb_sorted/result}"
MEMORY_LIMIT="${MEMORY_LIMIT:-2GB}"
TEMP_DIR="${TEMP_DIR:-./duckdb_temp}"
THREAD_COUNTS="${THREAD_COUNTS:-4 8 16 24 32 40 44}"
LOG_DIR="${LOG_DIR:-./logs/duckdb_parallelism_sweep}"

echo "=== DuckDB Parallelism Sweep ==="
echo "Input: $INPUT_FILE"
echo "Format: $FORMAT"
echo "Database: $DB_FILE"
echo "Table: $TABLE"
echo "Memory limit: $MEMORY_LIMIT"
echo "Thread counts: $THREAD_COUNTS"
echo "Log directory: $LOG_DIR"
echo ""

# Create log directory
mkdir -p "$LOG_DIR"

# Create output and temp directories
mkdir -p "$(dirname "$OUTPUT_BASE")"
# Create temp directory
mkdir -p "$TEMP_DIR"

# Load database if it doesn't exist
if [ ! -f "$DB_FILE" ]; then
    echo "Loading data into DuckDB..."
    cargo run --release --bin load-duckdb -- \
        --format "$FORMAT" \
        --input "$INPUT_FILE" \
        --db "$DB_FILE" \
        --table "$TABLE" \
        --threads 14

    echo "Flushing database to disk..."
    sync
    echo ""
fi

# Run sort for each thread count
for T in $THREAD_COUNTS; do
    RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    # Create individual log file for this configuration
    LOG_FILE="${LOG_DIR}/${MEMORY_LIMIT}_${T}threads_${RUN_TIMESTAMP}.log"

    echo "========================================="
    echo "Running with $T threads..."
    echo "========================================="
    echo "Log file: $LOG_FILE"

    OUTPUT_DIR="${OUTPUT_BASE}_${T}_threads"

    # Remove old output
    rm -rf "$OUTPUT_DIR"

    # Run and capture output and exit code
    set +e
    OUTPUT=$(cargo run --release --bin sort-duckdb -- \
        --db "$DB_FILE" \
        --table "$TABLE" \
        --output "$OUTPUT_DIR" \
        --memory-limit "$MEMORY_LIMIT" \
        --temp-dir "$TEMP_DIR" \
        --threads "$T" 2>&1)
    EXIT_CODE=$?
    set -e

    echo "$OUTPUT"

    # Extract timing from output
    DURATION=$(echo "$OUTPUT" | grep "TIMING:" | awk '{print $2}')

    # Write detailed log to individual file
    {
        echo "========================================="
        echo "DuckDB Parallelism Sweep - Configuration Log"
        echo "========================================="
        echo "Configuration: memory_limit=$MEMORY_LIMIT, threads=$T"
        echo "Input: $INPUT_FILE"
        echo "Database: $DB_FILE"
        echo "Table: $TABLE"
        echo "Temp directory: $TEMP_DIR"
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
            echo "Result: $MEMORY_LIMIT,$T,$DURATION"
        else
            echo "WARNING: Could not extract timing information"
        fi
        echo "End time: $(date +"%Y-%m-%d %H:%M:%S")"
        echo "========================================="
    } > "$LOG_FILE"

    # Report results
    if [ -n "$DURATION" ]; then
        echo "Result logged: memory_limit=$MEMORY_LIMIT, threads=$T, duration=${DURATION}s"
    else
        echo "Warning: Could not extract timing information"
    fi

    # Clean up output directory to save SSD space
    if [ -d "$OUTPUT_DIR" ]; then
        echo "Cleaning up output directory..."
        # Truncate all files in the directory to 0 bytes first
        find "$OUTPUT_DIR" -type f -exec truncate -s 0 {} \;
        sync
        rm -rf "$OUTPUT_DIR"
        echo "Output directory removed."
    fi

    echo ""
done

echo "=== Sweep Complete ==="
echo "All output directories have been cleaned up to save SSD space."
echo "Results saved to logs in: $LOG_DIR"
