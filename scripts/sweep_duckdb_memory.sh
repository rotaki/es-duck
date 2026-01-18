#!/bin/bash
# DuckDB memory sweep: vary memory limit at fixed thread count

set -e

# Configuration
INPUT_FILE="${INPUT_FILE:-testdata/test_gensort_5gb.dat}"
FORMAT="${FORMAT:-gensort}"
DB_FILE="${DB_FILE:-./duckdb_bench.db}"
TABLE="${TABLE:-bench_data}"
OUTPUT_BASE="${OUTPUT_BASE:-./duckdb_sorted/result}"
THREADS="${THREADS:-40}"
TEMP_DIR="${TEMP_DIR:-./duckdb_temp}"
# MEMORY_LIMITS="${MEMORY_LIMITS:-2GB 4GB 6GB 8GB 16GB 24GB 32GB}"
MEMORY_LIMITS="${MEMORY_LIMITS:-2GB}"
LOG_DIR="${LOG_DIR:-./logs/duckdb_memory_sweep}"

echo "=== DuckDB Memory Sweep ==="
echo "Input: $INPUT_FILE"
echo "Format: $FORMAT"
echo "Database: $DB_FILE"
echo "Table: $TABLE"
echo "Threads: $THREADS"
echo "Memory limits: $MEMORY_LIMITS"
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
        --threads "$THREADS"

    echo "Flushing database to disk..."
    sync
    echo ""
fi

# Run sort for each memory limit
for MEM in $MEMORY_LIMITS; do
    RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    # Create individual log file for this configuration
    LOG_FILE="${LOG_DIR}/${THREADS}threads_${MEM}_${RUN_TIMESTAMP}.log"

    echo "========================================="
    echo "Running with $MEM memory limit..."
    echo "========================================="
    echo "Log file: $LOG_FILE"

    OUTPUT_DIR="${OUTPUT_BASE}_${MEM}"

    # Remove old output
    rm -rf "$OUTPUT_DIR"

    # Run and capture output and exit code
    set +e
    OUTPUT=$(cargo run --release --bin sort-duckdb -- \
        --db "$DB_FILE" \
        --table "$TABLE" \
        --output "$OUTPUT_DIR" \
        --memory-limit "$MEM" \
        --temp-dir "$TEMP_DIR" \
        --threads "$THREADS" 2>&1)
    EXIT_CODE=$?
    set -e

    echo "$OUTPUT"

    # Extract timing from output
    DURATION=$(echo "$OUTPUT" | grep "TIMING:" | awk '{print $2}')

    # Write detailed log to individual file
    {
        echo "========================================="
        echo "DuckDB Memory Sweep - Configuration Log"
        echo "========================================="
        echo "Configuration: memory_limit=$MEM, threads=$THREADS"
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
            echo "Result: $MEM,$THREADS,$DURATION"
        else
            echo "WARNING: Could not extract timing information"
        fi
        echo "End time: $(date +"%Y-%m-%d %H:%M:%S")"
        echo "========================================="
    } > "$LOG_FILE"

    # Report results
    if [ -n "$DURATION" ]; then
        echo "Result logged: memory_limit=$MEM, threads=$THREADS, duration=${DURATION}s"
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
