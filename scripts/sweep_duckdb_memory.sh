#!/bin/bash
# DuckDB memory sweep: vary memory limit at fixed thread count

set -e

# Generate timestamp for this sweep run
SWEEP_TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Configuration
INPUT_FILE="${INPUT_FILE:-testdata/test_gensort_5gb.dat}"
FORMAT="${FORMAT:-gensort}"
DB_FILE="${DB_FILE:-./duckdb_bench.db}"
TABLE="${TABLE:-bench_data}"
THREADS="${THREADS:-40}"
TEMP_DIR="${TEMP_DIR:-./duckdb_temp}"
# MEMORY_LIMITS="${MEMORY_LIMITS:-2GB 4GB 6GB 8GB 16GB 24GB 32GB}"
MEMORY_LIMITS="${MEMORY_LIMITS:-100GB}"
LOG_DIR="${LOG_DIR:-./logs/duckdb_memory_sweep_${SWEEP_TIMESTAMP}}"
OUTPUT="${OUTPUT:-}"  # Optional output path for parquet mode

echo "=== DuckDB Memory Sweep ==="
echo "Input: $INPUT_FILE"
echo "Format: $FORMAT"
echo "Database: $DB_FILE"
echo "Table: $TABLE"
echo "Threads: $THREADS"
echo "Memory limits: $MEMORY_LIMITS"
echo "Log directory: $LOG_DIR"
if [ -n "$OUTPUT" ]; then
    echo "Mode: Parquet output to $OUTPUT"
else
    echo "Mode: Count (no output)"
fi
echo ""

# Create log directory
mkdir -p "$LOG_DIR"

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

    # Run and capture output and exit code
    set +e
    if [ -n "$OUTPUT" ]; then
        # Parquet mode: truncate output file first in case it's open
        if [ -f "$OUTPUT" ]; then
            echo "Truncating existing output file..."
            > "$OUTPUT"
            sync
        fi

        COMMAND_OUTPUT=$(cargo run --release --bin sort-duckdb -- \
            --db "$DB_FILE" \
            --table "$TABLE" \
            --memory-limit "$MEM" \
            --temp-dir "$TEMP_DIR" \
            --threads "$THREADS" \
            --output "$OUTPUT" 2>&1)
    else
        # Count mode
        COMMAND_OUTPUT=$(cargo run --release --bin sort-duckdb -- \
            --db "$DB_FILE" \
            --table "$TABLE" \
            --memory-limit "$MEM" \
            --temp-dir "$TEMP_DIR" \
            --threads "$THREADS" 2>&1)
    fi
    EXIT_CODE=$?
    set -e

    echo "$COMMAND_OUTPUT"

    # Extract timing from output
    DURATION=$(echo "$COMMAND_OUTPUT" | grep "TIMING:" | awk '{print $2}')

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
        echo "$COMMAND_OUTPUT"
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

    # Clean up parquet output file if it exists
    if [ -n "$OUTPUT" ] && [ -f "$OUTPUT" ]; then
        echo "Cleaning up output file..."
        OUTPUT_SIZE=$(du -sh "$OUTPUT" 2>/dev/null | cut -f1 || echo "unknown")
        echo "Output file size: $OUTPUT_SIZE"
        # Truncate first in case file is still open by DuckDB
        > "$OUTPUT"
        rm -f "$OUTPUT"
        sync
        echo "Output file removed and synced."
    fi

    # Clean up temp directory after each run
    if [ -d "$TEMP_DIR" ]; then
        echo "Cleaning up temp directory..."
        rm -rf "$TEMP_DIR"/*
        sync
        echo "Temp directory cleaned."
    fi

    echo ""
    echo "Waiting 30 seconds before next run..."
    sleep 30
    echo ""
done

echo "=== Sweep Complete ==="
echo "Results saved to logs in: $LOG_DIR"
