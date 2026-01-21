#!/bin/bash
# DuckDB parallelism sweep: vary threads from 4 to 44 at fixed memory budget
# FIXED VERSION: Shows real-time output and has timeout protection

set -e

# Generate timestamp for this sweep run
SWEEP_TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Configuration
INPUT_FILE="${INPUT_FILE:-testdata/test_gensort.dat}"
FORMAT="${FORMAT:-gensort}"
DB_FILE="${DB_FILE:-./duckdb_bench.db}"
TABLE="${TABLE:-bench_data}"
MEMORY_LIMIT="${MEMORY_LIMIT:-100GB}"
TEMP_DIR="${TEMP_DIR:-./duckdb_temp}"
# THREAD_COUNTS="${THREAD_COUNTS:-4 8 16 24 32 40 44}"
THREAD_COUNTS="${THREAD_COUNTS:-44 40 32 24 16 8 4}"
LOG_DIR="${LOG_DIR:-./logs/duckdb_parallelism_sweep_${SWEEP_TIMESTAMP}}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-7200}"  # 2 hour default timeout
OUTPUT="${OUTPUT:-}"  # Optional output path for parquet mode

echo "=== DuckDB Parallelism Sweep ==="
echo "Input: $INPUT_FILE"
echo "Format: $FORMAT"
echo "Database: $DB_FILE"
echo "Table: $TABLE"
echo "Memory limit: $MEMORY_LIMIT"
echo "Thread counts: $THREAD_COUNTS"
echo "Timeout: ${TIMEOUT_SECONDS}s"
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
    echo "NOTE: This will show output in real-time..."

    timeout $TIMEOUT_SECONDS cargo run --release --bin load-duckdb -- \
        --format "$FORMAT" \
        --input "$INPUT_FILE" \
        --db "$DB_FILE" \
        --table "$TABLE" \
        --threads 14 || {
        echo "ERROR: Database loading failed or timed out"
        exit 1
    }

    echo "Flushing database to disk..."
    sync

    # Show database size
    DB_SIZE=$(du -sh "$DB_FILE" | cut -f1)
    echo "Database created: $DB_SIZE"
    echo ""
fi

# Run sort for each thread count
for T in $THREAD_COUNTS; do
    RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    # Create individual log file for this configuration
    LOG_FILE="${LOG_DIR}/${MEMORY_LIMIT}_${T}threads_${RUN_TIMESTAMP}.log"
    TEMP_OUTPUT="/tmp/duckdb_sweep_${T}_${RUN_TIMESTAMP}.log"

    echo "========================================="
    echo "Running with $T threads..."
    echo "Start time: $(date +"%Y-%m-%d %H:%M:%S")"
    echo "========================================="
    echo "Log file: $LOG_FILE"
    echo "NOTE: Output will appear in real-time below..."
    echo ""

    # Run with timeout and show output in real-time using tee
    set +e
    if [ -n "$OUTPUT" ]; then
        # Parquet mode: truncate output file first in case it's open
        if [ -f "$OUTPUT" ]; then
            echo "Truncating existing output file..."
            > "$OUTPUT"
            sync
        fi

        timeout $TIMEOUT_SECONDS cargo run --release --bin sort-duckdb -- \
            --db "$DB_FILE" \
            --table "$TABLE" \
            --memory-limit "$MEMORY_LIMIT" \
            --temp-dir "$TEMP_DIR" \
            --threads "$T" \
            --output "$OUTPUT" 2>&1 | tee "$TEMP_OUTPUT"
    else
        # Count mode
        timeout $TIMEOUT_SECONDS cargo run --release --bin sort-duckdb -- \
            --db "$DB_FILE" \
            --table "$TABLE" \
            --memory-limit "$MEMORY_LIMIT" \
            --temp-dir "$TEMP_DIR" \
            --threads "$T" 2>&1 | tee "$TEMP_OUTPUT"
    fi

    EXIT_CODE=${PIPESTATUS[0]}
    set -e

    # Read captured output
    COMMAND_OUTPUT=$(cat "$TEMP_OUTPUT")

    # Check timeout
    if [ $EXIT_CODE -eq 124 ]; then
        echo ""
        echo "WARNING: Process timed out after ${TIMEOUT_SECONDS}s"
    fi

    # Extract timing from output
    DURATION=$(echo "$COMMAND_OUTPUT" | grep "TIMING:" | awk '{print $2}')

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
            echo "Result: $MEMORY_LIMIT,$T,$DURATION"
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
        echo "✓ Result logged: memory_limit=$MEMORY_LIMIT, threads=$T, duration=${DURATION}s"
    else
        echo "✗ Warning: Could not extract timing information"
    fi
    echo "End time: $(date +"%Y-%m-%d %H:%M:%S")"
    echo "========================================="

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
        TEMP_SIZE=$(du -sh "$TEMP_DIR" 2>/dev/null | cut -f1 || echo "unknown")
        echo "Temp directory size before cleanup: $TEMP_SIZE"
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
echo ""
echo "Summary of results:"
grep "Result:" "$LOG_DIR"/*.log 2>/dev/null || echo "No successful results found"
