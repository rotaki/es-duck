#!/bin/bash
# ClickHouse memory sweep: vary memory limit at fixed thread count
# FIXED VERSION: Shows real-time output and has timeout protection

set -e

# Generate timestamp for this sweep run
SWEEP_TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Configuration
INPUT_FILE="${INPUT_FILE:-testdata/test_gensort_5gb.dat}"
FORMAT="${FORMAT:-gensort}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
DATABASE="${DATABASE:-default}"
TABLE="${TABLE:-bench_data}"
THREADS="${THREADS:-40}"
# MEMORY_LIMITS="${MEMORY_LIMITS:-2GB 4GB 6GB 8GB 16GB 24GB 32GB}"
MEMORY_LIMITS="${MEMORY_LIMITS:-100GB}"
LOG_DIR="${LOG_DIR:-./logs/clickhouse_memory_sweep_${SWEEP_TIMESTAMP}}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-7200}"  # 2 hour default timeout
BENCHMARK_RUNS="${BENCHMARK_RUNS:-1}"  # Number of times to run each configuration
CLEAR_CACHE_SCRIPT="/usr/local/sbin/clearcache3.sh"
OUTPUT="${OUTPUT:-}"  # Optional output path for binary mode

echo "=== ClickHouse Memory Sweep ==="
echo "Input: $INPUT_FILE"
echo "Format: $FORMAT"
echo "ClickHouse URL: $CLICKHOUSE_URL"
echo "Database: $DATABASE"
echo "Table: $TABLE"
echo "Threads: $THREADS"
echo "Memory limits: $MEMORY_LIMITS"
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

# Check if table exists by querying system.tables
TABLE_EXISTS=$(curl -sS "${CLICKHOUSE_URL}/?query=SELECT%20count(*)%20FROM%20system.tables%20WHERE%20database%20=%20'${DATABASE}'%20AND%20name%20=%20'${TABLE}'" 2>/dev/null || echo "0")

# Load database if table doesn't exist
if [ "$TABLE_EXISTS" = "0" ]; then
    echo "Loading data into ClickHouse..."
    echo "NOTE: This will show output in real-time..."

    timeout $TIMEOUT_SECONDS cargo run --release --bin load-clickhouse --features db-clickhouse -- \
        --format "$FORMAT" \
        --input "$INPUT_FILE" \
        --url "$CLICKHOUSE_URL" \
        --database "$DATABASE" \
        --table "$TABLE" \
        --threads "$THREADS" || {
        echo "ERROR: Database loading failed or timed out"
        exit 1
    }

    echo "Syncing..."
    sync
    echo ""
fi

# Run sort for each memory limit
for MEM in $MEMORY_LIMITS; do
  for RUN in $(seq 1 $BENCHMARK_RUNS); do
    RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    # Create individual log file for this configuration with run number suffix
    LOG_FILE="${LOG_DIR}/${THREADS}threads_${MEM}_run${RUN}_${RUN_TIMESTAMP}.log"
    TEMP_OUTPUT="/tmp/clickhouse_sweep_${MEM}_run${RUN}_${RUN_TIMESTAMP}.log"

    echo "========================================="
    echo "Running with $MEM memory limit... (Run $RUN of $BENCHMARK_RUNS)"
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

        timeout $TIMEOUT_SECONDS cargo run --release --bin sort-clickhouse --features db-clickhouse -- \
            --url "$CLICKHOUSE_URL" \
            --database "$DATABASE" \
            --table "$TABLE" \
            --threads "$THREADS" \
            --output "$OUTPUT" 2>&1 | tee "$TEMP_OUTPUT"
    else
        # Count mode
        timeout $TIMEOUT_SECONDS cargo run --release --bin sort-clickhouse --features db-clickhouse -- \
            --url "$CLICKHOUSE_URL" \
            --database "$DATABASE" \
            --table "$TABLE" \
            --threads "$THREADS" 2>&1 | tee "$TEMP_OUTPUT"
    fi

    EXIT_CODE=${PIPESTATUS[0]}
    set -e

    # Read captured output
    COMMAND_OUTPUT=$(cat "$TEMP_OUTPUT")

    # Check timeout - skip remaining runs for this configuration
    if [ $EXIT_CODE -eq 124 ]; then
        echo ""
        echo "WARNING: Process timed out after ${TIMEOUT_SECONDS}s"
        echo "Skipping remaining benchmark runs for $MEM memory limit..."
        SKIP_REMAINING=true
    else
        SKIP_REMAINING=false
    fi

    # Clear ClickHouse's internal caches using SYSTEM DROP commands
    echo "Clearing ClickHouse caches..."
    curl -sS "${CLICKHOUSE_URL}/?query=SYSTEM%20DROP%20MARK%20CACHE" >/dev/null 2>&1 || true
    curl -sS "${CLICKHOUSE_URL}/?query=SYSTEM%20DROP%20UNCOMPRESSED%20CACHE" >/dev/null 2>&1 || true
    curl -sS "${CLICKHOUSE_URL}/?query=SYSTEM%20DROP%20COMPILED%20EXPRESSION%20CACHE" >/dev/null 2>&1 || true

    # Extract timing from output
    DURATION=$(echo "$COMMAND_OUTPUT" | grep "TIMING:" | awk '{print $2}' || true)

    # Write detailed log to individual file
    {
        echo "========================================="
        echo "ClickHouse Memory Sweep - Configuration Log"
        echo "========================================="
        echo "Configuration: memory_limit=$MEM, threads=$THREADS, run=$RUN/$BENCHMARK_RUNS"
        echo "Input: $INPUT_FILE"
        echo "ClickHouse URL: $CLICKHOUSE_URL"
        echo "Database: $DATABASE"
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
            echo "Result: $MEM,$THREADS,$RUN,$DURATION"
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
        echo "✓ Result logged: memory_limit=$MEM, threads=$THREADS, run=$RUN/$BENCHMARK_RUNS, duration=${DURATION}s"
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
