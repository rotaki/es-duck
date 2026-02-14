#!/bin/bash
# Unified benchmark orchestrator: runs parallelism sweeps across DuckDB, PostgreSQL, and ClickHouse
# Manages database lifecycle, SSD/HDD data movement, and PostgreSQL plan deduplication.
#
# Usage:
#   INPUT_FILE=/path/to/data.dat ./scripts/run_all_benchmarks.sh
#
# Required:
#   INPUT_FILE - path to input data file
#
# Optional overrides:
#   MEMORY_LIMIT    - memory budget (default: 10GB)
#   THREAD_COUNTS   - space-separated thread counts (default: "4 8 16 24 32 40 44")
#   BENCHMARK_RUNS  - runs per configuration (default: 3)
#   SSD_BASE        - SSD data directory (default: /mnt/nvme1/rotaki/es/datasets)
#   HDD_BASE        - HDD data directory (default: /tank/local/riki/datasets)
#   DATABASES       - which databases to benchmark (default: "duckdb postgres clickhouse")
#   TIMEOUT_SECONDS - per-benchmark timeout (default: 7200)
#   RCLONE_REMOTE   - rclone remote:path for log upload (default: gdrive:bench_results/gensort)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Configuration ─────────────────────────────────────────────────────────────
INPUT_FILE="${INPUT_FILE:?INPUT_FILE must be set}"
FORMAT="${FORMAT:-gensort}"
MEMORY_LIMIT="${MEMORY_LIMIT:-10GB}"
THREAD_COUNTS="${THREAD_COUNTS:-4 8 16 24 32 40 44}"
BENCHMARK_RUNS="${BENCHMARK_RUNS:-3}"
TABLE="${TABLE:-bench_data}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-7200}"
CLEAR_CACHE_SCRIPT="/usr/local/sbin/clearcache3.sh"

SSD_BASE="${SSD_BASE:-/mnt/nvme1/rotaki/es-duck/scripts}"
HDD_BASE="${HDD_BASE:-/tank/local/riki/datasets}"
DATABASES="${DATABASES:-duckdb postgres clickhouse}"
RCLONE_REMOTE="${RCLONE_REMOTE:-gdrive:bench_results/gensort}"

# Database-specific paths
POSTGRES_DIR="${SCRIPT_DIR}/postgres"
POSTGRES_PORT="${POSTGRES_PORT:-5433}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
CLICKHOUSE_BIN_DIR="${SCRIPT_DIR}/clickhouse-bin"
CLICKHOUSE_PID="${SCRIPT_DIR}/clickhouse.pid"
HTTP_PORT="${HTTP_PORT:-8123}"
TCP_PORT="${TCP_PORT:-9000}"

SWEEP_TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "========================================================"
echo "  Unified Benchmark Orchestrator"
echo "========================================================"
echo "Input:          $INPUT_FILE"
echo "Format:         $FORMAT"
echo "Memory:         $MEMORY_LIMIT"
echo "Thread counts:  $THREAD_COUNTS"
echo "Benchmark runs: $BENCHMARK_RUNS"
echo "SSD base:       $SSD_BASE"
echo "HDD base:       $HDD_BASE"
echo "Databases:      $DATABASES"
echo "Timeout:        ${TIMEOUT_SECONDS}s"
echo "Timestamp:      $SWEEP_TIMESTAMP"
echo "========================================================"
echo ""

# ── Installation Functions ────────────────────────────────────────────────────

POSTGRES_VERSION="18.1"

install_postgres() {
    if [[ -x "${POSTGRES_DIR}/bin/postgres" ]]; then
        echo "[install] PostgreSQL already installed at ${POSTGRES_DIR}/bin/postgres"
        return 0
    fi

    echo "[install] PostgreSQL not found locally. Downloading PostgreSQL ${POSTGRES_VERSION}..."

    # Detect OS and architecture
    local OS ARCH
    case "$(uname -s)" in
        Darwin*) OS="macos" ;;
        Linux*)  OS="linux" ;;
        *)
            echo "Error: Unsupported OS: $(uname -s)" >&2
            return 1
            ;;
    esac

    case "$(uname -m)" in
        x86_64|amd64)    ARCH="x86_64" ;;
        arm64|aarch64)   ARCH="arm64" ;;
        *)
            echo "Error: Unsupported architecture: $(uname -m)" >&2
            return 1
            ;;
    esac

    mkdir -p "${POSTGRES_DIR}"

    if [[ "$OS" == "macos" ]]; then
        local POSTGRES_APP_VERSION="2.8.1"
        local BINARY_URL BINARY_FILE BINARY_FAILED=""

        if [[ "$ARCH" == "arm64" ]]; then
            BINARY_URL="https://github.com/PostgresApp/PostgresApp/releases/download/v${POSTGRES_APP_VERSION}/Postgres-${POSTGRES_APP_VERSION}-18-arm64.zip"
        else
            BINARY_URL="https://github.com/PostgresApp/PostgresApp/releases/download/v${POSTGRES_APP_VERSION}/Postgres-${POSTGRES_APP_VERSION}-18.zip"
        fi

        BINARY_FILE="/tmp/postgres-${ARCH}.zip"

        echo "[install] Downloading from ${BINARY_URL}..."
        if curl -f -L -o "${BINARY_FILE}" "${BINARY_URL}"; then
            echo "[install] Extracting binaries..."
            cd /tmp
            unzip -q "${BINARY_FILE}"

            if [[ -d "Postgres.app" ]]; then
                cp -R "Postgres.app/Contents/Versions/18/"* "${POSTGRES_DIR}/"
                rm -rf "Postgres.app"
            fi

            rm "${BINARY_FILE}"
            cd "${SCRIPT_DIR}"

            if [[ -x "${POSTGRES_DIR}/bin/postgres" ]]; then
                echo "[install] Successfully installed PostgreSQL binaries!"
            else
                echo "[install] Binary extraction failed, falling back to source compilation..."
                BINARY_FAILED=1
            fi
        else
            echo "[install] Binary download failed, falling back to source compilation..."
            BINARY_FAILED=1
        fi

        if [[ -n "${BINARY_FAILED}" ]]; then
            local SOURCE_URL="https://ftp.postgresql.org/pub/source/v${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.tar.gz"
            local SOURCE_FILE="/tmp/postgresql-${POSTGRES_VERSION}.tar.gz"

            echo "[install] Downloading and compiling from source..."
            curl -L -o "${SOURCE_FILE}" "${SOURCE_URL}"
            tar -xzf "${SOURCE_FILE}" -C /tmp
            cd "/tmp/postgresql-${POSTGRES_VERSION}"
            ./configure --prefix="${POSTGRES_DIR}" --without-readline --without-zlib --without-icu
            make -j$(sysctl -n hw.ncpu)
            make install
            rm -rf "/tmp/postgresql-${POSTGRES_VERSION}" "${SOURCE_FILE}"
            cd "${SCRIPT_DIR}"
        fi
    else
        # Linux: compile from source
        local SOURCE_URL="https://ftp.postgresql.org/pub/source/v${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.tar.gz"
        local SOURCE_FILE="/tmp/postgresql-${POSTGRES_VERSION}.tar.gz"

        echo "[install] Compiling PostgreSQL from source for Linux..."
        curl -L -o "${SOURCE_FILE}" "${SOURCE_URL}"
        tar -xzf "${SOURCE_FILE}" -C /tmp
        cd "/tmp/postgresql-${POSTGRES_VERSION}"
        ./configure --prefix="${POSTGRES_DIR}" --without-readline --without-zlib --without-icu
        make -j$(nproc)
        make install
        rm -rf "/tmp/postgresql-${POSTGRES_VERSION}" "${SOURCE_FILE}"
        cd "${SCRIPT_DIR}"
    fi

    echo "[install] PostgreSQL ${POSTGRES_VERSION} installed successfully!"
}

install_clickhouse() {
    if [[ -x "${CLICKHOUSE_BIN_DIR}/clickhouse" ]]; then
        echo "[install] ClickHouse already installed at ${CLICKHOUSE_BIN_DIR}/clickhouse"
        return 0
    fi

    echo "[install] ClickHouse binary not found locally. Fetching latest version..."

    local TAG_NAME
    TAG_NAME=$(curl -s "https://api.github.com/repos/ClickHouse/ClickHouse/releases/latest" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')

    if [[ -z "$TAG_NAME" ]]; then
        echo "Error: Could not fetch latest ClickHouse version" >&2
        return 1
    fi

    local VERSION="${TAG_NAME#v}"
    VERSION="${VERSION%-stable}"

    echo "[install] Downloading ${TAG_NAME} (version ${VERSION})..."
    mkdir -p "${CLICKHOUSE_BIN_DIR}"

    local OS ARCH ASSET_NAME IS_TARBALL URL
    OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
    ARCH="$(uname -m)"

    if [[ "$OS" == "darwin" ]]; then
        IS_TARBALL=false
        if [[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]]; then
            ASSET_NAME="clickhouse-macos-aarch64"
        elif [[ "$ARCH" == "x86_64" ]]; then
            ASSET_NAME="clickhouse-macos"
        else
            echo "Unsupported macOS architecture: $ARCH" >&2
            return 1
        fi
    elif [[ "$OS" == "linux" ]]; then
        IS_TARBALL=true
        if [[ "$ARCH" == "x86_64" ]]; then
            ASSET_NAME="clickhouse-common-static-${VERSION}-amd64.tgz"
        elif [[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]]; then
            ASSET_NAME="clickhouse-common-static-${VERSION}-arm64.tgz"
        else
            echo "Unsupported Linux architecture: $ARCH" >&2
            return 1
        fi
    else
        echo "Unsupported operating system: $OS" >&2
        return 1
    fi

    URL="https://github.com/ClickHouse/ClickHouse/releases/download/${TAG_NAME}/${ASSET_NAME}"

    echo "[install] Downloading from ${URL}..."

    if [[ "$IS_TARBALL" == true ]]; then
        curl -f -L -o "${CLICKHOUSE_BIN_DIR}/${ASSET_NAME}" "${URL}"
        echo "[install] Extracting ClickHouse binary..."
        tar -xzf "${CLICKHOUSE_BIN_DIR}/${ASSET_NAME}" -C "${CLICKHOUSE_BIN_DIR}" --strip-components=3 --wildcards "*/usr/bin/clickhouse"
        rm "${CLICKHOUSE_BIN_DIR}/${ASSET_NAME}"
    else
        curl -f -L -o "${CLICKHOUSE_BIN_DIR}/clickhouse" "${URL}"
    fi

    chmod +x "${CLICKHOUSE_BIN_DIR}/clickhouse"
    echo "[install] ClickHouse installed successfully!"
}

# ── Helper Functions ──────────────────────────────────────────────────────────

stop_postgres() {
    echo "[helper] Stopping PostgreSQL if running..."
    # Try stopping with pg_ctl using both possible data dirs
    for DATA_DIR in "${SSD_BASE}/postgres-data" "${HDD_BASE}/postgres-data" "${SCRIPT_DIR}/postgres-data"; do
        if [[ -d "$DATA_DIR" ]] && [[ -x "${POSTGRES_DIR}/bin/pg_ctl" ]]; then
            "${POSTGRES_DIR}/bin/pg_ctl" -D "$DATA_DIR" stop 2>/dev/null || true
        fi
    done
    # Also check via pg_isready and kill if still running
    if pg_isready -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" >/dev/null 2>&1; then
        echo "[helper] PostgreSQL still responding, attempting pkill..."
        pkill -f "postgres.*-p ${POSTGRES_PORT}" 2>/dev/null || true
        sleep 2
    fi
    echo "[helper] PostgreSQL stopped."
}

stop_clickhouse() {
    echo "[helper] Stopping ClickHouse if running..."
    if [[ -f "${CLICKHOUSE_PID}" ]]; then
        PID=$(cat "${CLICKHOUSE_PID}")
        kill "$PID" 2>/dev/null || true
        sleep 2
        kill -9 "$PID" 2>/dev/null || true
        rm -f "${CLICKHOUSE_PID}"
    fi
    pkill -f "clickhouse server" 2>/dev/null || true
    sleep 1
    echo "[helper] ClickHouse stopped."
}

stop_other_dbs() {
    local current_db="$1"
    echo "[helper] Stopping other databases (current: $current_db)..."
    if [[ "$current_db" != "postgres" ]]; then
        stop_postgres
    fi
    if [[ "$current_db" != "clickhouse" ]]; then
        stop_clickhouse
    fi
}

check_ssd_space() {
    echo "[helper] Checking SSD space..."
    mkdir -p "$SSD_BASE"
    local avail_kb
    avail_kb=$(df --output=avail "$SSD_BASE" 2>/dev/null | tail -1 | tr -d ' ')
    local avail_gb=$((avail_kb / 1024 / 1024))
    echo "[helper] SSD available: ${avail_gb}GB"
    if [[ $avail_gb -lt 50 ]]; then
        echo "WARNING: Less than 50GB available on SSD ($SSD_BASE). Proceeding anyway..."
    fi
}

start_postgres() {
    local data_dir="$1"
    export PATH="${POSTGRES_DIR}/bin:${PATH}"

    # Initialize data directory if it doesn't exist
    if [[ ! -d "$data_dir" ]]; then
        echo "[postgres] Initializing data directory at $data_dir..."
        mkdir -p "$data_dir"
        "${POSTGRES_DIR}/bin/initdb" -D "$data_dir" -U "${POSTGRES_USER}" --no-locale --encoding=UTF8
    fi

    # Start if not already running
    if ! pg_isready -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" >/dev/null 2>&1; then
        echo "[postgres] Starting server with data dir: $data_dir..."
        "${POSTGRES_DIR}/bin/pg_ctl" -D "$data_dir" -l "${SCRIPT_DIR}/postgres.log" \
            -o "-p ${POSTGRES_PORT} -k ${SCRIPT_DIR} -c max_worker_processes=128 -c max_parallel_workers=128 -c max_connections=200" \
            start

        echo "[postgres] Waiting for server to be ready..."
        for i in {1..30}; do
            if pg_isready -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" >/dev/null 2>&1; then
                echo "[postgres] Server is ready!"
                return 0
            fi
            if [[ $i -eq 30 ]]; then
                echo "ERROR: PostgreSQL failed to start. Check ${SCRIPT_DIR}/postgres.log" >&2
                return 1
            fi
            sleep 1
        done
    else
        echo "[postgres] Server already running on port ${POSTGRES_PORT}."
    fi
}

start_clickhouse() {
    local data_dir="$1"
    mkdir -p "$data_dir" "${data_dir}/tmp" "${data_dir}/user_files"

    if curl -s "http://localhost:${HTTP_PORT}/ping" | grep -q "Ok" 2>/dev/null; then
        echo "[clickhouse] Server already running on port ${HTTP_PORT}."
        return 0
    fi

    echo "[clickhouse] Starting server with data dir: $data_dir..."
    "${CLICKHOUSE_BIN_DIR}/clickhouse" server \
        --daemon \
        --pid-file="${CLICKHOUSE_PID}" \
        --log-file="${SCRIPT_DIR}/clickhouse.log" \
        -- \
        --path "$data_dir" \
        --tmp_path "${data_dir}/tmp" \
        --user_files_path "${data_dir}/user_files" \
        --http_port ${HTTP_PORT} \
        --tcp_port ${TCP_PORT} \
        --logger.level information

    echo "[clickhouse] Waiting for server to be ready..."
    for i in {1..30}; do
        if curl -s "http://localhost:${HTTP_PORT}/ping" | grep -q "Ok" 2>/dev/null; then
            echo "[clickhouse] Server is ready!"
            return 0
        fi
        if [[ $i -eq 30 ]]; then
            echo "ERROR: ClickHouse failed to start. Check ${SCRIPT_DIR}/clickhouse.log" >&2
            return 1
        fi
        sleep 1
    done
}

# Prepare data on SSD for a given database.
# Usage: prepare_data_on_ssd <db_type>
# db_type: duckdb, postgres, clickhouse
prepare_data_on_ssd() {
    local db_type="$1"
    local ssd_path hdd_path

    case "$db_type" in
        duckdb)
            ssd_path="${SSD_BASE}/duckdb_bench.db"
            hdd_path="${HDD_BASE}/duckdb_bench.db"
            ;;
        postgres)
            ssd_path="${SSD_BASE}/postgres-data"
            hdd_path="${HDD_BASE}/postgres-data"
            ;;
        clickhouse)
            ssd_path="${SSD_BASE}/clickhouse-data"
            hdd_path="${HDD_BASE}/clickhouse-data"
            ;;
    esac

    if [[ -e "$ssd_path" ]]; then
        echo "[data] $db_type data already exists on SSD: $ssd_path"
        return 0
    fi

    if [[ -e "$hdd_path" ]]; then
        echo "[data] Moving $db_type data from HDD to SSD..."
        echo "[data]   From: $hdd_path"
        echo "[data]   To:   $ssd_path"
        mkdir -p "$(dirname "$ssd_path")"
        mv "$hdd_path" "$ssd_path"
        sync
        echo "[data] Move complete."
        return 0
    fi

    echo "[data] $db_type data not found anywhere. Will create on SSD."
    mkdir -p "$(dirname "$ssd_path")"

    case "$db_type" in
        duckdb)
            echo "[data] Loading data into DuckDB at $ssd_path..."
            cd "$PROJECT_DIR"
            cargo run --release --bin load-duckdb --features db-duckdb -- \
                --format "$FORMAT" \
                --input "$INPUT_FILE" \
                --db "$ssd_path" \
                --table "$TABLE" \
                --threads 14
            sync
            echo "[data] DuckDB data created."
            ;;
        postgres)
            # Start postgres first, then load
            start_postgres "$ssd_path"
            local db_conn="postgres://${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/bench"
            # Create database if needed
            local db_conn_base="postgres://${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/postgres"
            local db_exists
            db_exists=$(psql "$db_conn_base" -tAc "SELECT EXISTS (SELECT FROM pg_database WHERE datname = 'bench')" 2>/dev/null || echo "f")
            if [[ "$db_exists" = "f" ]]; then
                psql "$db_conn_base" -c "CREATE DATABASE bench" >/dev/null
            fi
            echo "[data] Loading data into PostgreSQL..."
            cd "$PROJECT_DIR"
            cargo run --release --bin load-postgres --features db-postgres -- \
                --format "$FORMAT" \
                --input "$INPUT_FILE" \
                --db "$db_conn" \
                --table "$TABLE" \
                --threads 14
            psql "$db_conn" -c "CHECKPOINT" >/dev/null
            sync
            echo "[data] PostgreSQL data created."
            ;;
        clickhouse)
            # Start clickhouse first, then load
            start_clickhouse "$ssd_path"
            echo "[data] Loading data into ClickHouse..."
            cd "$PROJECT_DIR"
            cargo run --release --bin load-clickhouse --features db-clickhouse -- \
                --format "$FORMAT" \
                --input "$INPUT_FILE" \
                --url "http://localhost:${HTTP_PORT}" \
                --database default \
                --table "$TABLE" \
                --threads 14
            sync
            echo "[data] ClickHouse data created."
            ;;
    esac
}

# Move data from SSD to HDD after benchmarking.
# Usage: move_data_to_hdd <db_type>
move_data_to_hdd() {
    local db_type="$1"
    local ssd_path hdd_path

    case "$db_type" in
        duckdb)
            ssd_path="${SSD_BASE}/duckdb_bench.db"
            hdd_path="${HDD_BASE}/duckdb_bench.db"
            ;;
        postgres)
            ssd_path="${SSD_BASE}/postgres-data"
            hdd_path="${HDD_BASE}/postgres-data"
            ;;
        clickhouse)
            ssd_path="${SSD_BASE}/clickhouse-data"
            hdd_path="${HDD_BASE}/clickhouse-data"
            ;;
    esac

    if [[ ! -e "$ssd_path" ]]; then
        echo "[data] No data on SSD to move for $db_type."
        return 0
    fi

    echo "[data] Moving $db_type data from SSD to HDD..."
    echo "[data]   From: $ssd_path"
    echo "[data]   To:   $hdd_path"
    mkdir -p "$(dirname "$hdd_path")"
    mv "$ssd_path" "$hdd_path"
    sync
    echo "[data] Move complete."
}

# Get PostgreSQL query plan for a given worker count (without executing).
# Usage: get_postgres_plan <worker_count> <db_connection>
# Outputs the plan text to stdout.
get_postgres_plan() {
    local workers="$1"
    local db_conn="$2"
    local total_kb

    # Parse memory to KB (matching sort_postgres.rs logic)
    local mem_upper
    mem_upper=$(echo "$MEMORY_LIMIT" | tr '[:lower:]' '[:upper:]')
    if [[ "$mem_upper" =~ ^([0-9.]+)G[B]?$ ]]; then
        total_kb=$(echo "${BASH_REMATCH[1]} * 1024 * 1024" | bc | cut -d. -f1)
    elif [[ "$mem_upper" =~ ^([0-9.]+)M[B]?$ ]]; then
        total_kb=$(echo "${BASH_REMATCH[1]} * 1024" | bc | cut -d. -f1)
    else
        echo "ERROR: Cannot parse MEMORY_LIMIT=$MEMORY_LIMIT" >&2
        return 1
    fi

    local work_mem_kb=$((total_kb / workers))

    psql "$db_conn" -tA <<EOF
BEGIN;
SET LOCAL transaction_read_only = on;
SET LOCAL work_mem = '${work_mem_kb}kB';
SET LOCAL max_parallel_workers_per_gather = ${workers};
SET LOCAL parallel_tuple_cost = 0;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL min_parallel_table_scan_size = '0';
SET LOCAL enable_parallel_append = on;
SET LOCAL temp_file_limit = -1;
EXPLAIN SELECT sort_key, payload FROM ${TABLE} ORDER BY sort_key;
ROLLBACK;
EOF
}

upload_logs() {
    local log_dir="$1"
    if ! command -v rclone >/dev/null 2>&1; then
        echo "[upload] rclone not found, skipping upload."
        return 0
    fi
    if [[ -z "${RCLONE_REMOTE:-}" ]]; then
        echo "[upload] RCLONE_REMOTE not set, skipping upload."
        return 0
    fi
    local dest="${RCLONE_REMOTE}/$(basename "$log_dir")"
    echo "[upload] Uploading $log_dir -> $dest ..."
    if rclone copy "$log_dir" "$dest" --progress; then
        echo "[upload] Upload complete: $dest"
    else
        echo "[upload] Warning: rclone upload failed (exit $?)" >&2
    fi
}

clear_system_caches() {
    if [[ -x "$CLEAR_CACHE_SCRIPT" ]]; then
        echo "[cache] Clearing system caches..."
        sudo "$CLEAR_CACHE_SCRIPT" || echo "Warning: Failed to clear caches"
    else
        echo "[cache] Warning: Cache clear script not found: $CLEAR_CACHE_SCRIPT"
    fi
}

# ── DuckDB Benchmark ─────────────────────────────────────────────────────────
run_duckdb_benchmark() {
    echo ""
    echo "========================================================"
    echo "  DuckDB Parallelism Sweep"
    echo "========================================================"

    stop_other_dbs "duckdb"
    check_ssd_space
    prepare_data_on_ssd "duckdb"

    clear_system_caches
    echo "[duckdb] Running sweep..."
    cd "$PROJECT_DIR"
    MEMORY_LIMIT="$MEMORY_LIMIT" \
    THREAD_COUNTS="$THREAD_COUNTS" \
    BENCHMARK_RUNS="$BENCHMARK_RUNS" \
    DB_FILE="${SSD_BASE}/duckdb_bench.db" \
    TEMP_DIR="${SSD_BASE}/duckdb_temp" \
    TABLE="$TABLE" \
    FORMAT="$FORMAT" \
    INPUT_FILE="$INPUT_FILE" \
    TIMEOUT_SECONDS="$TIMEOUT_SECONDS" \
        bash "${SCRIPT_DIR}/sweep_duckdb_parallelism.sh"

    echo "[duckdb] Sweep complete."
    # Upload logs (find the most recent duckdb sweep log dir)
    local duckdb_log_dir
    duckdb_log_dir=$(ls -dt ./logs/duckdb_parallelism_sweep_* 2>/dev/null | head -1)
    if [[ -n "$duckdb_log_dir" ]]; then
        upload_logs "$duckdb_log_dir"
    fi
    # Clean up temp dir on SSD
    rm -rf "${SSD_BASE}/duckdb_temp"
    move_data_to_hdd "duckdb"
}

# ── PostgreSQL Benchmark (with plan deduplication) ────────────────────────────
run_postgres_benchmark() {
    echo ""
    echo "========================================================"
    echo "  PostgreSQL Parallelism Sweep (with plan deduplication)"
    echo "========================================================"

    stop_other_dbs "postgres"
    check_ssd_space
    prepare_data_on_ssd "postgres"

    # Ensure server is started with SSD data dir
    start_postgres "${SSD_BASE}/postgres-data"

    local db_conn="postgres://${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/bench"

    # Create database if needed
    local db_conn_base="postgres://${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/postgres"
    local db_exists
    db_exists=$(psql "$db_conn_base" -tAc "SELECT EXISTS (SELECT FROM pg_database WHERE datname = 'bench')" 2>/dev/null || echo "f")
    if [[ "$db_exists" = "f" ]]; then
        psql "$db_conn_base" -c "CREATE DATABASE bench" >/dev/null
    fi

    local LOG_DIR="./logs/postgres_parallelism_sweep_${SWEEP_TIMESTAMP}"
    mkdir -p "$LOG_DIR"

    clear_system_caches
    echo "[postgres] Memory: $MEMORY_LIMIT"
    echo "[postgres] Worker counts: $THREAD_COUNTS"
    echo "[postgres] Benchmark runs: $BENCHMARK_RUNS"
    echo "[postgres] Log directory: $LOG_DIR"
    echo ""

    local prev_plan=""

    for W in $THREAD_COUNTS; do
        echo "========================================="
        echo "[postgres] Checking plan for $W workers..."
        echo "========================================="

        # Get the query plan for this worker count
        local current_plan
        current_plan=$(get_postgres_plan "$W" "$db_conn" 2>&1) || {
            echo "ERROR: Failed to get plan for $W workers"
            echo "Output: $current_plan"
            continue
        }

        # Compare with previous plan
        if [[ -n "$prev_plan" ]] && [[ "$current_plan" = "$prev_plan" ]]; then
            echo "[postgres] SKIPPING $W workers: plan identical to previous worker count."
            echo "[postgres] Plan dedup saved ${BENCHMARK_RUNS} benchmark run(s)."
            echo ""

            # Log the skip
            {
                echo "========================================="
                echo "PostgreSQL Parallelism Sweep - SKIPPED (identical plan)"
                echo "========================================="
                echo "Configuration: total_memory=$MEMORY_LIMIT, parallel_workers=$W"
                echo "Reason: Plan identical to previous worker count"
                echo "Time: $(date +"%Y-%m-%d %H:%M:%S")"
                echo ""
                echo "Plan:"
                echo "$current_plan"
            } > "${LOG_DIR}/${MEMORY_LIMIT}_${W}workers_SKIPPED.log"

            continue
        fi

        prev_plan="$current_plan"
        echo "[postgres] Plan differs from previous. Running $BENCHMARK_RUNS benchmark run(s)..."
        echo ""
        echo "Plan preview:"
        echo "$current_plan" | head -20
        echo ""

        # Run benchmarks for this worker count (adapted from sweep_postgres_parallelism.sh)
        for RUN in $(seq 1 "$BENCHMARK_RUNS"); do
            RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
            LOG_FILE="${LOG_DIR}/${MEMORY_LIMIT}_${W}workers_run${RUN}_${RUN_TIMESTAMP}.log"
            TEMP_OUTPUT="/tmp/postgres_sweep_${W}_run${RUN}_${RUN_TIMESTAMP}.log"

            echo "========================================="
            echo "Running with $W parallel workers... (Run $RUN of $BENCHMARK_RUNS)"
            echo "Start time: $(date +"%Y-%m-%d %H:%M:%S")"
            echo "========================================="
            echo "Log file: $LOG_FILE"
            echo ""

            set +e
            cd "$PROJECT_DIR"
            timeout "$TIMEOUT_SECONDS" cargo run --release --bin sort-postgres --features db-postgres -- \
                --db "$db_conn" \
                --table "$TABLE" \
                --total-memory "$MEMORY_LIMIT" \
                --parallel-workers "$W" 2>&1 | tee "$TEMP_OUTPUT"

            EXIT_CODE=${PIPESTATUS[0]}
            set -e

            COMMAND_OUTPUT=$(cat "$TEMP_OUTPUT")

            local SKIP_REMAINING=false
            if [[ $EXIT_CODE -eq 124 ]]; then
                echo ""
                echo "WARNING: Process timed out after ${TIMEOUT_SECONDS}s"
                echo "Skipping remaining benchmark runs for $W workers..."
                SKIP_REMAINING=true
            fi

            # Clear PostgreSQL caches
            psql "$db_conn" -c "DISCARD ALL" >/dev/null 2>&1 || true

            # Extract timing
            DURATION=$(echo "$COMMAND_OUTPUT" | grep "TIMING:" | awk '{print $2}')

            # Write log
            {
                echo "========================================="
                echo "PostgreSQL Parallelism Sweep - Configuration Log"
                echo "========================================="
                echo "Configuration: total_memory=$MEMORY_LIMIT, parallel_workers=$W, run=$RUN/$BENCHMARK_RUNS"
                echo "Database: $db_conn"
                echo "Table: $TABLE"
                echo "Timeout: ${TIMEOUT_SECONDS}s"
                echo "Start time: $(date +"%Y-%m-%d %H:%M:%S")"
                echo ""
                echo "Exit code: $EXIT_CODE"
                if [[ $EXIT_CODE -eq 0 ]]; then
                    echo "Status: SUCCESS"
                elif [[ $EXIT_CODE -eq 124 ]]; then
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
                if [[ -n "$DURATION" ]]; then
                    echo "Duration: ${DURATION}s"
                    echo "Result: $MEMORY_LIMIT,$W,$RUN,$DURATION"
                else
                    echo "WARNING: Could not extract timing information"
                fi
                echo "End time: $(date +"%Y-%m-%d %H:%M:%S")"
                echo "========================================="
            } > "$LOG_FILE"

            rm -f "$TEMP_OUTPUT"

            echo ""
            echo "========================================="
            if [[ -n "$DURATION" ]]; then
                echo "Result logged: total_memory=$MEMORY_LIMIT, parallel_workers=$W, run=$RUN/$BENCHMARK_RUNS, duration=${DURATION}s"
            else
                echo "Warning: Could not extract timing information"
            fi
            echo "End time: $(date +"%Y-%m-%d %H:%M:%S")"
            echo "========================================="

            clear_system_caches

            if [[ "$SKIP_REMAINING" = true ]]; then
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

    echo ""
    echo "=== PostgreSQL Sweep Complete ==="
    echo "Results saved to: $LOG_DIR"
    grep "Result:" "$LOG_DIR"/*.log 2>/dev/null || echo "No successful results found"

    upload_logs "$LOG_DIR"

    # Stop postgres and move data
    stop_postgres
    move_data_to_hdd "postgres"
}

# ── ClickHouse Benchmark ─────────────────────────────────────────────────────
run_clickhouse_benchmark() {
    echo ""
    echo "========================================================"
    echo "  ClickHouse Parallelism Sweep"
    echo "========================================================"

    stop_other_dbs "clickhouse"
    check_ssd_space
    prepare_data_on_ssd "clickhouse"

    # Ensure server is started with SSD data dir
    start_clickhouse "${SSD_BASE}/clickhouse-data"

    clear_system_caches
    echo "[clickhouse] Running sweep..."
    cd "$PROJECT_DIR"
    TOTAL_MEMORY="$MEMORY_LIMIT" \
    THREAD_COUNTS="$THREAD_COUNTS" \
    BENCHMARK_RUNS="$BENCHMARK_RUNS" \
    CLICKHOUSE_URL="http://localhost:${HTTP_PORT}" \
    TABLE="$TABLE" \
    FORMAT="$FORMAT" \
    INPUT_FILE="$INPUT_FILE" \
    TIMEOUT_SECONDS="$TIMEOUT_SECONDS" \
        bash "${SCRIPT_DIR}/sweep_clickhouse_parallelism.sh"

    # Upload logs (find the most recent clickhouse sweep log dir)
    local clickhouse_log_dir
    clickhouse_log_dir=$(ls -dt ./logs/clickhouse_parallelism_sweep_* 2>/dev/null | head -1)
    if [[ -n "$clickhouse_log_dir" ]]; then
        upload_logs "$clickhouse_log_dir"
    fi

    # Stop clickhouse and move data
    stop_clickhouse
    move_data_to_hdd "clickhouse"
}

# ── Main ──────────────────────────────────────────────────────────────────────

# Install database binaries upfront (no-op if already present)
for DB in $DATABASES; do
    case "$DB" in
        postgres)   install_postgres ;;
        clickhouse) install_clickhouse ;;
    esac
done

for DB in $DATABASES; do
    case "$DB" in
        duckdb)
            run_duckdb_benchmark
            ;;
        postgres)
            run_postgres_benchmark
            ;;
        clickhouse)
            run_clickhouse_benchmark
            ;;
        *)
            echo "ERROR: Unknown database: $DB"
            exit 1
            ;;
    esac
done

echo ""
echo "========================================================"
echo "  All benchmarks complete!"
echo "========================================================"
echo "Data has been moved to HDD: $HDD_BASE"
echo "Logs are in: ./logs/"
