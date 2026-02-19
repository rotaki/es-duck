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
#   HOST_TAG        - host/machine tag added to upload path (default: hostname)
#   DATASET_NAME    - dataset key used in DB file/dir names (default: FORMAT)
#   CGROUP_MODE     - cgroup enforcement mode: on|off (default: on)
#   PREFLIGHT_ONLY  - if set to 1, only run dependency checks and exit

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
HOST_TAG="${HOST_TAG:-$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo unknown-host)}"
DATASET_NAME="${DATASET_NAME:-${FORMAT}}"
CGROUP_MODE="${CGROUP_MODE:-on}" # on | off
PREFLIGHT_ONLY="${PREFLIGHT_ONLY:-0}"
# Stores the most recent systemd-run scope unit created by run_with_cgroup_limits.
LAST_CGROUP_SCOPE_UNIT=""

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
DATASET_NAME_SAFE=$(echo "${DATASET_NAME}" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9._-' '_' | sed -E 's/^_+|_+$//g')
if [[ -z "${DATASET_NAME_SAFE}" ]]; then
    DATASET_NAME_SAFE="dataset"
fi

HOST_TAG_SAFE=$(echo "${HOST_TAG}" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9._-' '_' | sed -E 's/^_+|_+$//g')
if [[ -z "${HOST_TAG_SAFE}" ]]; then
    HOST_TAG_SAFE="unknown-host"
fi

echo "========================================================"
echo "  Unified Benchmark Orchestrator"
echo "========================================================"
echo "Input:          $INPUT_FILE"
echo "Format:         $FORMAT"
echo "Dataset:        $DATASET_NAME_SAFE"
echo "Host tag:       $HOST_TAG_SAFE"
echo "Memory:         $MEMORY_LIMIT"
echo "Thread counts:  $THREAD_COUNTS"
echo "Benchmark runs: $BENCHMARK_RUNS"
echo "SSD base:       $SSD_BASE"
echo "HDD base:       $HDD_BASE"
echo "Databases:      $DATABASES"
echo "Timeout:        ${TIMEOUT_SECONDS}s"
echo "Cgroup mode:    $CGROUP_MODE"
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

PREFLIGHT_MISSING=()
PREFLIGHT_WARNINGS=()

add_preflight_missing() {
    PREFLIGHT_MISSING+=("$1")
}

add_preflight_warning() {
    PREFLIGHT_WARNINGS+=("$1")
}

require_command() {
    local cmd="$1"
    local why="${2:-}"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        if [[ -n "$why" ]]; then
            add_preflight_missing "command '$cmd' is required ($why)"
        else
            add_preflight_missing "command '$cmd' is required"
        fi
    fi
}

require_executable() {
    local path="$1"
    local why="${2:-}"
    if [[ ! -x "$path" ]]; then
        if [[ -n "$why" ]]; then
            add_preflight_missing "missing executable '$path' ($why)"
        else
            add_preflight_missing "missing executable '$path'"
        fi
    fi
}

require_file() {
    local path="$1"
    local why="${2:-}"
    if [[ ! -f "$path" ]]; then
        if [[ -n "$why" ]]; then
            add_preflight_missing "missing file '$path' ($why)"
        else
            add_preflight_missing "missing file '$path'"
        fi
    fi
}

preflight_checks() {
    PREFLIGHT_MISSING=()
    PREFLIGHT_WARNINGS=()

    echo "[preflight] Checking scripts and commands before benchmark run..."

    if [[ ! -f "$INPUT_FILE" ]]; then
        add_preflight_missing "input file does not exist: $INPUT_FILE"
    fi

    require_command cargo "build/run load-* and sort-* binaries"
    require_command timeout "enforce per-run timeouts"
    require_command awk "memory parsing and reporting"
    require_command sed "string parsing"
    require_command grep "plan/timing extraction"
    require_command tee "stream benchmark output to logs"
    require_command df "SSD capacity checks"
    require_command mv "move DB data between SSD/HDD"
    require_command sync "flush data to disk"
    require_command curl "health checks and binary downloads"

    case "$CGROUP_MODE" in
        on|off) ;;
        *)
            add_preflight_missing "invalid CGROUP_MODE='$CGROUP_MODE' (expected: on|off)"
            ;;
    esac

    if [[ "$CGROUP_MODE" == "on" ]]; then
        if [[ "$(uname -s)" != "Linux" ]]; then
            add_preflight_missing "CGROUP_MODE=on requires Linux"
        elif ! command -v systemd-run >/dev/null 2>&1; then
            add_preflight_missing "CGROUP_MODE=on requires 'systemd-run'"
        fi
        if ! parse_memory_to_bytes "$MEMORY_LIMIT" >/dev/null 2>&1; then
            add_preflight_missing "cannot parse MEMORY_LIMIT='$MEMORY_LIMIT' for cgroup enforcement"
        fi
    fi

    if [[ ! -x "$CLEAR_CACHE_SCRIPT" ]]; then
        add_preflight_warning "cache-clear script not executable: $CLEAR_CACHE_SCRIPT (system cache drop will be skipped)"
    fi

    if [[ -n "${RCLONE_REMOTE:-}" ]] && ! command -v rclone >/dev/null 2>&1; then
        add_preflight_warning "rclone not found: uploads to RCLONE_REMOTE will be skipped"
    fi

    local db
    for db in $DATABASES; do
        case "$db" in
            duckdb)
                require_file "${SCRIPT_DIR}/sweep_duckdb_parallelism.sh" "DuckDB sweep runner"
                ;;
            postgres)
                require_command psql "PostgreSQL control and queries"
                require_command pg_isready "PostgreSQL readiness checks"
                ;;
            clickhouse)
                require_file "${SCRIPT_DIR}/sweep_clickhouse_parallelism.sh" "ClickHouse sweep runner"
                ;;
            *)
                add_preflight_missing "unknown database in DATABASES: '$db'"
                ;;
        esac
    done

    if [[ ${#PREFLIGHT_WARNINGS[@]} -gt 0 ]]; then
        echo "[preflight] Warnings:"
        local warning
        for warning in "${PREFLIGHT_WARNINGS[@]}"; do
            echo "  - $warning"
        done
    fi

    if [[ ${#PREFLIGHT_MISSING[@]} -gt 0 ]]; then
        echo "[preflight] Missing requirements:"
        local missing
        for missing in "${PREFLIGHT_MISSING[@]}"; do
            echo "  - $missing"
        done
        echo "[preflight] Failed. Fix the missing items, then rerun."
        return 1
    fi

    echo "[preflight] All required scripts/commands are available."
    return 0
}

dataset_db_path() {
    local base="$1"
    local db_type="$2"

    case "$db_type" in
        duckdb)
            echo "${base}/duckdb_bench_${DATASET_NAME_SAFE}.db"
            ;;
        postgres)
            echo "${base}/postgres-data_${DATASET_NAME_SAFE}"
            ;;
        clickhouse)
            echo "${base}/clickhouse-data_${DATASET_NAME_SAFE}"
            ;;
        *)
            echo "ERROR: Unknown database type for path resolution: $db_type" >&2
            return 1
            ;;
    esac
}

parse_memory_to_bytes() {
    local raw="${1//[[:space:]]/}"
    local mem="${raw^^}"
    local value

    # Prefer GB/MB labels in scripts, but keep legacy binary-unit suffixes for compatibility.
    if [[ "$mem" =~ ^([0-9]+([.][0-9]+)?)(GB|GIB|G)$ ]]; then
        value="${BASH_REMATCH[1]}"
        awk -v v="$value" 'BEGIN { printf "%.0f\n", v * 1024 * 1024 * 1024 }'
    elif [[ "$mem" =~ ^([0-9]+([.][0-9]+)?)(MB|MIB|M)$ ]]; then
        value="${BASH_REMATCH[1]}"
        awk -v v="$value" 'BEGIN { printf "%.0f\n", v * 1024 * 1024 }'
    elif [[ "$mem" =~ ^([0-9]+([.][0-9]+)?)(KB|KIB|K)$ ]]; then
        value="${BASH_REMATCH[1]}"
        awk -v v="$value" 'BEGIN { printf "%.0f\n", v * 1024 }'
    elif [[ "$mem" =~ ^([0-9]+([.][0-9]+)?)B?$ ]]; then
        value="${BASH_REMATCH[1]}"
        awk -v v="$value" 'BEGIN { printf "%.0f\n", v }'
    else
        return 1
    fi
}

cgroup_memory_limit_bytes() {
    local memory_limit="$1"
    local base_bytes

    if ! base_bytes=$(parse_memory_to_bytes "$memory_limit"); then
        return 1
    fi

    # Enforce cgroup limit at 120% of benchmark memory budget.
    echo $((base_bytes * 120 / 100))
}

verify_pid_in_cgroup_scope() {
    local pid="$1"
    local expected_unit="$2"
    local process_label="$3"

    if [[ "$CGROUP_MODE" != "on" ]]; then
        return 0
    fi
    if [[ -z "$expected_unit" ]]; then
        echo "[cgroup] ERROR: no expected scope unit recorded for ${process_label} verification." >&2
        return 1
    fi
    if [[ -z "$pid" ]] || [[ ! "$pid" =~ ^[0-9]+$ ]]; then
        echo "[cgroup] ERROR: invalid PID '${pid}' for ${process_label} cgroup verification." >&2
        return 1
    fi

    local cgroup_file="/proc/${pid}/cgroup"
    if [[ ! -r "$cgroup_file" ]]; then
        echo "[cgroup] ERROR: cannot read ${cgroup_file} for ${process_label}." >&2
        return 1
    fi

    if grep -Fq "${expected_unit}.scope" "$cgroup_file"; then
        echo "[cgroup] Verified ${process_label} pid=${pid} in ${expected_unit}.scope"
        return 0
    fi

    echo "[cgroup] ERROR: ${process_label} pid=${pid} not in expected scope ${expected_unit}.scope" >&2
    echo "[cgroup] Observed cgroup memberships:" >&2
    sed 's/^/[cgroup]   /' "$cgroup_file" >&2
    return 1
}

run_with_cgroup_limits() {
    local memory_limit="$1"
    local scope_name="$2"
    shift 2

    LAST_CGROUP_SCOPE_UNIT=""

    if [[ "$CGROUP_MODE" == "off" ]]; then
        "$@"
        return $?
    fi

    if [[ "$CGROUP_MODE" != "on" ]]; then
        echo "[cgroup] ERROR: invalid CGROUP_MODE='$CGROUP_MODE' (expected on|off)." >&2
        return 1
    fi

    local limit_bytes
    if ! limit_bytes=$(cgroup_memory_limit_bytes "$memory_limit"); then
        echo "[cgroup] ERROR: cannot parse memory limit '$memory_limit'." >&2
        return 1
    fi

    if [[ "$(uname -s)" != "Linux" ]]; then
        echo "[cgroup] ERROR: CGROUP_MODE=on requires Linux." >&2
        return 1
    fi
    if ! command -v systemd-run >/dev/null 2>&1; then
        echo "[cgroup] ERROR: CGROUP_MODE=on requires systemd-run." >&2
        return 1
    fi

    local unit_name="es-duck-${scope_name}-${DATASET_NAME_SAFE}-$$-$(date +%s)"
    echo "[cgroup] Applying limits for ${scope_name}: memory.high=${limit_bytes}, memory.max=${limit_bytes}, memory.swap.max=0"

    # Probe cgroup scope creation with a no-op before running the real command.
    # This separates systemd-run infrastructure failures (scope can't be created)
    # from child process failures (OOM kill exit 137, non-zero exit, etc.).
    # Without this, an OOM-killed child would be mistaken for a setup failure and
    # the binary would be silently retried without memory limits.
    if ! systemd-run --user --scope --quiet --collect \
        --unit "${unit_name}-probe" \
        --property "MemoryAccounting=yes" \
        --property "MemoryHigh=${limit_bytes}" \
        --property "MemoryMax=${limit_bytes}" \
        --property "MemorySwapMax=0" \
        -- true 2>/dev/null; then
        echo "[cgroup] ERROR: failed to create cgroup scope (setup error)." >&2
        return 1
    fi

    LAST_CGROUP_SCOPE_UNIT="${unit_name}"

    # Scope creation works — any failure from here is the child's (e.g. OOM kill).
    # Do not retry: that would silently run the workload without memory constraints.
    systemd-run --user --scope --quiet --collect \
        --unit "${unit_name}" \
        --property "MemoryAccounting=yes" \
        --property "MemoryHigh=${limit_bytes}" \
        --property "MemoryMax=${limit_bytes}" \
        --property "MemorySwapMax=0" \
        -- "$@"
}

stop_postgres() {
    echo "[helper] Stopping PostgreSQL if running..."
    # Try stopping with pg_ctl using all known postgres data dirs (dataset-specific and legacy).
    local data_dir
    shopt -s nullglob
    for data_dir in "${SSD_BASE}"/postgres-data* "${HDD_BASE}"/postgres-data* "${SCRIPT_DIR}"/postgres-data*; do
        if [[ -d "$data_dir" ]] && [[ -x "${POSTGRES_DIR}/bin/pg_ctl" ]]; then
            "${POSTGRES_DIR}/bin/pg_ctl" -D "$data_dir" stop 2>/dev/null || true
        fi
    done
    shopt -u nullglob

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
    local avail_gib=$((avail_kb / 1024 / 1024))
    echo "[helper] SSD available: ${avail_gib}GB"
    if [[ $avail_gib -lt 50 ]]; then
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
        local postgres_scope_unit=""
        echo "[postgres] Starting server with data dir: $data_dir..."
        run_with_cgroup_limits "$MEMORY_LIMIT" "postgres-server" \
            "${POSTGRES_DIR}/bin/pg_ctl" -D "$data_dir" -l "${SCRIPT_DIR}/postgres.log" \
            -o "-p ${POSTGRES_PORT} -k ${SCRIPT_DIR} -c max_worker_processes=128 -c max_parallel_workers=128 -c max_connections=200" \
            start
        postgres_scope_unit="${LAST_CGROUP_SCOPE_UNIT}"

        echo "[postgres] Waiting for server to be ready..."
        for i in {1..30}; do
            if pg_isready -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" >/dev/null 2>&1; then
                local postmaster_pid
                postmaster_pid=$(head -n 1 "${data_dir}/postmaster.pid" 2>/dev/null | tr -d '[:space:]' || true)
                verify_pid_in_cgroup_scope "$postmaster_pid" "$postgres_scope_unit" "PostgreSQL server" || return 1
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
    local clickhouse_scope_unit=""
    mkdir -p "$data_dir" "${data_dir}/tmp" "${data_dir}/user_files"

    if curl -s "http://localhost:${HTTP_PORT}/ping" | grep -q "Ok" 2>/dev/null; then
        echo "[clickhouse] Server already running on port ${HTTP_PORT}."
        return 0
    fi

    echo "[clickhouse] Starting server with data dir: $data_dir..."
    run_with_cgroup_limits "$MEMORY_LIMIT" "clickhouse-server" \
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
    clickhouse_scope_unit="${LAST_CGROUP_SCOPE_UNIT}"

    echo "[clickhouse] Waiting for server to be ready..."
    for i in {1..30}; do
        if curl -s "http://localhost:${HTTP_PORT}/ping" | grep -q "Ok" 2>/dev/null; then
            local clickhouse_pid
            clickhouse_pid=$(tr -d '[:space:]' < "${CLICKHOUSE_PID}" 2>/dev/null || true)
            verify_pid_in_cgroup_scope "$clickhouse_pid" "$clickhouse_scope_unit" "ClickHouse server" || return 1
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

    ssd_path=$(dataset_db_path "$SSD_BASE" "$db_type")
    hdd_path=$(dataset_db_path "$HDD_BASE" "$db_type")

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
            run_with_cgroup_limits "$MEMORY_LIMIT" "duckdb-load" \
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
            run_with_cgroup_limits "$MEMORY_LIMIT" "postgres-load-client" \
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
            run_with_cgroup_limits "$MEMORY_LIMIT" "clickhouse-load-client" \
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

    ssd_path=$(dataset_db_path "$SSD_BASE" "$db_type")
    hdd_path=$(dataset_db_path "$HDD_BASE" "$db_type")

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

# Get PostgreSQL query plan for a given parallel worker count (without executing).
# Usage: get_postgres_plan <parallel_worker_count> <db_connection>
# Outputs the plan text to stdout.
get_postgres_plan() {
    local workers="$1"
    local db_conn="$2"
    local total_bytes total_kb

    # Parse memory to KiB (matching sort_postgres.rs logic).
    # Accepts GB/MB units (plus legacy suffixes and shorthand G/M).
    if ! total_bytes=$(parse_memory_to_bytes "$MEMORY_LIMIT"); then
        echo "ERROR: Cannot parse MEMORY_LIMIT=$MEMORY_LIMIT (expected GB or MB, e.g. '10GB')" >&2
        return 1
    fi
    total_kb=$((total_bytes / 1024))

    local total_procs=$((workers + 1))
    local work_mem_kb=$((total_kb / total_procs))

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

# Convert requested total threads/processes into PostgreSQL parallel workers.
# Threads include the leader process, so workers = threads - 1.
threads_to_postgres_workers() {
    local threads="$1"
    if [[ ! "$threads" =~ ^[0-9]+$ ]] || [[ "$threads" -lt 1 ]]; then
        echo "ERROR: Invalid thread count '$threads' (must be an integer >= 1)" >&2
        return 1
    fi
    echo $((threads - 1))
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
    local dest="${RCLONE_REMOTE}/${HOST_TAG_SAFE}/$(basename "$log_dir")"
    echo "[upload] Uploading $log_dir -> $dest ..."
    if rclone copy "$log_dir" "$dest" --progress; then
        echo "[upload] Upload complete: $dest"
    else
        echo "[upload] Warning: rclone upload failed (exit $?)" >&2
    fi
}

upload_log_file() {
    local log_file="$1"
    local log_dir="$2"

    if ! command -v rclone >/dev/null 2>&1; then
        echo "[upload] rclone not found, skipping per-run upload."
        return 0
    fi
    if [[ -z "${RCLONE_REMOTE:-}" ]]; then
        echo "[upload] RCLONE_REMOTE not set, skipping per-run upload."
        return 0
    fi

    local dest_dir="${RCLONE_REMOTE}/${HOST_TAG_SAFE}/$(basename "$log_dir")"
    local dest_file="${dest_dir}/$(basename "$log_file")"
    echo "[upload] Uploading log file $log_file -> $dest_file ..."
    if rclone copyto "$log_file" "$dest_file" --progress; then
        echo "[upload] Upload complete: $dest_file"
    else
        echo "[upload] Warning: per-run rclone upload failed (exit $?)" >&2
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
    local duckdb_db_file duckdb_temp_dir
    duckdb_db_file=$(dataset_db_path "$SSD_BASE" "duckdb")
    duckdb_temp_dir="${SSD_BASE}/duckdb_temp_${DATASET_NAME_SAFE}"

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
    run_with_cgroup_limits "$MEMORY_LIMIT" "duckdb-sweep" \
        env MEMORY_LIMIT="$MEMORY_LIMIT" \
            THREAD_COUNTS="$THREAD_COUNTS" \
            BENCHMARK_RUNS="$BENCHMARK_RUNS" \
            DB_FILE="$duckdb_db_file" \
            TEMP_DIR="$duckdb_temp_dir" \
            TABLE="$TABLE" \
            FORMAT="$FORMAT" \
            INPUT_FILE="$INPUT_FILE" \
            TIMEOUT_SECONDS="$TIMEOUT_SECONDS" \
            RCLONE_REMOTE="$RCLONE_REMOTE" \
            bash "${SCRIPT_DIR}/sweep_duckdb_parallelism.sh"

    echo "[duckdb] Sweep complete."
    # Upload logs (find the most recent duckdb sweep log dir)
    local duckdb_log_dir
    duckdb_log_dir=$(ls -dt ./logs/duckdb_parallelism_sweep_* 2>/dev/null | head -1)
    if [[ -n "$duckdb_log_dir" ]]; then
        upload_logs "$duckdb_log_dir"
    fi
    # Clean up temp dir on SSD
    rm -rf "$duckdb_temp_dir"
    move_data_to_hdd "duckdb"
}

# ── PostgreSQL Benchmark (with plan deduplication) ────────────────────────────
run_postgres_benchmark() {
    local postgres_ssd_dir
    postgres_ssd_dir=$(dataset_db_path "$SSD_BASE" "postgres")

    echo ""
    echo "========================================================"
    echo "  PostgreSQL Parallelism Sweep (with plan deduplication)"
    echo "========================================================"

    stop_other_dbs "postgres"
    check_ssd_space
    prepare_data_on_ssd "postgres"

    # Ensure server is started with SSD data dir
    start_postgres "$postgres_ssd_dir"

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
    echo "[postgres] Thread counts (total processes incl. leader): $THREAD_COUNTS"
    echo "[postgres] Benchmark runs: $BENCHMARK_RUNS"
    echo "[postgres] Log directory: $LOG_DIR"
    echo ""

    local prev_plan=""

    for T in $THREAD_COUNTS; do
        local W
        W=$(threads_to_postgres_workers "$T") || {
            echo "ERROR: Invalid PostgreSQL thread count: $T"
            continue
        }
        local TOTAL_PROCS=$((W + 1))

        echo "========================================="
        echo "[postgres] Checking plan for $T threads ($W workers + 1 leader)..."
        echo "========================================="

        # Get the query plan for this worker count
        local current_plan
        current_plan=$(get_postgres_plan "$W" "$db_conn" 2>&1) || {
            echo "ERROR: Failed to get plan for $T threads ($W workers)"
            echo "Output: $current_plan"
            continue
        }

        # Compare with previous plan
        if [[ -n "$prev_plan" ]] && [[ "$current_plan" = "$prev_plan" ]]; then
            echo "[postgres] SKIPPING $T threads: plan identical to previous thread count."
            echo "[postgres] Plan dedup saved ${BENCHMARK_RUNS} benchmark run(s)."
            echo ""

            # Log the skip
            {
                echo "========================================="
                echo "PostgreSQL Parallelism Sweep - SKIPPED (identical plan)"
                echo "========================================="
                echo "Configuration: total_memory=$MEMORY_LIMIT, threads=$T, parallel_workers=$W, total_processes=$TOTAL_PROCS"
                echo "Reason: Plan identical to previous thread count"
                echo "Time: $(date +"%Y-%m-%d %H:%M:%S")"
                echo ""
                echo "Plan:"
                echo "$current_plan"
            } > "${LOG_DIR}/${MEMORY_LIMIT}_${T}threads_${W}workers_SKIPPED.log"

            upload_log_file "${LOG_DIR}/${MEMORY_LIMIT}_${T}threads_${W}workers_SKIPPED.log" "$LOG_DIR"

            continue
        fi

        prev_plan="$current_plan"
        echo "[postgres] Plan differs from previous. Running $BENCHMARK_RUNS benchmark run(s)..."
        echo ""
        echo "Plan preview:"
        echo "$current_plan" | head -20
        echo ""

        # Run benchmarks for this thread count (adapted from sweep_postgres_parallelism.sh)
        for RUN in $(seq 1 "$BENCHMARK_RUNS"); do
            RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
            LOG_FILE="${LOG_DIR}/${MEMORY_LIMIT}_${T}threads_${W}workers_run${RUN}_${RUN_TIMESTAMP}.log"
            TEMP_OUTPUT="/tmp/postgres_sweep_${T}threads_${W}workers_run${RUN}_${RUN_TIMESTAMP}.log"

            echo "========================================="
            echo "Running with $T threads ($W workers + 1 leader)... (Run $RUN of $BENCHMARK_RUNS)"
            echo "Start time: $(date +"%Y-%m-%d %H:%M:%S")"
            echo "========================================="
            echo "Log file: $LOG_FILE"
            echo ""

            set +e
            cd "$PROJECT_DIR"
            run_with_cgroup_limits "$MEMORY_LIMIT" "postgres-sort-client" \
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
                echo "Skipping remaining benchmark runs for $T threads..."
                SKIP_REMAINING=true
            elif [[ $EXIT_CODE -eq 137 ]]; then
                echo ""
                echo "WARNING: Process killed by memory manager (OOM kill, exit 137)"
                echo "Skipping remaining benchmark runs for $T threads..."
                SKIP_REMAINING=true
            fi

            # Clear PostgreSQL caches
            psql "$db_conn" -c "DISCARD ALL" >/dev/null 2>&1 || true

            # Extract timing (grep returns non-zero if no match; suppress to avoid aborting
            # the orchestrator on OOM kill or any run that exits without printing TIMING:)
            DURATION=$(echo "$COMMAND_OUTPUT" | grep "TIMING:" | awk '{print $2}' || true)

            # Write log
            {
                echo "========================================="
                echo "PostgreSQL Parallelism Sweep - Configuration Log"
                echo "========================================="
                echo "Configuration: total_memory=$MEMORY_LIMIT, threads=$T, parallel_workers=$W, total_processes=$TOTAL_PROCS, run=$RUN/$BENCHMARK_RUNS"
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
                elif [[ $EXIT_CODE -eq 137 ]]; then
                    echo "Status: OOM_KILLED"
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

            upload_log_file "$LOG_FILE" "$LOG_DIR"

            rm -f "$TEMP_OUTPUT"

            echo ""
            echo "========================================="
            if [[ -n "$DURATION" ]]; then
                echo "Result logged: total_memory=$MEMORY_LIMIT, threads=$T, parallel_workers=$W, run=$RUN/$BENCHMARK_RUNS, duration=${DURATION}s"
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
    local clickhouse_ssd_dir
    clickhouse_ssd_dir=$(dataset_db_path "$SSD_BASE" "clickhouse")

    echo ""
    echo "========================================================"
    echo "  ClickHouse Parallelism Sweep"
    echo "========================================================"

    stop_other_dbs "clickhouse"
    check_ssd_space
    prepare_data_on_ssd "clickhouse"

    # Ensure server is started with SSD data dir
    start_clickhouse "$clickhouse_ssd_dir"

    clear_system_caches
    echo "[clickhouse] Running sweep..."
    cd "$PROJECT_DIR"
    run_with_cgroup_limits "$MEMORY_LIMIT" "clickhouse-sweep-client" \
        env TOTAL_MEMORY="$MEMORY_LIMIT" \
            THREAD_COUNTS="$THREAD_COUNTS" \
            BENCHMARK_RUNS="$BENCHMARK_RUNS" \
            CLICKHOUSE_URL="http://localhost:${HTTP_PORT}" \
            TABLE="$TABLE" \
            FORMAT="$FORMAT" \
            INPUT_FILE="$INPUT_FILE" \
            TIMEOUT_SECONDS="$TIMEOUT_SECONDS" \
            RCLONE_REMOTE="$RCLONE_REMOTE" \
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

preflight_checks

if [[ "$PREFLIGHT_ONLY" == "1" ]]; then
    echo "[preflight] PREFLIGHT_ONLY=1 set. Exiting before benchmark execution."
    exit 0
fi

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
