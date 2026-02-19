#!/bin/bash
# Runs the full benchmark suite for both datasets: gensort then lineitem.
#
# Usage:
#   ./scripts/run_both_datasets.sh
#
# Both datasets live on HDD at /tank/local/riki/datasets/
# Each dataset runs all 3 databases (duckdb, postgres, clickhouse).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASETS_DIR="/tank/local/riki/datasets"
RUN_ALL_SCRIPT="${SCRIPT_DIR}/run_all_benchmarks.sh"

if [[ ! -f "${RUN_ALL_SCRIPT}" ]]; then
    echo "ERROR: missing orchestrator script: ${RUN_ALL_SCRIPT}" >&2
    exit 1
fi

run_dataset_preflight() {
    local input_file="$1"
    local format="$2"
    local dataset_name="$3"
    local remote="$4"

    echo "--------------------------------------------------------"
    echo "  Preflight: ${dataset_name}"
    echo "--------------------------------------------------------"

    INPUT_FILE="${input_file}" \
    FORMAT="${format}" \
    DATASET_NAME="${dataset_name}" \
    RCLONE_REMOTE="${remote}" \
    PREFLIGHT_ONLY=1 \
        bash "${RUN_ALL_SCRIPT}"
    echo ""
}

echo "========================================================"
echo "  Running benchmarks for both datasets"
echo "========================================================"
echo ""

echo "Running preflight checks for both datasets before starting benchmarks..."
echo ""

run_dataset_preflight "${DATASETS_DIR}/gensort_200GiB.data" "gensort" "gensort" "gdrive:bench_results/gensort"
run_dataset_preflight "${DATASETS_DIR}/lineitem_sf500.k-8-9-13-14-15.v-0-3.kvbin" "kvbin" "lineitem" "gdrive:bench_results/lineitem"

echo "All preflight checks passed."
echo ""

# ── Dataset 1: gensort ────────────────────────────────────────────────────────
echo "############################################################"
echo "  DATASET 1/2: gensort_200GiB.data (format: gensort)"
echo "############################################################"
echo ""

INPUT_FILE="${DATASETS_DIR}/gensort_200GiB.data" \
FORMAT="gensort" \
DATASET_NAME="gensort" \
RCLONE_REMOTE="gdrive:bench_results/gensort" \
    bash "${RUN_ALL_SCRIPT}"

echo ""
echo "############################################################"
echo "  gensort benchmarks complete!"
echo "############################################################"
echo ""

# ── Dataset 2: lineitem ──────────────────────────────────────────────────────
echo "############################################################"
echo "  DATASET 2/2: lineitem_sf500 kvbin (format: kvbin)"
echo "############################################################"
echo ""

INPUT_FILE="${DATASETS_DIR}/lineitem_sf500.k-8-9-13-14-15.v-0-3.kvbin" \
FORMAT="kvbin" \
DATASET_NAME="lineitem" \
RCLONE_REMOTE="gdrive:bench_results/lineitem" \
    bash "${RUN_ALL_SCRIPT}"

echo ""
echo "############################################################"
echo "  All dataset benchmarks complete!"
echo "############################################################"
