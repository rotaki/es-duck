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

echo "========================================================"
echo "  Running benchmarks for both datasets"
echo "========================================================"
echo ""

# ── Dataset 1: gensort ────────────────────────────────────────────────────────
echo "############################################################"
echo "  DATASET 1/2: gensort_200GiB.data (format: gensort)"
echo "############################################################"
echo ""

INPUT_FILE="${DATASETS_DIR}/gensort_200GiB.data" \
FORMAT="gensort" \
RCLONE_REMOTE="gdrive:bench_results/gensort" \
    bash "${SCRIPT_DIR}/run_all_benchmarks.sh"

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
RCLONE_REMOTE="gdrive:bench_results/lineitem" \
    bash "${SCRIPT_DIR}/run_all_benchmarks.sh"

echo ""
echo "############################################################"
echo "  All dataset benchmarks complete!"
echo "############################################################"
