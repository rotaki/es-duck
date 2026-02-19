# Benchmark Scripts

This document describes the benchmark orchestration scripts that run parallelism sweeps across DuckDB, PostgreSQL, and ClickHouse.

## Scripts Overview

| Script | Purpose |
|--------|---------|
| `run_both_datasets.sh` | Top-level: runs all benchmarks for both gensort and lineitem datasets |
| `run_all_benchmarks.sh` | Orchestrator: runs parallelism sweeps for all 3 databases on a single dataset |
| `sweep_duckdb_parallelism.sh` | DuckDB sweep (called by orchestrator) |
| `sweep_postgres_parallelism.sh` | PostgreSQL sweep (standalone; orchestrator uses its own inline version) |
| `sweep_clickhouse_parallelism.sh` | ClickHouse sweep (called by orchestrator) |

## Quick Start

```bash
# Run everything (both datasets, all databases)
./scripts/run_both_datasets.sh

# Run a single dataset
INPUT_FILE=/tank/local/riki/datasets/gensort_200GiB.data \
FORMAT=gensort \
    ./scripts/run_all_benchmarks.sh

# Run only one database
INPUT_FILE=/path/to/data.dat DATABASES="postgres" ./scripts/run_all_benchmarks.sh

# Quick test with small data
INPUT_FILE=testdata/test_gensort.dat \
THREAD_COUNTS="4 8" \
BENCHMARK_RUNS=1 \
SSD_BASE=/tmp/bench_ssd \
HDD_BASE=/tmp/bench_hdd \
RCLONE_REMOTE="" \
    ./scripts/run_all_benchmarks.sh
```

## Configuration

All settings are controlled via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `INPUT_FILE` | **(required)** | Path to the input data file |
| `FORMAT` | `gensort` | Input format (`gensort` or `kvbin`) |
| `MEMORY_LIMIT` | `10GiB` | Memory budget for sorting |
| `THREAD_COUNTS` | `4 8 16 24 32 40 44` | Space-separated thread counts to sweep (PostgreSQL uses `workers = threads - 1`) |
| `BENCHMARK_RUNS` | `3` | Number of runs per configuration |
| `TABLE` | `bench_data` | Table name in each database |
| `TIMEOUT_SECONDS` | `7200` | Per-benchmark timeout (2 hours) |
| `SSD_BASE` | `/mnt/nvme1/rotaki/es-duck/scripts` | SSD directory for database data during benchmarks |
| `HDD_BASE` | `/tank/local/riki/datasets` | HDD directory for long-term storage |
| `DATABASES` | `duckdb postgres clickhouse` | Which databases to benchmark |
| `RCLONE_REMOTE` | `gdrive:bench_results/gensort` | rclone destination for log uploads (empty to skip) |
| `HOST_TAG` | hostname | Machine tag inserted into upload path (`<RCLONE_REMOTE>/<HOST_TAG>/...`) |

## Execution Flow

For each database (in order: duckdb, postgres, clickhouse):

```
1. Stop other databases
   - PostgreSQL: pg_ctl stop (tries SSD, HDD, and scripts/ data dirs)
   - ClickHouse: kill via PID file + pkill

2. Check SSD space
   - Warns if < 50GiB available, continues anyway

3. Prepare data on SSD
   - If data already on SSD:  use it directly
   - If data exists on HDD:   mv from HDD to SSD
   - If data doesn't exist:   create it on SSD via load-* binary

4. Clear system caches
   - Runs /usr/local/sbin/clearcache3.sh (drops OS page cache)

5. Run benchmark sweep
   - For each thread count: run BENCHMARK_RUNS times
   - 30-second cooldown between runs
   - System caches cleared after each run
   - Database-specific caches cleared (DISCARD ALL / SYSTEM DROP CACHE)

6. Upload logs
   - rclone copy to Google Drive (skipped if rclone not found)

7. Move data from SSD to HDD
   - Stop server first (postgres/clickhouse)
   - mv data directory back to HDD
```

## Database-Specific Details

### DuckDB

- **Data**: Single file (`$SSD_BASE/duckdb_bench_<dataset>.db`)
- **Temp**: `$SSD_BASE/duckdb_temp_<dataset>/` (cleaned up after sweep)
- **Sort binary**: `sort-duckdb --memory-limit 10GiB --threads N`
- **Delegation**: Calls `sweep_duckdb_parallelism.sh` directly

### PostgreSQL

- **Data**: Directory (`$SSD_BASE/postgres-data_<dataset>/`)
- **Server**: Started with `max_worker_processes=128`, `max_parallel_workers=128`
- **Sort binary**: `sort-postgres --total-memory 10GiB --parallel-workers N`
- **Delegation**: Runs benchmark loop **inline** (not via `sweep_postgres_parallelism.sh`) to support plan deduplication (see below)

### ClickHouse

- **Data**: Directory (`$SSD_BASE/clickhouse-data_<dataset>/`)
- **Server**: Started with `--logger.level information` (needed for `PEAK_MEMORY` extraction)
- **Sort binary**: `sort-clickhouse --memory-limit 10GiB --threads N`
- **Delegation**: Calls `sweep_clickhouse_parallelism.sh` directly

### Memory Enforcement

The `MEMORY_LIMIT` parameter is interpreted differently by each database:

| Database | Setting | Enforcement |
|----------|---------|-------------|
| DuckDB | `SET memory_limit` | Hard cap on total query memory. DuckDB spills to disk or errors when exceeded. |
| PostgreSQL | `SET work_mem = total_memory / (workers + 1 leader)` | Per-operator limit per backend and per sort/hash node. Controls when each sort/hash spills to disk, but does not cap total query memory. |
| ClickHouse | `max_bytes_before_external_sort` | Controls when sorting spills to disk, but `max_memory_usage` is **not** set, so total query memory is unbounded. |

As a result, ClickHouse may use significantly more memory than the configured `MEMORY_LIMIT` for non-sort operations (e.g., reading, decompression, buffering), while DuckDB is the most strictly bounded.

## PostgreSQL Plan Skipping

### The Problem

PostgreSQL's `sort-postgres` binary configures the query planner with:

```
work_mem = total_memory / (parallel_workers + 1)
max_parallel_workers_per_gather = parallel_workers
```

With a fixed total memory budget (e.g., 10GiB), changing the worker count changes `work_mem`:

| Workers | Total processes (workers + leader) | work_mem per backend |
|---------|-------------------------------------|----------------------|
| 4 | 5 | 2.0 GiB |
| 8 | 9 | ~1.11 GiB |
| 16 | 17 | ~602 MiB |
| 24 | 25 | ~410 MiB |
| 32 | 33 | ~310 MiB |
| 40 | 41 | ~250 MiB |
| 44 | 45 | ~228 MiB |

PostgreSQL's query planner may choose different execution strategies based on `work_mem` (e.g., in-memory quicksort vs. external merge sort). However, for some ranges of worker counts, the plan may be **identical** -- the optimizer makes the same choice despite the different `work_mem` values.

Running a full benchmark when the plan is identical is wasteful because:
- The same execution strategy is used
- The only difference is how many workers share the same work
- Each benchmark run takes significant time (minutes to hours with large data)

### How It Works

Before running any benchmark for a given thread count, the script:

1. **Opens a read-only transaction** to PostgreSQL
2. **Applies the same session settings** that `sort-postgres` would use:
   ```sql
   SET LOCAL work_mem = '<calculated>kB';
   SET LOCAL max_parallel_workers_per_gather = <N>;
   SET LOCAL parallel_tuple_cost = 0;
   SET LOCAL parallel_setup_cost = 0;
   SET LOCAL min_parallel_table_scan_size = '0';
   SET LOCAL enable_parallel_append = on;
   SET LOCAL temp_file_limit = -1;
   ```
3. **Runs `EXPLAIN`** (without `ANALYZE`) to get the planned execution strategy without actually executing the sort
4. **Compares the plan text** with the previous thread count's plan
5. **If identical**: skips all benchmark runs for this thread count and logs a `*_SKIPPED.log` file
6. **If different**: proceeds with the full benchmark runs
7. **Rolls back** the transaction (no side effects)

### Example Output

When plan deduplication triggers:
```
[postgres] Checking plan for 8 threads (7 workers + 1 leader)...
[postgres] SKIPPING 8 threads: plan identical to previous thread count.
[postgres] Plan dedup saved 3 benchmark run(s).
```

The skip is recorded in `logs/postgres_parallelism_sweep_*/10GiB_8threads_7workers_SKIPPED.log` with the full plan text for reference.

### When Plans Differ

Plans typically differ when `work_mem` crosses a threshold that changes the sort strategy. For example, the planner might switch from:
- **In-memory sort** (when `work_mem` is large enough to hold all data) to
- **External merge sort** (when `work_mem` is too small, requiring disk spills)

The exact threshold depends on data size and PostgreSQL's cost estimates.

## Log Structure

Logs are saved to `./logs/` with timestamped directories:

```
logs/
  duckdb_parallelism_sweep_20260214_110542/
    100MiB_4threads_run1_20260214_110542.log
    100MiB_8threads_run1_20260214_110612.log
  postgres_parallelism_sweep_20260214_110541/
    10GiB_4threads_3workers_run1_20260214_110655.log
    10GiB_8threads_7workers_SKIPPED.log              # plan dedup
    10GiB_16threads_15workers_run1_20260214_111030.log
  clickhouse_parallelism_sweep_20260214_110743/
    10GiB_4threads_run1_20260214_110743.log
    10GiB_8threads_run1_20260214_110813.log
```

Each log file contains:
- Configuration details (memory, threads, run number)
- Full command output (including query plans and timing)
- Exit code and status (SUCCESS / TIMEOUT / FAILED)
- Summary with extracted `Result: <memory>,<threads>,<run>,<duration>`

## Cache Clearing

Caches are cleared at multiple levels to ensure consistent measurements:

| When | What |
|------|------|
| Before first run of each database | System caches (`clearcache3.sh`) |
| After each benchmark run | System caches (`clearcache3.sh`) |
| After each PostgreSQL run | `DISCARD ALL` (clears PG session caches) |
| After each ClickHouse run | `SYSTEM DROP MARK CACHE`, `SYSTEM DROP UNCOMPRESSED CACHE`, `SYSTEM DROP COMPILED EXPRESSION CACHE` |

## Multi-Dataset Runs

`run_both_datasets.sh` runs the full orchestrator twice:

1. **gensort** (`gensort_200GiB.data`, format `gensort`) -- logs uploaded to `gdrive:bench_results/gensort/<host_tag>/`
2. **lineitem** (`lineitem_sf500.k-8-9-13-14-15.v-0-3.kvbin`, format `kvbin`) -- logs uploaded to `gdrive:bench_results/lineitem/<host_tag>/`

Between datasets, all database data is moved from SSD to HDD, so the SSD starts fresh for the next dataset's load.
