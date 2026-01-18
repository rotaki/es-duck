# Benchmark Scripts

This directory contains scripts for benchmarking external sort implementations in DuckDB and PostgreSQL.

## Scripts

### DuckDB Scripts

#### `run_duckdb_load_and_sort.sh`
Orchestrates the full workflow: load data → flush → sort

```bash
./scripts/run_duckdb_load_and_sort.sh \
    --format gensort \
    --input testdata/test_gensort.dat \
    --db /tmp/bench.db \
    --output /tmp/sorted_output \
    --memory-limit 2GB \
    --threads 16
```

**Options:**
- `--format <FORMAT>` - Input format: `gensort` or `kvbin`
- `--input <INPUT>` - Input data file path
- `--db <DB>` - DuckDB database file path
- `--output <OUTPUT>` - Output directory for sorted Parquet files
- `--table <TABLE>` - Table name (default: bench_data)
- `--memory-limit <MEMORY>` - Memory limit (default: 1GB)
- `--temp-dir <DIR>` - Temporary directory for spilling
- `--threads <N>` - Number of threads
- `--skip-load` - Skip load step (DB already exists)

#### `run_duckdb_bench.sh`
Combined benchmark with timing and cache flushing (using bench_duckdb_sort.rs)

```bash
./scripts/run_duckdb_bench.sh
```

Environment variables: `INPUT_FILE`, `FORMAT`, `DB_FILE`, `OUTPUT_DIR`, `MEMORY_LIMIT`, `TEMP_DIR`

### PostgreSQL Scripts

#### `run_postgres_load_and_sort.sh`
Orchestrates the full workflow: load data → flush → sort

```bash
./scripts/run_postgres_load_and_sort.sh \
    --format gensort \
    --input testdata/test_gensort.dat \
    --db "postgres://localhost/bench" \
    --output /tmp/sorted_output.bin \
    --total-memory 2GB \
    --parallel-workers 16
```

**Options:**
- `--format <FORMAT>` - Input format: `gensort` or `kvbin`
- `--input <INPUT>` - Input data file path
- `--db <DB_CONNECTION>` - PostgreSQL connection string
- `--output <OUTPUT>` - Output file for sorted binary COPY data
- `--table <TABLE>` - Table name (default: bench_data)
- `--total-memory <MEMORY>` - work_mem setting (default: 64MB)
- `--temp-tablespace <TS>` - Temporary tablespace
- `--parallel-workers <N>` - Number of parallel workers
- `--skip-load` - Skip load step (table already exists)

#### `run_postgres_bench.sh`
Combined benchmark with timing and cache flushing (using bench_postgres_sort.rs)

```bash
./scripts/run_postgres_bench.sh
```

Environment variables: `INPUT_FILE`, `FORMAT`, `DB_CONNECTION`, `TABLE`, `OUTPUT_FILE`, `WORK_MEM`, `TEMP_TABLESPACE`

## Parameter Sweep Scripts

### DuckDB Sweeps

#### `sweep_duckdb_parallelism.sh`
Vary thread count (4, 8, 16, 24, 32, 40, 44) at fixed memory

```bash
./scripts/sweep_duckdb_parallelism.sh
```

Environment variables:
- `INPUT_FILE` - Input data file (default: testdata/test_gensort.dat)
- `FORMAT` - Input format (default: gensort)
- `DB_FILE` - Database file (default: /tmp/duckdb_bench.db)
- `OUTPUT_BASE` - Output directory base (default: /tmp/duckdb_sorted)
- `MEMORY_LIMIT` - Fixed memory limit (default: 2GB)
- `TEMP_DIR` - Temp directory (default: /tmp/duckdb_temp)
- `THREAD_COUNTS` - Thread counts to test (default: "4 8 16 24 32 40 44")

#### `sweep_duckdb_memory.sh`
Vary memory limit (1GB, 4GB, 6GB, 8GB, 16GB, 24GB, 32GB) at fixed thread count

```bash
./scripts/sweep_duckdb_memory.sh
```

Environment variables:
- `THREADS` - Fixed thread count (default: 16)
- `MEMORY_LIMITS` - Memory limits to test (default: "1GB 4GB 6GB 8GB 16GB 24GB 32GB")
- Other variables same as parallelism sweep

### PostgreSQL Sweeps

#### `sweep_postgres_parallelism.sh`
Vary parallel worker count (4, 8, 16, 24, 32, 40, 44) at fixed work_mem

```bash
./scripts/sweep_postgres_parallelism.sh
```

Environment variables:
- `INPUT_FILE` - Input data file (default: testdata/test_gensort.dat)
- `FORMAT` - Input format (default: gensort)
- `DB_CONNECTION` - PostgreSQL connection string (default: postgres://localhost/bench)
- `TABLE` - Table name (default: bench_data)
- `OUTPUT_BASE` - Output file base (default: /tmp/postgres_sorted)
- `WORK_MEM` - Fixed work_mem (default: 2GB)
- `WORKER_COUNTS` - Worker counts to test (default: "4 8 16 24 32 40 44")

#### `sweep_postgres_memory.sh`
Vary work_mem (1GB, 4GB, 6GB, 8GB, 16GB, 24GB, 32GB) at fixed parallel worker count

```bash
./scripts/sweep_postgres_memory.sh
```

Environment variables:
- `PARALLEL_WORKERS` - Fixed parallel worker count (default: 16)
- `MEMORY_LIMITS` - Memory limits to test (default: "1GB 4GB 6GB 8GB 16GB 24GB 32GB")
- Other variables same as parallelism sweep

### Custom Sweep Example

```bash
# DuckDB parallelism sweep with custom dataset
INPUT_FILE=data/large.dat \
DB_FILE=/ssd/bench.db \
OUTPUT_BASE=/ssd/results/duckdb \
MEMORY_LIMIT=4GB \
THREAD_COUNTS="8 16 32 64" \
./scripts/sweep_duckdb_parallelism.sh

# PostgreSQL memory sweep with custom parameters
DB_CONNECTION="postgres://user:pass@host/bench" \
OUTPUT_BASE=/results/postgres \
PARALLEL_WORKERS=32 \
MEMORY_LIMITS="512MB 1GB 2GB 4GB" \
./scripts/sweep_postgres_memory.sh
```

## Notes

- The `--skip-load` flag is useful for running multiple sort benchmarks on the same dataset
- All scripts use `sync` to flush filesystem buffers (sudo not required)
- DuckDB exports to Parquet format, PostgreSQL exports to binary COPY format
- Release builds are used for optimal performance
