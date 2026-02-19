use clap::Parser;
use postgres::{Client, NoTls};
use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "sort-postgres")]
struct Args {
    #[arg(long)]
    db: String,

    #[arg(long, default_value = "bench_data")]
    table: String,

    /// TOTAL memory budget for the entire sort (e.g., "2GB", "4GB")
    #[arg(long, default_value = "2GB")]
    total_memory: String,

    /// Number of parallel workers (Total processes = workers + 1)
    #[arg(long, default_value = "7")]
    parallel_workers: i32,

    /// Output path for sorted data (binary format). If not provided, runs count mode instead.
    #[arg(long)]
    output: Option<String>,
}

/// Parses strings like "2GB", "512MB" into a numeric KiB value
fn parse_memory_to_kb(mem_str: &str) -> Result<i64, Box<dyn Error>> {
    let s = mem_str.to_uppercase();
    if s.ends_with("GIB") || s.ends_with("GB") || s.ends_with('G') {
        let val: f64 = s
            .trim_end_matches("GIB")
            .trim_end_matches("GB")
            .trim_end_matches('G')
            .parse()?;
        Ok((val * 1024.0 * 1024.0) as i64)
    } else if s.ends_with("MIB") || s.ends_with("MB") || s.ends_with('M') {
        let val: f64 = s
            .trim_end_matches("MIB")
            .trim_end_matches("MB")
            .trim_end_matches('M')
            .parse()?;
        Ok((val * 1024.0) as i64)
    } else if s.ends_with("KIB") || s.ends_with("KB") || s.ends_with('K') {
        let val: f64 = s
            .trim_end_matches("KIB")
            .trim_end_matches("KB")
            .trim_end_matches('K')
            .parse()?;
        Ok(val as i64)
    } else {
        Err("Unsupported memory format. Use GB or MB (e.g., '2GB')".into())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    // 1. CALCULATE WORK_MEM PER BACKEND PROCESS
    // PostgreSQL parallel query uses N workers + 1 leader process.
    // To keep a fixed total-process budget, divide by (workers + leader).
    if args.parallel_workers < 0 {
        return Err("parallel-workers must be >= 0".into());
    }
    let total_procs = args.parallel_workers + 1;
    if total_procs <= 0 {
        return Err("Invalid process count derived from parallel-workers".into());
    }
    let total_kb = parse_memory_to_kb(&args.total_memory)?;
    let work_mem_kb = total_kb / total_procs as i64;
    let work_mem_setting = format!("{}kB", work_mem_kb);

    let mut client = Client::connect(&args.db, NoTls)?;

    client.batch_execute("BEGIN")?;
    client.batch_execute("SET LOCAL transaction_read_only = on")?;

    // 2. APPLY CALCULATED SETTINGS
    println!(
        "Total Budget: {} | Workers: {} | Total Processes: {} (workers + 1 leader)",
        args.total_memory, args.parallel_workers, total_procs
    );
    println!(
        "Calculated work_mem per backend process (worker/leader): {}",
        work_mem_setting
    );
    println!(
        "Note: work_mem is per backend per sort/hash node; total memory can exceed this estimate."
    );

    client.batch_execute(&format!("SET LOCAL work_mem = '{}'", work_mem_setting))?;
    client.batch_execute(&format!(
        "SET LOCAL max_parallel_workers_per_gather = {}",
        args.parallel_workers
    ))?;

    // Nudge Optimizer to ensure it actually uses the workers
    client.batch_execute("SET LOCAL parallel_tuple_cost = 0")?;
    client.batch_execute("SET LOCAL parallel_setup_cost = 0")?;
    client.batch_execute("SET LOCAL min_parallel_table_scan_size = '0'")?;
    client.batch_execute("SET LOCAL enable_parallel_append = on")?;
    client.batch_execute("SET LOCAL temp_file_limit = -1")?;

    // --- Gather and print table statistics ---
    println!("\nGathering table statistics...");

    let row_count: i64 = client
        .query_one(&format!("SELECT COUNT(*) FROM {}", args.table), &[])?
        .get(0);

    let table_size: i64 = client
        .query_one(
            &format!("SELECT pg_total_relation_size('{}')", args.table),
            &[],
        )?
        .get(0);

    let size_gib = table_size as f64 / (1024.0 * 1024.0 * 1024.0);

    println!("Table: {}", args.table);
    println!("Row count: {}", row_count);
    println!("Size: {:.2} GiB", size_gib);
    println!();

    // Build the actual query based on mode
    if let Some(ref output_path) = args.output {
        // Binary output mode: Write sorted results to file
        // Convert to absolute path (PostgreSQL requires absolute paths for COPY TO FILE)
        let absolute_path = if PathBuf::from(output_path).is_absolute() {
            output_path.to_string()
        } else {
            std::env::current_dir()?
                .join(output_path)
                .to_str()
                .ok_or("Invalid path")?
                .to_string()
        };

        let select_query = format!(
            "SELECT sort_key, payload FROM {} ORDER BY sort_key",
            args.table
        );
        let query = format!(
            "COPY ({}) TO '{}' (FORMAT BINARY)",
            select_query, absolute_path
        );

        // --- Run EXPLAIN on the SELECT query (COPY cannot be EXPLAINed) ---
        println!("\nRunning EXPLAIN on the SELECT query...");
        let explain_query = format!("EXPLAIN (BUFFERS, VERBOSE) {}", select_query);

        let explain_rows = client.query(&explain_query, &[])?;

        println!("\n===== QUERY PLAN =====");
        for row in explain_rows {
            let line: String = row.get(0);
            println!("{}", line);
        }
        println!("======================\n");

        // --- Final Execution ---
        println!(
            "\nRunning external sort (writing to '{}')...",
            absolute_path
        );
        let start = Instant::now();

        client.batch_execute(&query)?;
        client.batch_execute("COMMIT")?;
        let duration = start.elapsed();

        println!(
            "\nExternal sorting completed and written to binary file in {:.2} seconds.",
            duration.as_secs_f64()
        );
        println!("TIMING: {:.2} seconds", duration.as_secs_f64());
    } else {
        // Analyze mode: Run EXPLAIN ANALYZE to execute sort without writing
        let query = format!(
            "SELECT sort_key, payload FROM {} ORDER BY sort_key",
            args.table
        );
        let explain_analyze_query = format!("EXPLAIN ANALYZE {}", query);

        println!("\nRunning EXPLAIN ANALYZE (sort without writing)...");
        let start = Instant::now();

        let explain_rows = client.query(&explain_analyze_query, &[])?;
        client.batch_execute("COMMIT")?;
        let duration = start.elapsed();

        println!("\n===== EXPLAIN ANALYZE RESULTS =====");
        for row in explain_rows {
            let line: String = row.get(0);
            println!("{}", line);
        }
        println!("====================================\n");

        println!(
            "\nExternal sorting completed in {:.2} seconds.",
            duration.as_secs_f64()
        );
        println!("TIMING: {:.2} seconds", duration.as_secs_f64());
    }

    Ok(())
}
