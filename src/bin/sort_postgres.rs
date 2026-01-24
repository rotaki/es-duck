use clap::Parser;
use postgres::{Client, NoTls};
use std::error::Error;
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

/// Parses strings like "2GB", "512MB" into a numeric byte value
fn parse_memory_to_kb(mem_str: &str) -> Result<i64, Box<dyn Error>> {
    let s = mem_str.to_uppercase();
    if s.ends_with("GB") || s.ends_with("G") {
        let val: f64 = s.trim_end_matches('G').trim_end_matches("GB").parse()?;
        Ok((val * 1024.0 * 1024.0) as i64)
    } else if s.ends_with("MB") || s.ends_with("M") {
        let val: f64 = s.trim_end_matches('M').trim_end_matches("MB").parse()?;
        Ok((val * 1024.0) as i64)
    } else {
        Err("Unsupported memory format. Use GB or MB (e.g., '2GB')".into())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    // 1. CALCULATE WORK_MEM PER WORKER
    // NOTE: PostgreSQL parallel query uses N workers + 1 leader process
    // For example, --parallel-workers=40 creates 41 total processes (40 workers + 1 leader)
    // We divide total memory budget by N (the parallel_workers parameter) to get work_mem
    let total_procs = args.parallel_workers + 1;
    let total_kb = parse_memory_to_kb(&args.total_memory)?;
    let work_mem_kb = total_kb / args.parallel_workers as i64;
    let work_mem_setting = format!("{}kB", work_mem_kb);

    let mut client = Client::connect(&args.db, NoTls)?;

    client.batch_execute("BEGIN")?;
    client.batch_execute("SET LOCAL transaction_read_only = on")?;

    // 2. APPLY CALCULATED SETTINGS
    println!(
        "Total Budget: {} | Workers: {} | Total Processes: {} (workers + 1 leader)",
        args.total_memory, args.parallel_workers, total_procs
    );
    println!("Calculated work_mem per worker: {}", work_mem_setting);

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

    let size_gb = table_size as f64 / (1024.0 * 1024.0 * 1024.0);

    println!("Table: {}", args.table);
    println!("Row count: {}", row_count);
    println!("Size: {:.2} GB", size_gb);
    println!();

    // Build the actual query based on mode
    if let Some(ref output_path) = args.output {
        // Binary output mode: Write sorted results to file
        let select_query = format!(
            "SELECT sort_key, payload FROM {} ORDER BY sort_key",
            args.table
        );
        let query = format!(
            "COPY ({}) TO '{}' (FORMAT BINARY)",
            select_query, output_path
        );

        // --- Run EXPLAIN on the actual query ---
        println!("\nRunning EXPLAIN on the actual query...");
        let explain_query = format!("EXPLAIN (BUFFERS, VERBOSE) {}", query);

        let explain_rows = client.query(&explain_query, &[])?;

        println!("\n===== QUERY PLAN =====");
        for row in explain_rows {
            let line: String = row.get(0);
            println!("{}", line);
        }
        println!("======================\n");

        // --- Final Execution ---
        println!("\nRunning external sort (writing to '{}')...", output_path);
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
