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

    #[arg(long)]
    temp_tablespace: Option<String>,

    /// Number of parallel workers (Total processes = workers + 1)
    #[arg(long, default_value = "7")]
    parallel_workers: i32,
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
    // Total processes = Workers + 1 (the Leader)
    let total_procs = args.parallel_workers + 1;
    let total_kb = parse_memory_to_kb(&args.total_memory)?;
    let work_mem_kb = total_kb / total_procs as i64;
    let work_mem_setting = format!("{}kB", work_mem_kb);

    let mut client = Client::connect(&args.db, NoTls)?;

    client.batch_execute("BEGIN")?;
    client.batch_execute("SET LOCAL transaction_read_only = on")?;

    // 2. APPLY CALCULATED SETTINGS
    println!(
        "Total Budget: {} | Workers: {} | Total Processes: {}",
        args.total_memory, args.parallel_workers, total_procs
    );
    println!("Calculated work_mem per process: {}", work_mem_setting);

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

    if let Some(ref ts) = args.temp_tablespace {
        client.batch_execute(&format!("SET LOCAL temp_tablespaces = '{}'", ts))?;
    }

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

    // --- Verification via EXPLAIN ANALYZE ---
    let select_query = format!(
        "SELECT sort_key, payload FROM {} ORDER BY sort_key",
        args.table
    );
    println!("Verifying external sort with EXPLAIN ANALYZE...");

    let analyze_rows =
        client.query(&format!("EXPLAIN (ANALYZE, BUFFERS) {}", select_query), &[])?;

    let mut found_external = false;
    for row in analyze_rows {
        let line: String = row.get(0);
        if line.contains("Sort Method:") {
            println!(">>> {}", line.trim());
            if line.contains("external merge") {
                found_external = true;
            }
        }
    }

    if !found_external {
        return Err("Sort did not spill to disk! Check your data size or memory budget.".into());
    }

    // --- Final Execution ---
    let count_query = format!(
        "SELECT count(payload) FROM (SELECT payload FROM {} ORDER BY sort_key OFFSET 1) AS subquery",
        args.table
    );

    println!("\nExecuting count query...");
    let start = Instant::now();
    let count: i64 = client.query_one(&count_query, &[])?.get(0);
    client.batch_execute("COMMIT")?;
    let duration = start.elapsed();

    println!("Count result: {}", count);
    println!("TIMING: {:.2} seconds", duration.as_secs_f64());

    Ok(())
}
