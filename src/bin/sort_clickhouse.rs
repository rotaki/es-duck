use clap::Parser;
use clickhouse::Client;
use std::error::Error;
use std::path::PathBuf;
use std::process::Command;
use std::time::Instant;
use uuid::Uuid;

/// Parses strings like "1GB", "512MB" into a numeric byte value
fn parse_memory_to_bytes(mem_str: &str) -> Result<u64, Box<dyn Error>> {
    let s = mem_str.to_uppercase();
    if s.ends_with("GIB") || s.ends_with("GB") || s.ends_with('G') {
        let val: f64 = s
            .trim_end_matches("GIB")
            .trim_end_matches("GB")
            .trim_end_matches('G')
            .parse()?;
        Ok((val * 1024.0 * 1024.0 * 1024.0) as u64)
    } else if s.ends_with("MIB") || s.ends_with("MB") || s.ends_with('M') {
        let val: f64 = s
            .trim_end_matches("MIB")
            .trim_end_matches("MB")
            .trim_end_matches('M')
            .parse()?;
        Ok((val * 1024.0 * 1024.0) as u64)
    } else if s.ends_with("KIB") || s.ends_with("KB") || s.ends_with('K') {
        let val: f64 = s
            .trim_end_matches("KIB")
            .trim_end_matches("KB")
            .trim_end_matches('K')
            .parse()?;
        Ok((val * 1024.0) as u64)
    } else {
        Err("Unsupported memory format. Use GB or MB (e.g., '1GB', '512MB')".into())
    }
}

#[derive(Parser)]
#[command(name = "sort-clickhouse")]
#[command(about = "Run external sorting on a ClickHouse table")]
struct Args {
    /// ClickHouse server URL
    #[arg(long, default_value = "http://localhost:8123")]
    url: String,

    /// Database name
    #[arg(long, default_value = "default")]
    database: String,

    /// Table name to sort
    #[arg(long, default_value = "bench_data")]
    table: String,

    /// Memory limit for external sorting (e.g., "1GB", "512MB")
    #[arg(long, default_value = "1GB")]
    memory_limit: String,

    #[arg(long)]
    threads: Option<usize>,

    /// Output path for sorted data (CSV format). If not provided, runs query without output.
    #[arg(long)]
    output: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    // Initialize ClickHouse connection
    let client = Client::default()
        .with_url(&args.url)
        .with_database(&args.database);

    // Get table statistics
    println!("Gathering table statistics...");
    let row_count: u64 = client
        .query(&format!("SELECT COUNT(*) FROM {}", args.table))
        .fetch_one::<u64>()
        .await?;

    // Get approximate table size
    let table_size_bytes: u64 = client
        .query(&format!(
            "SELECT sum(bytes_on_disk) FROM system.parts WHERE database = '{}' AND table = '{}'",
            args.database, args.table
        ))
        .fetch_one::<u64>()
        .await
        .unwrap_or(0);

    println!("Table: {}", args.table);
    println!("Row count: {}", row_count);
    println!(
        "Table size: {} bytes ({:.2} GiB)",
        table_size_bytes,
        table_size_bytes as f64 / 1_073_741_824.0
    );

    // Parse memory limit
    let max_bytes = parse_memory_to_bytes(&args.memory_limit)?;
    println!("Parsed memory limit: {} bytes", max_bytes);

    // Build settings
    let mut settings = Vec::new();
    if let Some(threads) = args.threads {
        println!("Setting max_threads to {}", threads);
        settings.push(format!("max_threads = {}", threads));
    }
    settings.push(format!("max_bytes_before_external_sort = {}", max_bytes));
    settings.push(format!("max_bytes_ratio_before_external_sort = 0"));
    println!(
        "Setting max_bytes_before_external_sort to {} bytes",
        max_bytes
    );
    // settings.push(format!("max_memory_usage = {}", max_bytes));
    // println!("Setting max_memory_usage to {} bytes", max_bytes);

    let settings_clause = if settings.is_empty() {
        String::new()
    } else {
        format!("SETTINGS {}", settings.join(", "))
    };

    // Build the query
    let select_query = format!(
        "SELECT sort_key, payload FROM {} ORDER BY sort_key {}",
        args.table, settings_clause
    );

    // Execute EXPLAIN to show the query plan
    {
        let explain_query = format!("EXPLAIN {}", select_query);
        println!("\n===== QUERY PLAN =====");

        let mut cursor = client.query(&explain_query).fetch::<String>()?;
        while let Some(line) = cursor.next().await? {
            println!("{}", line);
        }
        println!("======================\n");
    }

    // Determine the mode
    let (query, mode_description) = if let Some(output_path) = &args.output {
        // Export mode: write sorted data to file
        let path = output_path.display();
        let query = format!("{} INTO OUTFILE '{}' FORMAT Native", select_query, path,);
        (query, format!("writing to '{}' in Native format", path))
    } else {
        // Query mode: use FORMAT Null to execute without returning data
        let query = format!("{} FORMAT Null", select_query);
        (query, "query mode (no output)".to_string())
    };

    println!("Running external sort ({})...", mode_description);

    // Generate a unique query_id for tracking in system.query_log
    let sort_query_id = Uuid::new_v4().to_string();
    println!("Sort query_id: {}", sort_query_id);

    let start = Instant::now();

    // Execute the query with the query_id for tracking
    let query_result = client
        .query(&query)
        .with_option("query_id", &sort_query_id)
        .execute()
        .await;

    let duration = start.elapsed();
    println!("\nTIMING: {:.2} seconds", duration.as_secs_f64());

    // Print the UUID for reference
    println!("QUERY_ID: {}", sort_query_id);

    // Grep the log file for memory usage
    let grep_output = Command::new("grep")
        .args([&sort_query_id, "scripts/clickhouse.log"])
        .output();

    match grep_output {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            // Look for the MemoryTracker line
            for line in stdout.lines() {
                if line.contains("MemoryTracker") && line.contains("peak memory usage") {
                    // Extract the memory part: "Query peak memory usage: 2.15 MiB."
                    if let Some(mem_start) = line.find("peak memory usage:") {
                        let mem_str = &line[mem_start..];
                        println!("PEAK_MEMORY: {}", mem_str);
                    }
                    break;
                }
            }
        }
        Err(e) => {
            eprintln!("Warning: Could not grep log file: {}", e);
        }
    }

    // Propagate the original query error if it failed
    query_result?;

    Ok(())
}
