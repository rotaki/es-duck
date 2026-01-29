use clap::Parser;
use clickhouse::Client;
use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;

/// Parses strings like "1GB", "512MB" into a numeric byte value
fn parse_memory_to_bytes(mem_str: &str) -> Result<u64, Box<dyn Error>> {
    let s = mem_str.to_uppercase();
    if s.ends_with("GB") || s.ends_with("G") {
        let val: f64 = s.trim_end_matches('G').trim_end_matches("GB").parse()?;
        Ok((val * 1024.0 * 1024.0 * 1024.0) as u64)
    } else if s.ends_with("MB") || s.ends_with("M") {
        let val: f64 = s.trim_end_matches('M').trim_end_matches("MB").parse()?;
        Ok((val * 1024.0 * 1024.0) as u64)
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
        "Table size: {} bytes ({:.2} GB)",
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
    // settings.push(format!("max_memory_usage = {}", max_bytes * 2));
    // println!("Setting max_memory_usage to {} bytes", max_bytes * 2);

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

    let start = Instant::now();

    // Execute the query (both modes use execute() now)
    client.query(&query).execute().await?;

    let duration = start.elapsed();
    println!("\nTIMING: {:.2} seconds", duration.as_secs_f64());

    Ok(())
}
