use clap::Parser;
use duckdb::Connection;
use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "sort-duckdb")]
#[command(about = "Run external sorting on a DuckDB database and export to Parquet")]
struct Args {
    /// Path to the DuckDB database file
    #[arg(long)]
    db: PathBuf,

    /// Table name to sort
    #[arg(long, default_value = "bench_data")]
    table: String,

    /// Output directory for sorted Parquet files
    #[arg(long)]
    output: PathBuf,

    /// Temporary directory for DuckDB spilling (should be on fast SSD)
    #[arg(long)]
    temp_dir: Option<PathBuf>,

    /// Memory limit for DuckDB (e.g., "1GB", "512MB")
    #[arg(long, default_value = "1GB")]
    memory_limit: String,

    /// Number of threads for DuckDB to use
    #[arg(long)]
    threads: Option<usize>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    // Check if database exists
    if !args.db.exists() {
        eprintln!("Error: Database file {:?} does not exist.", args.db);
        std::process::exit(1);
    }

    // Open the database
    let conn = Connection::open(&args.db)?;

    // Set threads if provided
    if let Some(threads) = args.threads {
        println!("Setting threads to {}", threads);
        conn.execute(&format!("SET threads = {};", threads), [])?;
    }

    // Set temp directory if provided
    if let Some(ref temp_dir) = args.temp_dir {
        println!("Setting temp_directory to {:?}", temp_dir);
        conn.execute(
            &format!("SET temp_directory = '{}';", temp_dir.display()),
            [],
        )?;
    }

    // Set memory limit
    println!("Setting memory_limit to {}", args.memory_limit);
    conn.execute(&format!("SET memory_limit = '{}';", args.memory_limit), [])?;

    // Get table statistics
    println!("Gathering table statistics...");
    let row_count: i64 =
        conn.query_row(&format!("SELECT COUNT(*) FROM {}", args.table), [], |row| {
            row.get(0)
        })?;

    // Get database file size directly from filesystem
    let db_metadata = std::fs::metadata(&args.db)?;
    let table_size_bytes = db_metadata.len();

    println!("Table: {}", args.table);
    println!("Row count: {}", row_count);
    println!(
        "Database size: {} bytes ({:.2} GB)",
        table_size_bytes,
        table_size_bytes as f64 / 1_073_741_824.0
    );

    // Build the SELECT query for sorting
    let select_query = format!(
        "SELECT sort_key, payload FROM {} ORDER BY sort_key",
        args.table
    );

    // First, run EXPLAIN to see the query plan (without ANALYZE to avoid actual execution)
    println!("\nRunning EXPLAIN...");
    let explain_query = format!("EXPLAIN {}", select_query);

    // Execute EXPLAIN and get the plan as a string
    let plan_result: String = conn.query_row(&explain_query, [], |row| row.get(1))?;

    println!("\n===== QUERY PLAN =====");
    println!("{}", plan_result);
    println!("======================\n");

    // Build the count query with OFFSET 1
    let query = format!(
        r#"SELECT count(payload)
FROM (SELECT payload
FROM {}
ORDER BY sort_key
OFFSET 1);"#,
        args.table
    );

    println!("Running external sort on table '{}'...", args.table);

    let start = Instant::now();
    let count: i64 = conn.query_row(&query, [], |row| row.get(0))?;
    let duration = start.elapsed();

    println!(
        "\nExternal sorting completed in {:.2} seconds.",
        duration.as_secs_f64()
    );
    println!("Count result: {}", count);
    println!("TIMING: {:.2}", duration.as_secs_f64());
    Ok(())
}

fn get_dir_size(path: &PathBuf) -> Result<u64, Box<dyn Error>> {
    let mut total_size = 0u64;

    if path.is_file() {
        return Ok(std::fs::metadata(path)?.len());
    }

    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_file() {
                total_size += metadata.len();
            } else if metadata.is_dir() {
                total_size += get_dir_size(&entry.path().into())?;
            }
        }
    }

    Ok(total_size)
}
