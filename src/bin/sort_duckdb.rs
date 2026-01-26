use clap::Parser;
use duckdb::Connection;
use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "sort-duckdb")]
#[command(about = "Run external sorting on a DuckDB database")]
struct Args {
    /// Path to the DuckDB database file
    #[arg(long)]
    db: PathBuf,

    /// Table name to sort
    #[arg(long, default_value = "bench_data")]
    table: String,

    /// Temporary directory for DuckDB spilling (should be on fast SSD)
    #[arg(long)]
    temp_dir: Option<PathBuf>,

    /// Memory limit for DuckDB (e.g., "1GB", "512MB")
    #[arg(long, default_value = "1GB")]
    memory_limit: String,

    /// Number of threads for DuckDB to use
    #[arg(long)]
    threads: Option<usize>,

    /// Output path for sorted data (parquet format). If not provided, runs analyze mode instead.
    #[arg(long)]
    output: Option<PathBuf>,
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
    // Quote table name as an identifier: "foo""bar"
    let table = format!("\"{}\"", args.table.replace('"', "\"\""));
    let select_query = format!("SELECT sort_key, payload FROM {} ORDER BY sort_key", table);

    // Build the actual query that will be executed based on mode
    let (query, mode_description) = if let Some(output_path) = &args.output {
        let path = output_path.display().to_string().replace('\'', "''");
        let copy_query = format!(
            "COPY ({}) TO '{}' (FORMAT PARQUET, PRESERVE_ORDER true)",
            select_query, path
        );
        (
            copy_query,
            format!("writing to '{}'", output_path.display()),
        )
    } else {
        let analyze_query = format!("EXPLAIN ANALYZE {}", select_query);
        (analyze_query, format!("analyze mode on '{}'", args.table))
    };

    // Always print the sort-only plan (useful for both modes)
    {
        let explain_sort = format!("EXPLAIN {}", select_query);
        let mut stmt = conn.prepare(&explain_sort)?;
        let mut rows = stmt.query([])?;

        println!("\n===== SORT-ONLY EXPLAIN PLAN =====");
        while let Some(row) = rows.next()? {
            // DuckDB EXPLAIN commonly returns (explain_key, explain_value)
            // If it’s 1-col in your build, change get(1) -> get(0).
            let result: String = row.get(1)?;
            println!("{}", result);
        }
        println!("=================================\n");
    }

    // Execute the query
    println!("Running external sort ({})...", mode_description);

    let start = Instant::now();

    if args.output.is_some() {
        // Parquet mode: execute the COPY statement
        conn.execute(&query, [])?;
    } else {
        // Analyze mode: execute EXPLAIN ANALYZE and collect results (don’t print during timing)
        let mut stmt = conn.prepare(&query)?;
        let mut rows = stmt.query([])?;

        let mut explain_lines: Vec<String> = Vec::new();
        while let Some(row) = rows.next()? {
            let line: String = row.get(1)?; // if 1-col output, use get(0)
            explain_lines.push(line);
        }

        let duration = start.elapsed();
        println!("TIMING: {:.2}", duration.as_secs_f64());

        // Print after timing to avoid stdout overhead in the measurement
        println!("\n===== EXPLAIN ANALYZE RESULTS =====");
        for line in explain_lines {
            println!("{}", line);
        }
        println!("====================================\n");

        // early return so we don’t print TIMING twice
        return Ok(());
    }

    let duration = start.elapsed();
    println!("TIMING: {:.2}", duration.as_secs_f64());
    Ok(())
}
