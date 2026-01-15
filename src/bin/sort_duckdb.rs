use clap::Parser;
use duckdb::Connection;
use std::error::Error;
use std::path::PathBuf;

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

    // Set temp directory if provided
    if let Some(ref temp_dir) = args.temp_dir {
        println!("Setting temp_directory to {:?}", temp_dir);
        conn.execute(&format!("SET temp_directory = '{}';", temp_dir.display()), [])?;
    }

    // Set memory limit
    println!("Setting memory_limit to {}", args.memory_limit);
    conn.execute(&format!("SET memory_limit = '{}';", args.memory_limit), [])?;

    // Build the COPY query for external sorting
    let output_path = args.output.display();
    let query = format!(
        r#"COPY (
            SELECT sort_key, payload
            FROM {}
            ORDER BY sort_key
        ) TO '{}'
        (FORMAT PARQUET, PRESERVE_ORDER true);"#,
        args.table, output_path
    );

    println!("Running external sort on table '{}'...", args.table);
    println!("Output: {:?}", args.output);

    conn.execute(&query, [])?;

    println!("External sorting completed successfully.");
    Ok(())
}
