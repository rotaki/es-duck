use clap::Parser;
use postgres::{Client, NoTls};
use std::error::Error;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "sort-postgres")]
#[command(about = "Run external sorting on a PostgreSQL table and export to binary COPY format")]
struct Args {
    /// PostgreSQL connection string, e.g. postgres://user:pass@host/db
    #[arg(long)]
    db: String,

    /// Table name to sort
    #[arg(long, default_value = "bench_data")]
    table: String,

    /// Output file path for sorted binary COPY data
    #[arg(long)]
    output: PathBuf,

    /// work_mem setting for sorting (e.g., "64MB", "1GB")
    #[arg(long, default_value = "64MB")]
    work_mem: String,

    /// Temporary tablespace for sorting (optional, must exist)
    #[arg(long)]
    temp_tablespace: Option<String>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let mut client = Client::connect(&args.db, NoTls)?;

    // Start a read-only transaction for the sort/export
    client.batch_execute("BEGIN")?;
    client.batch_execute("SET LOCAL transaction_read_only = on")?;

    // Set work_mem for external sort
    println!("Setting work_mem to {}", args.work_mem);
    client.batch_execute(&format!("SET LOCAL work_mem = '{}'", args.work_mem))?;

    // Allow unlimited temp file spill (may require privileges)
    client.batch_execute("SET LOCAL temp_file_limit = -1")?;

    // Set temp tablespace if provided
    if let Some(ref ts) = args.temp_tablespace {
        println!("Setting temp_tablespaces to {}", ts);
        client.batch_execute(&format!("SET LOCAL temp_tablespaces = '{}'", ts))?;
    }

    // Build and execute the COPY query for external sorting
    let output_path = args.output.display();
    let query = format!(
        "COPY (SELECT sort_key, payload FROM {} ORDER BY sort_key) TO '{}' WITH (FORMAT binary)",
        args.table, output_path
    );

    println!("Running external sort on table '{}'...", args.table);
    println!("Output: {:?}", args.output);

    client.batch_execute(&query)?;
    client.batch_execute("COMMIT")?;

    println!("External sorting completed successfully.");
    Ok(())
}
