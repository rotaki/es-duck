use clap::{Parser, ValueEnum};
use duckdb::{Appender, Connection, params};
use std::error::Error;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::PathBuf;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum InputFormat {
    Gensort,
    Kvbin,
}

#[derive(Parser)]
#[command(name = "es-duck-duckdb")]
struct Args {
    #[arg(long, value_enum)]
    format: InputFormat,

    #[arg(long)]
    input: PathBuf,

    #[arg(long)]
    db: PathBuf,

    #[arg(long, default_value = "bench_data")]
    table: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    // Check if destination file already exists
    if args.db.exists() {
        eprintln!("Error: Destination file {:?} already exists.", args.db);
        std::process::exit(1);
    }

    // 1. Initialize DuckDB connection
    let conn = Connection::open(&args.db)?;

    // 2. Prepare the schema
    conn.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS {} (sort_key BLOB, payload BLOB);",
            args.table
        ),
        [],
    )?;

    // 3. Initialize the Appender
    let mut appender = conn.appender(&args.table)?;

    println!("Starting load from {:?}...", args.input);

    let rows = match args.format {
        InputFormat::Gensort => load_gensort(&args.input, &mut appender)?,
        InputFormat::Kvbin => load_kvbin(&args.input, &mut appender)?,
    };

    println!("Successfully appended {} rows to DuckDB.", rows);
    Ok(())
}

fn load_gensort(input: &PathBuf, appender: &mut Appender) -> Result<u64, Box<dyn Error>> {
    const KEY_SIZE: usize = 10;
    const PAYLOAD_SIZE: usize = 90;
    const RECORD_SIZE: usize = KEY_SIZE + PAYLOAD_SIZE;

    let file = File::open(input)?;
    let file_size = file.metadata()?.len();
    let num_records = file_size / RECORD_SIZE as u64;

    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut buf = vec![0u8; RECORD_SIZE];

    for _ in 0..num_records {
        reader.read_exact(&mut buf)?;

        // Store as raw bytes in BLOB columns
        let key = &buf[..KEY_SIZE];
        let payload = &buf[KEY_SIZE..];

        appender.append_row(params![key, payload])?;
    }

    Ok(num_records)
}

fn load_kvbin(input: &PathBuf, appender: &mut Appender) -> Result<u64, Box<dyn Error>> {
    let file = File::open(input)?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut rows = 0;

    loop {
        let mut len_buf = [0u8; 4];
        if let Err(e) = reader.read_exact(&mut len_buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(e.into());
        }
        let klen = u32::from_le_bytes(len_buf) as usize;
        let mut key_bytes = vec![0u8; klen];
        reader.read_exact(&mut key_bytes)?;

        reader.read_exact(&mut len_buf)?;
        let vlen = u32::from_le_bytes(len_buf) as usize;
        let mut val_bytes = vec![0u8; vlen];
        reader.read_exact(&mut val_bytes)?;

        // Append as BLOB
        appender.append_row(params![key_bytes.as_slice(), val_bytes.as_slice()])?;
        rows += 1;
    }

    Ok(rows)
}
