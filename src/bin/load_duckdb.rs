use clap::{Parser, ValueEnum};
use duckdb::{Connection, params};
use std::error::Error;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread;

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

    #[arg(long, default_value_t = 1)]
    threads: usize,
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();

    // Check if destination file already exists
    if args.db.exists() {
        eprintln!("Error: Destination file {:?} already exists.", args.db);
        std::process::exit(1);
    }

    // 1. Initialize DuckDB connection and create table
    let conn = Connection::open(&args.db)?;
    conn.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS {} (sort_key BLOB, payload BLOB);",
            args.table
        ),
        [],
    )?;
    drop(conn);

    println!(
        "Starting load from {:?} with {} threads...",
        args.input, args.threads
    );

    let rows = match args.format {
        InputFormat::Gensort => {
            load_gensort_parallel(&args.input, &args.db, &args.table, args.threads)?
        }
        InputFormat::Kvbin => {
            load_kvbin_parallel(&args.input, &args.db, &args.table, args.threads)?
        }
    };

    println!("Successfully appended {} rows to DuckDB.", rows);
    Ok(())
}

fn load_gensort_parallel(
    input: &PathBuf,
    db: &PathBuf,
    table: &str,
    num_threads: usize,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    const KEY_SIZE: usize = 10;
    const PAYLOAD_SIZE: usize = 90;
    const RECORD_SIZE: usize = KEY_SIZE + PAYLOAD_SIZE;

    let file = File::open(input)?;
    let file_size = file.metadata()?.len();
    let total_records = file_size / RECORD_SIZE as u64;
    drop(file);

    if num_threads == 1 {
        // Single-threaded path: read and append directly
        let conn = Connection::open(db)?;
        let mut appender = conn.appender(table)?;

        let file = File::open(input)?;
        let mut reader = BufReader::with_capacity(1024 * 1024, file);
        let mut buf = vec![0u8; RECORD_SIZE];

        for _ in 0..total_records {
            reader.read_exact(&mut buf)?;
            let key = &buf[..KEY_SIZE];
            let payload = &buf[KEY_SIZE..];
            appender.append_row(params![key, payload])?;
        }

        return Ok(total_records);
    }

    // Channel with bounded buffer (100k records = ~10 MB in flight)
    let (tx, rx) = sync_channel::<(Vec<u8>, Vec<u8>)>(100_000);

    // Multi-threaded path: spawn reader threads
    let records_per_thread = (total_records + num_threads as u64 - 1) / num_threads as u64;
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let start_record = thread_id as u64 * records_per_thread;
        let end_record = ((thread_id + 1) as u64 * records_per_thread).min(total_records);

        if start_record >= total_records {
            break;
        }

        let input = input.clone();
        let tx = tx.clone();

        let handle = thread::spawn(move || -> Result<u64, Box<dyn Error + Send + Sync>> {
            send_gensort_chunk(&input, start_record, end_record, tx)
        });

        handles.push(handle);
    }

    // Drop original sender so channel closes when all threads finish
    drop(tx);

    // Main thread: consume from channel and append to DB
    let conn = Connection::open(db)?;
    let mut appender = conn.appender(table)?;
    let mut total_rows = 0u64;

    for (key, payload) in rx {
        appender.append_row(params![key.as_slice(), payload.as_slice()])?;
        total_rows += 1;
    }

    // Wait for all threads and check for errors
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(result) => match result {
                Ok(rows) => println!("Thread {} read {} rows", i, rows),
                Err(e) => return Err(format!("Thread {} failed: {}", i, e).into()),
            },
            Err(_) => return Err(format!("Thread {} panicked", i).into()),
        }
    }

    Ok(total_rows)
}

fn send_gensort_chunk(
    input: &PathBuf,
    start_record: u64,
    end_record: u64,
    tx: SyncSender<(Vec<u8>, Vec<u8>)>,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    const KEY_SIZE: usize = 10;
    const PAYLOAD_SIZE: usize = 90;
    const RECORD_SIZE: usize = KEY_SIZE + PAYLOAD_SIZE;

    let mut file = File::open(input)?;
    file.seek(SeekFrom::Start(start_record * RECORD_SIZE as u64))?;

    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut buf = vec![0u8; RECORD_SIZE];
    let num_records = end_record - start_record;

    for _ in 0..num_records {
        reader.read_exact(&mut buf)?;

        let key = buf[..KEY_SIZE].to_vec();
        let payload = buf[KEY_SIZE..].to_vec();

        // Send to channel; blocks if channel is full (backpressure)
        tx.send((key, payload))
            .map_err(|_| "Failed to send record to channel")?;
    }

    Ok(num_records)
}

fn load_kvbin_parallel(
    input: &PathBuf,
    db: &PathBuf,
    table: &str,
    _num_threads: usize,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    // Read all records into memory (kvbin is variable-length, can't seek)
    let file = File::open(input)?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut records: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

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

        records.push((key_bytes, val_bytes));
    }

    let total_records = records.len();
    drop(reader);

    // Sequential append with single connection
    println!("Appending {} total rows to DuckDB...", total_records);
    let conn = Connection::open(db)?;
    let mut appender = conn.appender(table)?;
    for (key, val) in &records {
        appender.append_row(params![key.as_slice(), val.as_slice()])?;
    }

    Ok(total_records as u64)
}
