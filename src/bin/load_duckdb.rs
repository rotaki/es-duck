use clap::{Parser, ValueEnum};
use duckdb::{Appender, Connection, params};
use std::error::Error;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
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
        // Single-threaded path
        let conn = Connection::open(db)?;
        let mut appender = conn.appender(table)?;
        return load_gensort_chunk(input, 0, total_records, &mut appender);
    }

    // Multi-threaded path
    let records_per_thread = (total_records + num_threads as u64 - 1) / num_threads as u64;
    let total_rows = Arc::new(Mutex::new(0u64));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let start_record = thread_id as u64 * records_per_thread;
        let end_record = ((thread_id + 1) as u64 * records_per_thread).min(total_records);

        if start_record >= total_records {
            break;
        }

        let input = input.clone();
        let db = db.clone();
        let table = table.to_string();
        let total_rows = Arc::clone(&total_rows);

        let handle = thread::spawn(move || -> Result<u64, Box<dyn Error + Send + Sync>> {
            let conn = Connection::open(&db)?;
            let mut appender = conn.appender(&table)?;
            let rows = load_gensort_chunk(&input, start_record, end_record, &mut appender)?;

            let mut total = total_rows.lock().unwrap();
            *total += rows;

            Ok(rows)
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(result) => match result {
                Ok(rows) => println!("Thread {} loaded {} rows", i, rows),
                Err(e) => return Err(format!("Thread {} failed: {}", i, e).into()),
            },
            Err(_) => return Err(format!("Thread {} panicked", i).into()),
        }
    }

    let total = *total_rows.lock().unwrap();
    Ok(total)
}

fn load_gensort_chunk(
    input: &PathBuf,
    start_record: u64,
    end_record: u64,
    appender: &mut Appender,
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

        let key = &buf[..KEY_SIZE];
        let payload = &buf[KEY_SIZE..];

        appender.append_row(params![key, payload])?;
    }

    Ok(num_records)
}

fn load_kvbin_parallel(
    input: &PathBuf,
    db: &PathBuf,
    table: &str,
    num_threads: usize,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    // First, read all records into memory
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

    if num_threads == 1 {
        // Single-threaded path
        let conn = Connection::open(db)?;
        let mut appender = conn.appender(table)?;
        for (key, val) in records {
            appender.append_row(params![key.as_slice(), val.as_slice()])?;
        }
        return Ok(total_records as u64);
    }

    // Multi-threaded path: partition records into chunks
    let records = Arc::new(records);
    let records_per_thread = (total_records + num_threads - 1) / num_threads;
    let total_rows = Arc::new(Mutex::new(0u64));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let start_idx = thread_id * records_per_thread;
        let end_idx = ((thread_id + 1) * records_per_thread).min(total_records);

        if start_idx >= total_records {
            break;
        }

        let db = db.clone();
        let table = table.to_string();
        let records = Arc::clone(&records);
        let total_rows = Arc::clone(&total_rows);

        let handle = thread::spawn(move || -> Result<u64, Box<dyn Error + Send + Sync>> {
            let conn = Connection::open(&db)?;
            let mut appender = conn.appender(&table)?;
            let mut rows = 0u64;

            for i in start_idx..end_idx {
                let (key, val) = &records[i];
                appender.append_row(params![key.as_slice(), val.as_slice()])?;
                rows += 1;
            }

            let mut total = total_rows.lock().unwrap();
            *total += rows;

            Ok(rows)
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(result) => match result {
                Ok(rows) => println!("Thread {} loaded {} rows", i, rows),
                Err(e) => return Err(format!("Thread {} failed: {}", i, e).into()),
            },
            Err(_) => return Err(format!("Thread {} panicked", i).into()),
        }
    }

    let total = *total_rows.lock().unwrap();
    Ok(total)
}
