use clap::{Parser, ValueEnum};
use duckdb::{Connection, params};
use std::error::Error;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
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
    const BATCH_SIZE: usize = 50_000; // Process 50k records per batch
    const FLUSH_INTERVAL: usize = 10; // Flush every 10 batches (500k records)

    let file = File::open(input)?;
    let file_size = file.metadata()?.len();
    let total_records = file_size / RECORD_SIZE as u64;
    drop(file);

    if num_threads == 1 {
        // Single-threaded path: read and append directly with batching
        let conn = Connection::open(db)?;
        let mut appender = conn.appender(table)?;

        let file = File::open(input)?;
        let mut reader = BufReader::with_capacity(16 * 1024 * 1024, file);
        let mut buf = vec![0u8; RECORD_SIZE];
        let mut last_million_printed = 0u64;

        for i in 0..total_records {
            reader.read_exact(&mut buf)?;
            let key = &buf[..KEY_SIZE];
            let payload = &buf[KEY_SIZE..];
            appender.append_row(params![key, payload])?;

            if (i + 1) % (BATCH_SIZE as u64 * FLUSH_INTERVAL as u64) == 0 {
                appender.flush()?;
                let current_million = (i + 1) / 1_000_000;
                if current_million > last_million_printed {
                    println!("Loaded {} million records...", current_million);
                    last_million_printed = current_million;
                }
            }
        }

        appender.flush()?;
        return Ok(total_records);
    }

    // Channel with batched records - send Vec of fixed-size byte arrays
    type RecordBatch = Vec<[u8; RECORD_SIZE]>;
    let (tx, rx) = sync_channel::<RecordBatch>(num_threads * 2);

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
            send_gensort_chunk_batched(&input, start_record, end_record, tx, BATCH_SIZE)
        });

        handles.push(handle);
    }

    // Drop original sender so channel closes when all threads finish
    drop(tx);

    // Main thread: consume from channel and append to DB
    let conn = Connection::open(db)?;
    let mut appender = conn.appender(table)?;
    let mut total_rows = 0u64;
    let mut batch_count = 0usize;
    let mut last_million_printed = 0u64;

    for batch in rx {
        for record in &batch {
            let key = &record[..KEY_SIZE];
            let payload = &record[KEY_SIZE..];
            appender.append_row(params![key, payload])?;
        }
        total_rows += batch.len() as u64;
        batch_count += 1;

        if batch_count % FLUSH_INTERVAL == 0 {
            appender.flush()?;
            let current_million = total_rows / 1_000_000;
            if current_million > last_million_printed {
                println!("Loaded {} million records...", current_million);
                last_million_printed = current_million;
            }
        }
    }

    appender.flush()?;

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

fn send_gensort_chunk_batched(
    input: &PathBuf,
    start_record: u64,
    end_record: u64,
    tx: SyncSender<Vec<[u8; 100]>>,
    batch_size: usize,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    const RECORD_SIZE: usize = 100;

    let mut file = File::open(input)?;
    file.seek(SeekFrom::Start(start_record * RECORD_SIZE as u64))?;

    let mut reader = BufReader::with_capacity(16 * 1024 * 1024, file);
    let num_records = end_record - start_record;

    // Allocate batch buffer once and reuse it
    let mut batch = Vec::with_capacity(batch_size);

    for _ in 0..num_records {
        let mut record = [0u8; RECORD_SIZE];
        reader.read_exact(&mut record)?;
        batch.push(record);

        // Send full batches
        if batch.len() >= batch_size {
            tx.send(batch)
                .map_err(|_| "Failed to send batch to channel")?;
            batch = Vec::with_capacity(batch_size);
        }
    }

    // Send remaining records
    if !batch.is_empty() {
        tx.send(batch)
            .map_err(|_| "Failed to send batch to channel")?;
    }

    Ok(num_records)
}

fn load_index(index_file: impl AsRef<Path>, file_size: u64) -> Result<Vec<u64>, String> {
    let mut index_points = vec![0];

    let mut index_file =
        File::open(index_file).map_err(|e| format!("Failed to open index file: {e}"))?;
    let size = index_file
        .metadata()
        .map_err(|e| format!("Failed to get index file metadata: {e}"))?
        .len();

    let mut buf = Vec::with_capacity(size as usize);
    unsafe {
        buf.set_len(size as usize);
    }
    index_file
        .read_exact(&mut buf)
        .map_err(|e| format!("Failed to read index file: {e}"))?;

    index_points.extend(
        buf.chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .filter(|&off| off > 0 && off < file_size),
    );

    index_points.push(file_size);
    index_points.sort_unstable();
    index_points.dedup();
    Ok(index_points)
}

fn send_kvbin_chunk_indexed(
    input: &PathBuf,
    start_offset: u64,
    end_offset: u64,
    tx: SyncSender<(Vec<u8>, Vec<u8>)>,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    let mut file = File::open(input)?;
    file.seek(SeekFrom::Start(start_offset))?;
    let mut reader = BufReader::with_capacity(4 * 1024 * 1024, file);

    let mut rows = 0u64;
    let mut len_buf = [0u8; 4];
    let mut key_buf = Vec::new();
    let mut val_buf = Vec::new();
    let mut current_pos = start_offset;

    // Read records until we reach end_offset
    while current_pos < end_offset {
        // Read key length
        if let Err(e) = reader.read_exact(&mut len_buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(e.into());
        }
        let klen = u32::from_le_bytes(len_buf) as usize;
        key_buf.resize(klen, 0);
        reader.read_exact(&mut key_buf)?;

        // Read value length
        reader.read_exact(&mut len_buf)?;
        let vlen = u32::from_le_bytes(len_buf) as usize;
        val_buf.resize(vlen, 0);
        reader.read_exact(&mut val_buf)?;

        current_pos += 8 + klen as u64 + vlen as u64; // 4 bytes klen + 4 bytes vlen + data

        tx.send((key_buf.clone(), val_buf.clone()))
            .map_err(|_| "Failed to send record to channel")?;
        rows += 1;

        // Stop if we've crossed into the next partition
        if current_pos >= end_offset {
            break;
        }
    }

    Ok(rows)
}

fn load_kvbin_parallel(
    input: &PathBuf,
    db: &PathBuf,
    table: &str,
    num_threads: usize,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    // Check for index file (original filename + .idx)
    let mut index_path = input.as_os_str().to_owned();
    index_path.push(".idx");
    let index_path = PathBuf::from(index_path);
    let file_size = File::open(input)?.metadata()?.len();

    if index_path.exists() && num_threads > 1 {
        // Parallel loading using index
        println!("Loading index from {:?}...", index_path);
        let offsets = load_index(&index_path, file_size)
            .map_err(|e| -> Box<dyn Error + Send + Sync> { e.into() })?;

        println!(
            "Index loaded: {} offset points, using {} threads",
            offsets.len(),
            num_threads
        );

        let (tx, rx) = sync_channel::<(Vec<u8>, Vec<u8>)>(100_000);

        // Divide the file into N partitions based on offsets
        let partitions_per_thread = (offsets.len() + num_threads - 1) / num_threads;
        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let start_partition = thread_id * partitions_per_thread;
            let end_partition = ((thread_id + 1) * partitions_per_thread).min(offsets.len());

            if start_partition >= offsets.len() - 1 {
                break;
            }

            let start_offset = offsets[start_partition];
            let end_offset = offsets[end_partition.min(offsets.len() - 1)];

            let input = input.clone();
            let tx = tx.clone();

            let handle = thread::spawn(move || -> Result<u64, Box<dyn Error + Send + Sync>> {
                send_kvbin_chunk_indexed(&input, start_offset, end_offset, tx)
            });

            handles.push(handle);
        }

        drop(tx);

        // Main thread: append to DB
        let conn = Connection::open(db)?;
        let mut appender = conn.appender(table)?;
        let mut total_rows = 0u64;

        for (key, val) in rx {
            appender.append_row(params![key.as_slice(), val.as_slice()])?;
            total_rows += 1;
        }

        // Wait for all threads
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
    } else {
        // Sequential loading (no index or single thread)
        if !index_path.exists() {
            println!("No index file found, using sequential loading");
        }

        let file = File::open(input)?;
        let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);

        let conn = Connection::open(db)?;
        let mut appender = conn.appender(table)?;

        let mut rows = 0u64;
        let mut len_buf = [0u8; 4];
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();

        loop {
            // Read key length
            if let Err(e) = reader.read_exact(&mut len_buf) {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(e.into());
            }
            let klen = u32::from_le_bytes(len_buf) as usize;
            key_buf.resize(klen, 0);
            reader.read_exact(&mut key_buf)?;

            // Read value length
            reader.read_exact(&mut len_buf)?;
            let vlen = u32::from_le_bytes(len_buf) as usize;
            val_buf.resize(vlen, 0);
            reader.read_exact(&mut val_buf)?;

            appender.append_row(params![key_buf.as_slice(), val_buf.as_slice()])?;
            rows += 1;
        }

        Ok(rows)
    }
}
