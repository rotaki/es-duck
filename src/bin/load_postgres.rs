use clap::{Parser, ValueEnum};
use postgres::binary_copy::BinaryCopyInWriter;
use postgres::types::Type;
use postgres::{Client, NoTls};
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum InputFormat {
    Gensort,
    Kvbin,
}

#[derive(Parser)]
#[command(name = "es-duck-postgres")]
struct Args {
    #[arg(long, value_enum)]
    format: InputFormat,

    #[arg(long)]
    input: PathBuf,

    /// PostgreSQL connection string, e.g. postgres://user:pass@host/db
    #[arg(long)]
    db: String,

    #[arg(long, default_value = "bench_data")]
    table: String,

    #[arg(long, default_value_t = 1)]
    threads: usize,
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();

    let mut client = Client::connect(&args.db, NoTls)?;

    client.batch_execute(&format!(
        "CREATE UNLOGGED TABLE IF NOT EXISTS {} (sort_key BYTEA, payload BYTEA);",
        args.table
    ))?;

    drop(client);

    println!(
        "Starting load from {:?} with {} threads...",
        args.input, args.threads
    );

    let rows = match args.format {
        InputFormat::Gensort => load_gensort(&args.input, &args.db, &args.table, args.threads)?,
        InputFormat::Kvbin => {
            let mut client = Client::connect(&args.db, NoTls)?;
            load_kvbin(&args.input, &mut client, &args.table)?
        }
    };

    println!("Successfully loaded {} rows", rows);
    Ok(())
}

fn load_gensort_chunk(
    input: &PathBuf,
    db_conn_str: &str,
    table: &str,
    start_record: u64,
    end_record: u64,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    const KEY_SIZE: usize = 10;
    const PAYLOAD_SIZE: usize = 90;
    const RECORD_SIZE: usize = KEY_SIZE + PAYLOAD_SIZE;

    let mut file = File::open(input)?;
    file.seek(SeekFrom::Start(start_record * RECORD_SIZE as u64))?;

    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut buf = vec![0u8; RECORD_SIZE];
    let num_records = end_record - start_record;

    let mut client = Client::connect(db_conn_str, NoTls)?;
    let mut tx = client.transaction()?;
    tx.batch_execute("SET LOCAL synchronous_commit = off;")?;

    let copy_stmt = format!("COPY {} (sort_key, payload) FROM STDIN BINARY", table);
    let sink = tx.copy_in(&copy_stmt)?;
    let mut writer = BinaryCopyInWriter::new(sink, &[Type::BYTEA, Type::BYTEA]);

    for _ in 0..num_records {
        reader.read_exact(&mut buf)?;
        let key = &buf[..KEY_SIZE];
        let payload = &buf[KEY_SIZE..];
        writer.write(&[&key, &payload])?;
    }

    let inserted = writer.finish()?;
    tx.commit()?;
    Ok(inserted)
}

fn load_gensort(
    input: &PathBuf,
    db_conn_str: &str,
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
        return load_gensort_chunk(input, db_conn_str, table, 0, total_records);
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
        let db_conn_str = db_conn_str.to_string();
        let table = table.to_string();
        let total_rows = Arc::clone(&total_rows);

        let handle = thread::spawn(move || -> Result<u64, Box<dyn Error + Send + Sync>> {
            let rows = load_gensort_chunk(&input, &db_conn_str, &table, start_record, end_record)?;

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
    println!("Inserted {} total rows into {}", total, table);
    Ok(total)
}

fn load_kvbin(
    input: &PathBuf,
    client: &mut Client,
    table: &str,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    let file = File::open(input)?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);

    let mut tx = client.transaction()?;
    tx.batch_execute("SET LOCAL synchronous_commit = off;")?;

    let copy_stmt = format!("COPY {} (sort_key, payload) FROM STDIN BINARY", table);
    let sink = tx.copy_in(&copy_stmt)?;
    let mut writer = BinaryCopyInWriter::new(sink, &[Type::BYTEA, Type::BYTEA]);

    let mut rows: u64 = 0;
    let mut len_buf = [0u8; 4];
    let mut key_buf: Vec<u8> = Vec::new();
    let mut val_buf: Vec<u8> = Vec::new();

    loop {
        // read klen
        if let Err(e) = reader.read_exact(&mut len_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(e.into());
        }
        let klen = u32::from_le_bytes(len_buf) as usize;
        key_buf.resize(klen, 0);
        reader.read_exact(&mut key_buf)?;

        // read vlen
        reader.read_exact(&mut len_buf)?;
        let vlen = u32::from_le_bytes(len_buf) as usize;
        val_buf.resize(vlen, 0);
        reader.read_exact(&mut val_buf)?;

        writer.write(&[&key_buf.as_slice(), &val_buf.as_slice()])?;
        rows += 1;
    }

    let inserted = writer.finish()?;
    tx.commit()?;
    println!("Inserted {} rows into {}", inserted, table);
    Ok(inserted.max(rows)) // inserted should equal rows; keep it robust
}
