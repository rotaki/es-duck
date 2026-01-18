use clap::{Parser, ValueEnum};
use postgres::binary_copy::BinaryCopyInWriter;
use postgres::types::Type;
use postgres::{Client, NoTls};
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
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let mut client = Client::connect(&args.db, NoTls)?;

    client.batch_execute(&format!(
        "CREATE UNLOGGED TABLE IF NOT EXISTS {} (sort_key BYTEA, payload BYTEA);",
        args.table
    ))?;

    let rows = match args.format {
        InputFormat::Gensort => load_gensort(&args.input, &mut client, &args.table)?,
        InputFormat::Kvbin => load_kvbin(&args.input, &mut client, &args.table)?,
    };

    Ok(())
}

fn load_gensort(
    input: &PathBuf,
    client: &mut Client,
    table: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    const KEY_SIZE: usize = 10;
    const PAYLOAD_SIZE: usize = 90;
    const RECORD_SIZE: usize = KEY_SIZE + PAYLOAD_SIZE;

    let file = File::open(input)?;
    let file_size = file.metadata()?.len();
    let num_records = file_size / RECORD_SIZE as u64;

    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut buf = [0u8; RECORD_SIZE];

    let mut tx = client.transaction()?;
    // Nice for bulk load benchmarks; remove if you care about durability of each commit.
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

    let inserted = writer.finish()?; // rows inserted
    tx.commit()?;
    println!("Inserted {} rows into {}", inserted, table);
    Ok(inserted)
}

fn load_kvbin(
    input: &PathBuf,
    client: &mut Client,
    table: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
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
