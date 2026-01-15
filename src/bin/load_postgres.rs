use clap::{Parser, ValueEnum};
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
) -> Result<u64, Box<dyn Error>> {
    const KEY_SIZE: usize = 10;
    const PAYLOAD_SIZE: usize = 90;
    const RECORD_SIZE: usize = KEY_SIZE + PAYLOAD_SIZE;

    let file = File::open(input)?;
    let file_size = file.metadata()?.len();
    let num_records = file_size / RECORD_SIZE as u64;

    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut buf = vec![0u8; RECORD_SIZE];

    let mut tx = client.transaction()?;
    let insert_sql = format!(
        "INSERT INTO {} (sort_key, payload) VALUES ($1, $2)",
        table
    );
    let stmt = tx.prepare(&insert_sql)?;

    for _ in 0..num_records {
        reader.read_exact(&mut buf)?;

        let key = &buf[..KEY_SIZE];
        let payload = &buf[KEY_SIZE..];

        tx.execute(&stmt, &[&key, &payload])?;
    }

    tx.commit()?;
    Ok(num_records)
}

fn load_kvbin(
    input: &PathBuf,
    client: &mut Client,
    table: &str,
) -> Result<u64, Box<dyn Error>> {
    let file = File::open(input)?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut rows = 0;

    let mut tx = client.transaction()?;
    let insert_sql = format!(
        "INSERT INTO {} (sort_key, payload) VALUES ($1, $2)",
        table
    );
    let stmt = tx.prepare(&insert_sql)?;

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

        tx.execute(&stmt, &[&key_bytes, &val_bytes])?;
        rows += 1;
    }

    tx.commit()?;
    Ok(rows)
}
