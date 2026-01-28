use clap::{Parser, ValueEnum};
use clickhouse::Client;
use std::error::Error;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc::{Sender, channel};
use tokio::task;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum InputFormat {
    Gensort,
    Kvbin,
}

#[derive(Parser)]
#[command(name = "es-duck-clickhouse")]
struct Args {
    #[arg(long, value_enum)]
    format: InputFormat,

    #[arg(long)]
    input: PathBuf,

    #[arg(long, default_value = "http://localhost:8123")]
    url: String,

    #[arg(long, default_value = "default")]
    database: String,

    #[arg(long, default_value = "bench_data")]
    table: String,

    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Number of records to batch before sending (higher = more memory, less overhead)
    #[arg(long, default_value_t = 100_000)]
    batch_size: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();

    // Initialize ClickHouse connection for table setup
    let client = Client::default()
        .with_url(&args.url)
        .with_database(&args.database);

    // Create table (unsorted for benchmarking)
    println!("Creating table if not exists...");
    client
        .query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                sort_key String,
                payload String
            ) ENGINE = MergeTree()
            ORDER BY tuple()",
            args.table
        ))
        .execute()
        .await?;

    println!(
        "Starting load from {:?} with {} threads (batch_size={})...",
        args.input, args.threads, args.batch_size
    );

    let rows = match args.format {
        InputFormat::Gensort => {
            load_gensort_streaming(
                &args.input,
                &args.url,
                &args.table,
                args.threads,
                args.batch_size,
            )
            .await?
        }
        InputFormat::Kvbin => {
            load_kvbin_streaming(
                &args.input,
                &args.url,
                &args.table,
                args.threads,
                args.batch_size,
            )
            .await?
        }
    };

    println!("Successfully loaded {} rows to ClickHouse.", rows);
    Ok(())
}

/// Optimized Gensort loader using direct RowBinary streaming
async fn load_gensort_streaming(
    input: &PathBuf,
    url: &str,
    table: &str,
    num_threads: usize,
    batch_size: usize,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    const KEY_SIZE: usize = 10;
    const PAYLOAD_SIZE: usize = 90;
    const RECORD_SIZE: usize = KEY_SIZE + PAYLOAD_SIZE;

    let file = File::open(input)?;
    let file_size = file.metadata()?.len();
    let total_records = file_size / RECORD_SIZE as u64;
    drop(file);

    // Use bounded channel to prevent OOM (buffer up to threads*4 batches)
    let (tx, rx) = channel::<Vec<u8>>(num_threads * 4);

    // Spawn HTTP uploader task
    let upload_url = format!("{}/?query=INSERT+INTO+{}+FORMAT+RowBinary", url, table);
    let total_rows = Arc::new(AtomicU64::new(0));
    let total_rows_clone = total_rows.clone();

    let uploader = tokio::spawn(async move {
        let client = reqwest::Client::new();
        let reader = ChannelReader::new(rx, total_rows_clone);
        let stream = tokio_util::io::ReaderStream::new(reader);
        client
            .post(&upload_url)
            .body(reqwest::Body::wrap_stream(stream))
            .send()
            .await
    });

    // Spawn reader/formatter threads
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

        let handle = task::spawn_blocking(move || -> Result<u64, Box<dyn Error + Send + Sync>> {
            format_gensort_to_rowbinary(&input, start_record, end_record, tx, batch_size)
        });

        handles.push(handle);
    }

    // Drop original sender so channel closes when all threads finish
    drop(tx);

    // Wait for all reader threads
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(result) => match result {
                Ok(rows) => println!("Thread {} formatted {} records", i, rows),
                Err(e) => return Err(format!("Thread {} failed: {}", i, e).into()),
            },
            Err(e) => return Err(format!("Thread {} panicked: {}", i, e).into()),
        }
    }

    // Wait for uploader
    let resp = uploader
        .await
        .map_err(|e| format!("Uploader task failed: {}", e))??;
    if !resp.status().is_success() {
        let error_text = resp
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());
        return Err(format!("ClickHouse error: {}", error_text).into());
    }

    Ok(total_rows.load(Ordering::Relaxed))
}

/// Formats Gensort records into ClickHouse RowBinary format
/// RowBinary for (String, String): [varint_len][bytes][varint_len][bytes]
fn format_gensort_to_rowbinary(
    input: &PathBuf,
    start_record: u64,
    end_record: u64,
    tx: Sender<Vec<u8>>,
    batch_size: usize,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    const RECORD_SIZE: usize = 100;
    const KEY_SIZE: usize = 10;
    const PAYLOAD_SIZE: usize = 90;

    let mut file = File::open(input)?;
    file.seek(SeekFrom::Start(start_record * RECORD_SIZE as u64))?;
    let mut reader = BufReader::with_capacity(4 * 1024 * 1024, file);

    // Pre-allocate output buffer: each record = 1 byte + 10 bytes + 1 byte + 90 bytes = 102 bytes
    let mut output_buffer = Vec::with_capacity(batch_size * 102);
    let mut raw_record = [0u8; RECORD_SIZE];
    let num_records = end_record - start_record;

    for _ in 0..num_records {
        reader.read_exact(&mut raw_record)?;

        // Key: varint length (10 fits in 1 byte) + data
        output_buffer.push(KEY_SIZE as u8);
        output_buffer.extend_from_slice(&raw_record[..KEY_SIZE]);

        // Payload: varint length (90 fits in 1 byte) + data
        output_buffer.push(PAYLOAD_SIZE as u8);
        output_buffer.extend_from_slice(&raw_record[KEY_SIZE..]);

        // Send batch when full
        if output_buffer.len() >= batch_size * 102 {
            tx.blocking_send(std::mem::take(&mut output_buffer))?;
            output_buffer = Vec::with_capacity(batch_size * 102);
        }
    }

    // Send remaining records
    if !output_buffer.is_empty() {
        tx.blocking_send(output_buffer)?;
    }

    Ok(num_records)
}

/// Optimized Kvbin loader using direct RowBinary streaming
async fn load_kvbin_streaming(
    input: &PathBuf,
    url: &str,
    table: &str,
    num_threads: usize,
    batch_size: usize,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    let file_size = File::open(input)?.metadata()?.len();

    // Check for index file
    let mut index_path = input.as_os_str().to_owned();
    index_path.push(".idx");
    let index_path = PathBuf::from(index_path);

    if !index_path.exists() || num_threads == 1 {
        if !index_path.exists() {
            println!("No index file found, using sequential loading");
        }
        return load_kvbin_sequential(input, url, table).await;
    }

    // Parallel loading using index
    println!("Loading index from {:?}...", index_path);
    let offsets = load_index(&index_path, file_size)
        .map_err(|e| -> Box<dyn Error + Send + Sync> { e.into() })?;

    println!(
        "Index loaded: {} offset points, using {} threads",
        offsets.len(),
        num_threads
    );

    // Use bounded channel to prevent OOM
    let (tx, rx) = channel::<Vec<u8>>(num_threads * 4);

    // Spawn HTTP uploader task
    let upload_url = format!("{}/?query=INSERT+INTO+{}+FORMAT+RowBinary", url, table);
    let total_rows = Arc::new(AtomicU64::new(0));
    let total_rows_clone = total_rows.clone();

    let uploader = tokio::spawn(async move {
        let client = reqwest::Client::new();
        let reader = ChannelReader::new(rx, total_rows_clone);
        let stream = tokio_util::io::ReaderStream::new(reader);
        client
            .post(&upload_url)
            .body(reqwest::Body::wrap_stream(stream))
            .send()
            .await
    });

    // Divide work among threads
    let offsets = Arc::new(offsets);
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

        let handle = task::spawn_blocking(move || -> Result<u64, Box<dyn Error + Send + Sync>> {
            format_kvbin_to_rowbinary(&input, start_offset, end_offset, tx, batch_size)
        });

        handles.push(handle);
    }

    drop(tx);

    // Wait for all reader threads
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(result) => match result {
                Ok(rows) => println!("Thread {} formatted {} records", i, rows),
                Err(e) => return Err(format!("Thread {} failed: {}", i, e).into()),
            },
            Err(e) => return Err(format!("Thread {} panicked: {}", i, e).into()),
        }
    }

    // Wait for uploader
    let resp = uploader
        .await
        .map_err(|e| format!("Uploader task failed: {}", e))??;
    if !resp.status().is_success() {
        let error_text = resp
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());
        return Err(format!("ClickHouse error: {}", error_text).into());
    }

    Ok(total_rows.load(Ordering::Relaxed))
}

/// Sequential kvbin loading for single-threaded or non-indexed cases
async fn load_kvbin_sequential(
    input: &PathBuf,
    url: &str,
    table: &str,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    let (tx, rx) = channel::<Vec<u8>>(4);

    let upload_url = format!("{}/?query=INSERT+INTO+{}+FORMAT+RowBinary", url, table);
    let total_rows = Arc::new(AtomicU64::new(0));
    let total_rows_clone = total_rows.clone();

    let uploader = tokio::spawn(async move {
        let client = reqwest::Client::new();
        let reader = ChannelReader::new(rx, total_rows_clone);
        let stream = tokio_util::io::ReaderStream::new(reader);
        client
            .post(&upload_url)
            .body(reqwest::Body::wrap_stream(stream))
            .send()
            .await
    });

    let input = input.clone();
    let reader_task = task::spawn_blocking(move || -> Result<u64, Box<dyn Error + Send + Sync>> {
        format_kvbin_sequential_to_rowbinary(&input, tx, 50_000)
    });

    let rows = reader_task.await??;

    let resp = uploader
        .await
        .map_err(|e| format!("Uploader task failed: {}", e))??;
    if !resp.status().is_success() {
        let error_text = resp
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());
        return Err(format!("ClickHouse error: {}", error_text).into());
    }

    Ok(rows)
}

/// Formats kvbin records into RowBinary format
fn format_kvbin_to_rowbinary(
    input: &PathBuf,
    start_offset: u64,
    end_offset: u64,
    tx: Sender<Vec<u8>>,
    batch_size: usize,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    let mut file = File::open(input)?;
    file.seek(SeekFrom::Start(start_offset))?;
    let mut reader = BufReader::with_capacity(4 * 1024 * 1024, file);

    let mut output_buffer = Vec::with_capacity(batch_size * 128); // Estimate
    let mut rows = 0u64;
    let mut len_buf = [0u8; 4];
    let mut key_buf = Vec::new();
    let mut val_buf = Vec::new();
    let mut current_pos = start_offset;

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

        current_pos += 8 + klen as u64 + vlen as u64;

        // Write to RowBinary: varint key_len + key + varint val_len + val
        write_varint(&mut output_buffer, klen as u64);
        output_buffer.extend_from_slice(&key_buf);
        write_varint(&mut output_buffer, vlen as u64);
        output_buffer.extend_from_slice(&val_buf);

        rows += 1;

        // Send batch when large enough
        if output_buffer.len() >= batch_size * 128 {
            tx.blocking_send(std::mem::take(&mut output_buffer))?;
            output_buffer = Vec::with_capacity(batch_size * 128);
        }

        if current_pos >= end_offset {
            break;
        }
    }

    // Send remaining data
    if !output_buffer.is_empty() {
        tx.blocking_send(output_buffer)?;
    }

    Ok(rows)
}

fn format_kvbin_sequential_to_rowbinary(
    input: &PathBuf,
    tx: Sender<Vec<u8>>,
    batch_size: usize,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    let file = File::open(input)?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);

    let mut output_buffer = Vec::with_capacity(batch_size * 128);
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

        // Write to RowBinary
        write_varint(&mut output_buffer, klen as u64);
        output_buffer.extend_from_slice(&key_buf);
        write_varint(&mut output_buffer, vlen as u64);
        output_buffer.extend_from_slice(&val_buf);

        rows += 1;

        // Send batch
        if output_buffer.len() >= batch_size * 128 {
            tx.blocking_send(std::mem::take(&mut output_buffer))?;
            output_buffer = Vec::with_capacity(batch_size * 128);
        }
    }

    if !output_buffer.is_empty() {
        tx.blocking_send(output_buffer)?;
    }

    Ok(rows)
}

/// Write variable-length integer (LEB128 encoding used by ClickHouse)
fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
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

/// Reader that pulls data from channel and tracks row count
struct ChannelReader {
    rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    current_chunk: Option<Vec<u8>>,
    pos: usize,
    total_rows: Arc<AtomicU64>,
    last_million_printed: u64,
}

impl ChannelReader {
    fn new(rx: tokio::sync::mpsc::Receiver<Vec<u8>>, total_rows: Arc<AtomicU64>) -> Self {
        Self {
            rx,
            current_chunk: None,
            pos: 0,
            total_rows,
            last_million_printed: 0,
        }
    }
}

impl AsyncRead for ChannelReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            // Try to read from current chunk
            if let Some(ref chunk) = self.current_chunk {
                let pos = self.pos;
                let chunk_len = chunk.len();
                if pos < chunk_len {
                    let remaining = chunk_len - pos;
                    let to_copy = remaining.min(buf.remaining());
                    buf.put_slice(&chunk[pos..pos + to_copy]);
                    self.pos += to_copy;

                    // Clear chunk if fully consumed
                    if self.pos >= chunk_len {
                        self.current_chunk = None;
                        self.pos = 0;
                    }

                    return Poll::Ready(Ok(()));
                }
            }

            // Need new chunk from channel
            match self.rx.try_recv() {
                Ok(chunk) => {
                    // Estimate rows (for gensort: 102 bytes/row, for kvbin: varies)
                    let estimated_rows = chunk.len() / 102;
                    let new_total = self
                        .total_rows
                        .fetch_add(estimated_rows as u64, Ordering::Relaxed)
                        + estimated_rows as u64;

                    let current_million = new_total / 1_000_000;
                    if current_million > self.last_million_printed {
                        println!("Uploaded ~{} million records...", current_million);
                        self.last_million_printed = current_million;
                    }

                    self.current_chunk = Some(chunk);
                    self.pos = 0;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    // No data available, register waker and return Pending
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    // Channel closed, EOF
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}
