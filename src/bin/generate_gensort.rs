use clap::Parser;
use rand::RngCore;
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "generate-gensort")]
struct Args {
    /// Output file path
    #[arg(long)]
    output: PathBuf,

    /// Number of records to generate
    #[arg(long)]
    num_records: u64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    const KEY_SIZE: usize = 10;
    const PAYLOAD_SIZE: usize = 90;
    const RECORD_SIZE: usize = KEY_SIZE + PAYLOAD_SIZE;

    let file = File::create(&args.output)?;
    let mut writer = BufWriter::with_capacity(16 * 1024 * 1024, file); // 16MB buffer

    let mut record = vec![0u8; RECORD_SIZE];
    let mut rng = rand::rng();

    let start = std::time::Instant::now();
    let mut last_report = start;

    for i in 0..args.num_records {
        // Generate random 10-byte key
        rng.fill_bytes(&mut record[..KEY_SIZE]);

        // Generate random 90-byte payload
        rng.fill_bytes(&mut record[KEY_SIZE..]);

        writer.write_all(&record)?;

        // Progress reporting every 1 million records
        if i > 0 && i % 1_000_000 == 0 {
            let elapsed = last_report.elapsed().as_secs_f64();
            let records_per_sec = 1_000_000.0 / elapsed;
            let mb_written = (i * RECORD_SIZE as u64) as f64 / (1024.0 * 1024.0);
            let progress = (i as f64 / args.num_records as f64) * 100.0;

            eprintln!(
                "Progress: {:.1}% ({} / {} records, {:.2} MB, {:.0} rec/s)",
                progress, i, args.num_records, mb_written, records_per_sec
            );

            last_report = std::time::Instant::now();
        }
    }

    writer.flush()?;

    let total_elapsed = start.elapsed().as_secs_f64();
    let total_mb = (args.num_records * RECORD_SIZE as u64) as f64 / (1024.0 * 1024.0);
    let total_gb = total_mb / 1024.0;

    eprintln!("\n=== Generation Complete ===");
    eprintln!("Records: {}", args.num_records);
    eprintln!("File size: {:.2} GB ({:.2} MB)", total_gb, total_mb);
    eprintln!("Time: {:.2} seconds", total_elapsed);
    eprintln!("Speed: {:.2} MB/s", total_mb / total_elapsed);
    eprintln!("Output: {}", args.output.display());

    Ok(())
}
