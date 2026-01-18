use duckdb::Connection;
use std::fs;
use std::process::Command;

fn load_duckdb_binary() -> String {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    format!("target/{}/load-duckdb", profile)
}

fn sort_duckdb_binary() -> String {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    format!("target/{}/sort-duckdb", profile)
}

fn run_loader(format: &str, input: &str, db: &str, table: &str) -> std::process::Output {
    Command::new(load_duckdb_binary())
        .args([
            "--format", format, "--input", input, "--db", db, "--table", table,
        ])
        .output()
        .expect("Failed to execute command")
}

fn run_sorter(db: &str, output: &str, table: &str, memory_limit: &str) -> std::process::Output {
    Command::new(sort_duckdb_binary())
        .args([
            "--db",
            db,
            "--output",
            output,
            "--table",
            table,
            "--memory-limit",
            memory_limit,
        ])
        .output()
        .expect("Failed to execute command")
}

#[test]
fn test_gensort_format() {
    let db_path = "/tmp/test_gensort_integration.duckdb";
    let input_path = "testdata/test_gensort.dat";
    let table = "gensort_test";

    // Clean up any existing database
    let _ = fs::remove_file(db_path);

    // Run the loader
    let output = run_loader("gensort", input_path, db_path, table);
    assert!(
        output.status.success(),
        "Loader failed: {:?}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify the data
    let conn = Connection::open(db_path).expect("Failed to open database");
    let mut stmt = conn
        .prepare(&format!("SELECT sort_key, payload FROM {}", table))
        .unwrap();
    let rows: Vec<(Vec<u8>, Vec<u8>)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(rows.len(), 3, "Expected 3 rows");

    // Verify keys
    assert_eq!(&rows[0].0, b"AAAAAAAAAA");
    assert_eq!(&rows[1].0, b"BBBBBBBBBB");
    assert_eq!(&rows[2].0, b"CCCCCCCCCC");

    // Verify payloads (90 bytes each)
    assert_eq!(rows[0].1.len(), 90);
    assert_eq!(rows[1].1.len(), 90);
    assert_eq!(rows[2].1.len(), 90);
    assert!(rows[0].1.iter().all(|&b| b == b'1'));
    assert!(rows[1].1.iter().all(|&b| b == b'2'));
    assert!(rows[2].1.iter().all(|&b| b == b'3'));

    // Clean up
    let _ = fs::remove_file(db_path);
}

#[test]
fn test_kvbin_format() {
    let db_path = "/tmp/test_kvbin_integration.duckdb";
    let input_path = "testdata/test_kvbin.dat";
    let table = "kvbin_test";

    // Clean up any existing database
    let _ = fs::remove_file(db_path);

    // Run the loader
    let output = run_loader("kvbin", input_path, db_path, table);
    assert!(
        output.status.success(),
        "Loader failed: {:?}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify the data
    let conn = Connection::open(db_path).expect("Failed to open database");
    let mut stmt = conn
        .prepare(&format!("SELECT sort_key, payload FROM {}", table))
        .unwrap();
    let rows: Vec<(Vec<u8>, Vec<u8>)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(rows.len(), 3, "Expected 3 rows");

    // Verify key-value pairs
    assert_eq!(&rows[0].0, b"key1");
    assert_eq!(&rows[0].1, b"value1");
    assert_eq!(&rows[1].0, b"key2");
    assert_eq!(&rows[1].1, b"value2");
    assert_eq!(&rows[2].0, b"hello");
    assert_eq!(&rows[2].1, b"world");

    // Clean up
    let _ = fs::remove_file(db_path);
}

#[test]
fn test_binary_data_preserved() {
    // Create a test file with non-UTF8 binary data
    let input_path = "/tmp/test_binary.dat";
    let db_path = "/tmp/test_binary_integration.duckdb";
    let table = "binary_test";

    // Create gensort record with binary data (including null bytes and high bytes)
    let mut record = vec![0u8; 100];
    // Key: 10 bytes with various binary values
    record[0..10].copy_from_slice(&[0x00, 0xFF, 0x80, 0x7F, 0x01, 0xFE, 0x10, 0xEF, 0x55, 0xAA]);
    // Payload: 90 bytes
    for i in 10..100 {
        record[i] = (i % 256) as u8;
    }
    fs::write(input_path, &record).expect("Failed to write test file");

    // Clean up any existing database
    let _ = fs::remove_file(db_path);

    // Run the loader
    let output = run_loader("gensort", input_path, db_path, table);
    assert!(
        output.status.success(),
        "Loader failed: {:?}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify the binary data was preserved exactly
    let conn = Connection::open(db_path).expect("Failed to open database");
    let mut stmt = conn
        .prepare(&format!("SELECT sort_key, payload FROM {}", table))
        .unwrap();
    let (key, payload): (Vec<u8>, Vec<u8>) = stmt
        .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap();

    assert_eq!(key, &record[0..10], "Binary key not preserved");
    assert_eq!(payload, &record[10..100], "Binary payload not preserved");

    // Clean up
    let _ = fs::remove_file(input_path);
    let _ = fs::remove_file(db_path);
}

#[test]
fn test_default_table_name() {
    let db_path = "/tmp/test_default_table.duckdb";
    let input_path = "testdata/test_kvbin.dat";

    // Clean up any existing database
    let _ = fs::remove_file(db_path);

    // Run without --table argument
    let output = Command::new(load_duckdb_binary())
        .args(["--format", "kvbin", "--input", input_path, "--db", db_path])
        .output()
        .expect("Failed to execute command");
    assert!(
        output.status.success(),
        "Loader failed: {:?}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify the default table name was used
    let conn = Connection::open(db_path).expect("Failed to open database");
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM bench_data", [], |row| row.get(0))
        .expect("Default table 'bench_data' should exist");
    assert_eq!(count, 3);

    // Clean up
    let _ = fs::remove_file(db_path);
}

#[test]
fn test_external_sort() {
    use rand::Rng;

    let input_path = "/tmp/test_sort_input.dat";
    let db_path = "/tmp/test_sort.duckdb";
    let output_path = "/tmp/test_sort_output.parquet";
    let table = "sort_test";

    // Generate 100 random gensort records with random keys
    let mut rng = rand::rng();
    let mut records: Vec<[u8; 100]> = Vec::with_capacity(100);
    for i in 0..100u8 {
        let mut record = [0u8; 100];
        // Random 10-byte key
        rng.fill(&mut record[0..10]);
        // Payload with record number for identification
        record[10] = i;
        for j in 11..100 {
            record[j] = b'X';
        }
        records.push(record);
    }

    // Write records (in random order, not sorted)
    let mut file_data = Vec::with_capacity(100 * 100);
    for record in &records {
        file_data.extend_from_slice(record);
    }
    fs::write(input_path, &file_data).expect("Failed to write test file");

    // Clean up any existing files
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(output_path);

    // Load data into DuckDB
    let output = run_loader("gensort", input_path, db_path, table);
    assert!(
        output.status.success(),
        "Loader failed: stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Run external sort
    let output = run_sorter(db_path, output_path, table, "128MB");
    assert!(
        output.status.success(),
        "Sorter failed: stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify output is sorted by reading from DuckDB via parquet
    let conn = Connection::open_in_memory().expect("Failed to open in-memory database");
    let query = format!(
        "SELECT sort_key, payload FROM read_parquet('{}')",
        output_path
    );
    let mut stmt = conn.prepare(&query).unwrap();
    let rows: Vec<(Vec<u8>, Vec<u8>)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(rows.len(), 100, "Expected 100 rows in output");

    // Verify the output is sorted by comparing adjacent keys
    for i in 1..rows.len() {
        let prev_key = &rows[i - 1].0;
        let curr_key = &rows[i].0;
        assert!(
            prev_key <= curr_key,
            "Output not sorted at index {}: {:?} > {:?}",
            i,
            prev_key,
            curr_key
        );
    }

    // Verify first key is smallest and last key is largest
    let min_key = rows.iter().map(|(k, _)| k).min().unwrap();
    let max_key = rows.iter().map(|(k, _)| k).max().unwrap();
    assert_eq!(&rows[0].0, min_key, "First row should have smallest key");
    assert_eq!(&rows[99].0, max_key, "Last row should have largest key");

    // Clean up
    let _ = fs::remove_file(input_path);
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(output_path);
}
