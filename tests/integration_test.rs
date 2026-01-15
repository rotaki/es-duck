use std::process::Command;
use std::fs;
use duckdb::Connection;

fn binary_path() -> String {
    let profile = if cfg!(debug_assertions) { "debug" } else { "release" };
    format!("target/{}/es-duck", profile)
}

fn run_loader(format: &str, input: &str, db: &str, table: &str) -> std::process::Output {
    Command::new(binary_path())
        .args(["--format", format, "--input", input, "--db", db, "--table", table])
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
    assert!(output.status.success(), "Loader failed: {:?}", String::from_utf8_lossy(&output.stderr));

    // Verify the data
    let conn = Connection::open(db_path).expect("Failed to open database");
    let mut stmt = conn.prepare(&format!("SELECT sort_key, payload FROM {}", table)).unwrap();
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
    assert!(output.status.success(), "Loader failed: {:?}", String::from_utf8_lossy(&output.stderr));

    // Verify the data
    let conn = Connection::open(db_path).expect("Failed to open database");
    let mut stmt = conn.prepare(&format!("SELECT sort_key, payload FROM {}", table)).unwrap();
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
    assert!(output.status.success(), "Loader failed: {:?}", String::from_utf8_lossy(&output.stderr));

    // Verify the binary data was preserved exactly
    let conn = Connection::open(db_path).expect("Failed to open database");
    let mut stmt = conn.prepare(&format!("SELECT sort_key, payload FROM {}", table)).unwrap();
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
    let output = Command::new(binary_path())
        .args(["--format", "kvbin", "--input", input_path, "--db", db_path])
        .output()
        .expect("Failed to execute command");
    assert!(output.status.success(), "Loader failed: {:?}", String::from_utf8_lossy(&output.stderr));

    // Verify the default table name was used
    let conn = Connection::open(db_path).expect("Failed to open database");
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM bench_data", [], |row| row.get(0))
        .expect("Default table 'bench_data' should exist");
    assert_eq!(count, 3);

    // Clean up
    let _ = fs::remove_file(db_path);
}
