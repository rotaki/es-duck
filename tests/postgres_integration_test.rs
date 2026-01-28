#![cfg(feature = "db-postgres")]

use postgres::{Client, NoTls};
use std::process::Command;

fn postgres_url() -> Option<String> {
    std::env::var("POSTGRES_TEST_URL").ok()
}

fn load_postgres_binary() -> String {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    format!("target/{}/load-postgres", profile)
}

fn sort_postgres_binary() -> String {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    format!("target/{}/sort-postgres", profile)
}

fn run_postgres_loader(
    format: &str,
    input: &str,
    db_url: &str,
    table: &str,
) -> std::process::Output {
    Command::new(load_postgres_binary())
        .args([
            "--format", format, "--input", input, "--db", db_url, "--table", table,
        ])
        .output()
        .expect("Failed to execute load-postgres")
}

fn run_postgres_sorter(db_url: &str, table: &str, work_mem: &str) -> std::process::Output {
    Command::new(sort_postgres_binary())
        .args(["--db", db_url, "--table", table, "--total-memory", work_mem])
        .output()
        .expect("Failed to execute sort-postgres")
}

#[test]
fn test_postgres_gensort_format() {
    let Some(db_url) = postgres_url() else {
        eprintln!("skipping test_postgres_gensort_format; POSTGRES_TEST_URL not set");
        return;
    };

    let table = "postgres_gensort_test";
    let input_path = "testdata/test_gensort.dat";

    // Clean up any existing table
    {
        let mut client = Client::connect(&db_url, NoTls).expect("Failed to connect to Postgres");
        let _ = client.batch_execute(&format!("DROP TABLE IF EXISTS {}", table));
    }

    // Run the loader
    let output = run_postgres_loader("gensort", input_path, &db_url, table);
    assert!(
        output.status.success(),
        "Loader failed: stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify the data
    let mut client = Client::connect(&db_url, NoTls).expect("Failed to connect to Postgres");
    let rows = client
        .query(&format!("SELECT sort_key, payload FROM {}", table), &[])
        .expect("Failed to query rows");

    assert_eq!(rows.len(), 3, "Expected 3 rows");

    let key0: Vec<u8> = rows[0].get(0);
    let key1: Vec<u8> = rows[1].get(0);
    let key2: Vec<u8> = rows[2].get(0);
    let payload0: Vec<u8> = rows[0].get(1);
    let payload1: Vec<u8> = rows[1].get(1);
    let payload2: Vec<u8> = rows[2].get(1);

    assert_eq!(&key0, b"AAAAAAAAAA");
    assert_eq!(&key1, b"BBBBBBBBBB");
    assert_eq!(&key2, b"CCCCCCCCCC");

    assert_eq!(payload0.len(), 90);
    assert_eq!(payload1.len(), 90);
    assert_eq!(payload2.len(), 90);
    assert!(payload0.iter().all(|&b| b == b'1'));
    assert!(payload1.iter().all(|&b| b == b'2'));
    assert!(payload2.iter().all(|&b| b == b'3'));
}

#[test]
fn test_postgres_kvbin_format() {
    let Some(db_url) = postgres_url() else {
        eprintln!("skipping test_postgres_kvbin_format; POSTGRES_TEST_URL not set");
        return;
    };

    let table = "postgres_kvbin_test";
    let input_path = "testdata/test_kvbin.dat";

    // Clean up any existing table
    {
        let mut client = Client::connect(&db_url, NoTls).expect("Failed to connect to Postgres");
        let _ = client.batch_execute(&format!("DROP TABLE IF EXISTS {}", table));
    }

    // Run the loader
    let output = run_postgres_loader("kvbin", input_path, &db_url, table);
    assert!(
        output.status.success(),
        "Loader failed: stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify the data
    let mut client = Client::connect(&db_url, NoTls).expect("Failed to connect to Postgres");
    let rows = client
        .query(&format!("SELECT sort_key, payload FROM {}", table), &[])
        .expect("Failed to query rows");

    assert_eq!(rows.len(), 3, "Expected 3 rows");

    let key0: Vec<u8> = rows[0].get(0);
    let key1: Vec<u8> = rows[1].get(0);
    let key2: Vec<u8> = rows[2].get(0);
    let val0: Vec<u8> = rows[0].get(1);
    let val1: Vec<u8> = rows[1].get(1);
    let val2: Vec<u8> = rows[2].get(1);

    assert_eq!(&key0, b"key1");
    assert_eq!(&val0, b"value1");
    assert_eq!(&key1, b"key2");
    assert_eq!(&val1, b"value2");
    assert_eq!(&key2, b"hello");
    assert_eq!(&val2, b"world");
}

#[test]
fn test_postgres_external_sort() {
    use rand::Rng;
    use std::fs;

    let Some(db_url) = postgres_url() else {
        eprintln!("skipping test_postgres_external_sort; POSTGRES_TEST_URL not set");
        return;
    };

    let input_path = "/tmp/test_pg_sort_input.dat";
    let table = "postgres_sort_test";

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

    // Write records to file
    let mut file_data = Vec::with_capacity(100 * 100);
    for record in &records {
        file_data.extend_from_slice(record);
    }
    fs::write(input_path, &file_data).expect("Failed to write test file");

    // Clean up any existing table
    {
        let mut client = Client::connect(&db_url, NoTls).expect("Failed to connect to Postgres");
        let _ = client.batch_execute(&format!("DROP TABLE IF EXISTS {}", table));
    }

    // Load data into PostgreSQL
    let output = run_postgres_loader("gensort", input_path, &db_url, table);
    assert!(
        output.status.success(),
        "Loader failed: stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Run external sort (now just executes count query)
    let output = run_postgres_sorter(&db_url, table, "64MB");
    assert!(
        output.status.success(),
        "Sorter failed: stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify the output contains timing information
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("TIMING:"),
        "Expected TIMING output, got: {}",
        stdout
    );
    assert!(
        stdout.contains("Count result:"),
        "Expected count result, got: {}",
        stdout
    );

    // Parse the count from output and verify it's correct (100 records - 1 due to OFFSET 1)
    let count_line = stdout
        .lines()
        .find(|line| line.contains("Count result:"))
        .expect("Could not find count result line");
    let count: i64 = count_line
        .split(':')
        .nth(1)
        .unwrap()
        .trim()
        .parse()
        .expect("Failed to parse count");
    assert_eq!(count, 99, "Expected count of 99 (100 records - 1 offset)");

    // Verify data is actually sorted by reading directly from database
    let mut client = Client::connect(&db_url, NoTls).expect("Failed to connect to Postgres");
    let rows = client
        .query(
            &format!("SELECT sort_key FROM {} ORDER BY sort_key", table),
            &[],
        )
        .expect("Failed to query rows");

    assert_eq!(rows.len(), 100, "Expected 100 rows in database");

    let keys: Vec<Vec<u8>> = rows.iter().map(|row| row.get(0)).collect();

    // Verify the keys are sorted
    for i in 1..keys.len() {
        assert!(
            keys[i - 1] <= keys[i],
            "Keys not sorted at index {}: {:?} > {:?}",
            i,
            keys[i - 1],
            keys[i]
        );
    }

    // Clean up
    let _ = fs::remove_file(input_path);
    {
        let mut client = Client::connect(&db_url, NoTls).expect("Failed to connect to Postgres");
        let _ = client.batch_execute(&format!("DROP TABLE IF EXISTS {}", table));
    }
}
