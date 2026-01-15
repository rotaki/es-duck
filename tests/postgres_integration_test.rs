use std::process::Command;
use postgres::{Client, NoTls};

fn postgres_url() -> Option<String> {
    std::env::var("POSTGRES_TEST_URL").ok()
}

fn binary_path_postgres() -> String {
    let profile = if cfg!(debug_assertions) { "debug" } else { "release" };
    format!("target/{}/es-duck-postgres", profile)
}

fn run_postgres_loader(format: &str, input: &str, db_url: &str, table: &str) -> std::process::Output {
    Command::new(binary_path_postgres())
        .args(["--format", format, "--input", input, "--db", db_url, "--table", table])
        .output()
        .expect("Failed to execute es-duck-postgres")
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

