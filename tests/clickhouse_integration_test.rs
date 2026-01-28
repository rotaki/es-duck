#![cfg(feature = "db-clickhouse")]

use clickhouse::Client;
use std::fs;
use std::process::Command;

use std::sync::Once;

static INIT: Once = Once::new();

fn setup_env() {
    INIT.call_once(|| {
        unsafe {
            // Only set if not already present to allow overrides from the shell
            if std::env::var("CLICKHOUSE_URL").is_err() {
                std::env::set_var("CLICKHOUSE_URL", "http://localhost:8123");
            }
            if std::env::var("CLICKHOUSE_USER").is_err() {
                std::env::set_var("CLICKHOUSE_USER", "default");
            }
            // if std::env::var("CLICKHOUSE_PASSWORD").is_err() {
            //     std::env::set_var("CLICKHOUSE_PASSWORD", "");
            // }
            if std::env::var("CLICKHOUSE_DATABASE").is_err() {
                std::env::set_var("CLICKHOUSE_DATABASE", "default");
            }
        }
    });
}

fn clickhouse_url() -> Option<String> {
    std::env::var("CLICKHOUSE_URL").ok()
}

fn clickhouse_database() -> String {
    std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".to_string())
}

fn load_clickhouse_binary() -> String {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    format!("target/{}/load-clickhouse", profile)
}

fn sort_clickhouse_binary() -> String {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    format!("target/{}/sort-clickhouse", profile)
}

fn run_clickhouse_loader(
    format: &str,
    input: &str,
    url: &str,
    database: &str,
    table: &str,
) -> std::process::Output {
    Command::new(load_clickhouse_binary())
        .args([
            "--format",
            format,
            "--input",
            input,
            "--url",
            url,
            "--database",
            database,
            "--table",
            table,
        ])
        .output()
        .expect("Failed to execute load-clickhouse")
}

fn run_clickhouse_sorter(url: &str, database: &str, table: &str) -> std::process::Output {
    Command::new(sort_clickhouse_binary())
        .args(["--url", url, "--database", database, "--table", table])
        .output()
        .expect("Failed to execute sort-clickhouse")
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct ClickhouseRow {
    sort_key: String,
    payload: String,
}

async fn drop_table(client: &Client, table: &str) {
    client
        .query(&format!("DROP TABLE IF EXISTS {}", table))
        .execute()
        .await
        .expect("Failed to drop table");
}

async fn fetch_rows(client: &Client, table: &str) -> Vec<ClickhouseRow> {
    let query = format!("SELECT sort_key, payload FROM {} ORDER BY sort_key", table);
    let mut cursor = client
        .query(&query)
        .fetch::<ClickhouseRow>()
        .expect("Failed to query rows");

    let mut rows = Vec::new();
    while let Some(row) = cursor.next().await.expect("Failed to fetch row") {
        rows.push(row);
    }
    rows
}

// Update your helper to use these variables
fn clickhouse_client() -> Client {
    setup_env();
    let url = std::env::var("CLICKHOUSE_URL").unwrap();
    let user = std::env::var("CLICKHOUSE_USER").unwrap();
    // let pwd = std::env::var("CLICKHOUSE_PASSWORD").unwrap();
    let db = std::env::var("CLICKHOUSE_DATABASE").unwrap();

    Client::default()
        .with_url(url)
        .with_user(user)
        // .with_password(pwd)
        .with_database(db)
}

#[tokio::test]
async fn test_clickhouse_gensort_format() {
    setup_env();

    let url = clickhouse_url().unwrap();
    let database = clickhouse_database();
    let table = "clickhouse_gensort_test";
    let input_path = "testdata/test_gensort.dat";

    let client = clickhouse_client();
    drop_table(&client, table).await;

    let output = run_clickhouse_loader("gensort", input_path, &url, &database, table);
    assert!(
        output.status.success(),
        "Loader failed: stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let rows = fetch_rows(&client, table).await;
    assert_eq!(rows.len(), 3, "Expected 3 rows");

    let row0 = &rows[0];
    let row1 = &rows[1];
    let row2 = &rows[2];

    assert_eq!(row0.sort_key.as_bytes(), b"AAAAAAAAAA");
    assert_eq!(row1.sort_key.as_bytes(), b"BBBBBBBBBB");
    assert_eq!(row2.sort_key.as_bytes(), b"CCCCCCCCCC");

    assert_eq!(row0.payload.len(), 90);
    assert_eq!(row1.payload.len(), 90);
    assert_eq!(row2.payload.len(), 90);
    assert!(row0.payload.as_bytes().iter().all(|&b| b == b'1'));
    assert!(row1.payload.as_bytes().iter().all(|&b| b == b'2'));
    assert!(row2.payload.as_bytes().iter().all(|&b| b == b'3'));

    drop_table(&client, table).await;
}

#[tokio::test]
async fn test_clickhouse_kvbin_format() {
    setup_env();

    let url = clickhouse_url().unwrap();
    let database = clickhouse_database();
    let table = "clickhouse_kvbin_test";
    let input_path = "testdata/test_kvbin.dat";

    let client = Client::default().with_url(&url).with_database(&database);
    drop_table(&client, table).await;

    let output = run_clickhouse_loader("kvbin", input_path, &url, &database, table);
    assert!(
        output.status.success(),
        "Loader failed: stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let rows = fetch_rows(&client, table).await;
    assert_eq!(rows.len(), 3, "Expected 3 rows");

    let row0 = &rows[0];
    let row1 = &rows[1];
    let row2 = &rows[2];

    assert_eq!(row0.sort_key.as_bytes(), b"hello");
    assert_eq!(row0.payload.as_bytes(), b"world");
    assert_eq!(row1.sort_key.as_bytes(), b"key1");
    assert_eq!(row1.payload.as_bytes(), b"value1");
    assert_eq!(row2.sort_key.as_bytes(), b"key2");
    assert_eq!(row2.payload.as_bytes(), b"value2");

    drop_table(&client, table).await;
}

#[tokio::test]
async fn test_clickhouse_external_sort() {
    setup_env();

    let url = clickhouse_url().unwrap();
    let database = clickhouse_database();
    let input_path = "/tmp/test_clickhouse_sort_input.dat";
    let table = "clickhouse_sort_test";
    let record_count = 20usize;

    let mut records: Vec<[u8; 100]> = Vec::with_capacity(record_count);
    for i in 0..record_count {
        let mut record = [0u8; 100];
        let key = format!("{:010}", record_count - i);
        record[0..10].copy_from_slice(key.as_bytes());
        record[10] = i as u8;
        for j in 11..100 {
            record[j] = b'X';
        }
        records.push(record);
    }

    let mut file_data = Vec::with_capacity(record_count * 100);
    for record in &records {
        file_data.extend_from_slice(record);
    }
    fs::write(input_path, &file_data).expect("Failed to write test file");

    let client = Client::default().with_url(&url).with_database(&database);
    drop_table(&client, table).await;

    let output = run_clickhouse_loader("gensort", input_path, &url, &database, table);
    assert!(
        output.status.success(),
        "Loader failed: stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let output = run_clickhouse_sorter(&url, &database, table);
    assert!(
        output.status.success(),
        "Sorter failed: stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("TIMING:"),
        "Expected TIMING output, got: {}",
        stdout
    );

    // Verify the data is correctly sorted by fetching from database
    let rows = fetch_rows(&client, table).await;
    assert_eq!(rows.len(), record_count, "Expected {} rows", record_count);
    for i in 1..rows.len() {
        let prev = rows[i - 1].sort_key.as_bytes();
        let next = rows[i].sort_key.as_bytes();
        assert!(
            prev <= next,
            "Keys not sorted at index {}: {:?} > {:?}",
            i,
            prev,
            next
        );
    }

    let _ = fs::remove_file(input_path);
    drop_table(&client, table).await;
}
