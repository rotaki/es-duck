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

fn run_postgres_sorter(
    db_url: &str,
    output: &str,
    table: &str,
    work_mem: &str,
) -> std::process::Output {
    Command::new(sort_postgres_binary())
        .args([
            "--db",
            db_url,
            "--output",
            output,
            "--table",
            table,
            "--total-memory",
            work_mem,
        ])
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
    use std::io::{self, Read};

    let Some(db_url) = postgres_url() else {
        eprintln!("skipping test_postgres_external_sort; POSTGRES_TEST_URL not set");
        return;
    };

    let input_path = "/tmp/test_pg_sort_input.dat";
    let output_path = "/tmp/test_pg_sort_output.bin";
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

    // Clean up any existing table and output file
    {
        let mut client = Client::connect(&db_url, NoTls).expect("Failed to connect to Postgres");
        let _ = client.batch_execute(&format!("DROP TABLE IF EXISTS {}", table));
    }
    let _ = fs::remove_file(output_path);

    // Load data into PostgreSQL
    let output = run_postgres_loader("gensort", input_path, &db_url, table);
    assert!(
        output.status.success(),
        "Loader failed: stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Run external sort
    let output = run_postgres_sorter(&db_url, output_path, table, "64MB");
    assert!(
        output.status.success(),
        "Sorter failed: stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Read and parse the PostgreSQL binary COPY format output
    if !std::path::Path::new(output_path).exists() {
        panic!(
            "Output file does not exist: {}\n\
             If using Docker, ensure /tmp is mounted: -v /tmp:/tmp\n\
             If using local PostgreSQL, ensure the PostgreSQL user has write access to /tmp",
            output_path
        );
    }
    let sorted_keys =
        parse_postgres_binary_copy(output_path).expect("Failed to parse binary copy output");

    assert_eq!(sorted_keys.len(), 100, "Expected 100 rows in output");

    // Verify the output is sorted by comparing adjacent keys
    for i in 1..sorted_keys.len() {
        let prev_key = &sorted_keys[i - 1];
        let curr_key = &sorted_keys[i];
        assert!(
            prev_key <= curr_key,
            "Output not sorted at index {}: {:?} > {:?}",
            i,
            prev_key,
            curr_key
        );
    }

    // Verify first key is smallest and last key is largest
    let min_key = sorted_keys.iter().min().unwrap();
    let max_key = sorted_keys.iter().max().unwrap();
    assert_eq!(
        &sorted_keys[0], min_key,
        "First row should have smallest key"
    );
    assert_eq!(
        &sorted_keys[99], max_key,
        "Last row should have largest key"
    );

    // Clean up
    let _ = fs::remove_file(input_path);
    let _ = fs::remove_file(output_path);
    {
        let mut client = Client::connect(&db_url, NoTls).expect("Failed to connect to Postgres");
        let _ = client.batch_execute(&format!("DROP TABLE IF EXISTS {}", table));
    }
}

/// Parse PostgreSQL binary COPY format and extract sort_key values
fn parse_postgres_binary_copy(path: &str) -> std::io::Result<Vec<Vec<u8>>> {
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    let mut keys = Vec::new();
    let mut pos = 0;

    // Binary COPY header: "PGCOPY\n\xff\r\n\0" (11 bytes) + flags (4 bytes) + header extension (4 bytes)
    // Total header: 19 bytes minimum
    if data.len() < 19 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "File too short for binary COPY header",
        ));
    }

    // Verify signature "PGCOPY\n\xff\r\n\0"
    let signature = b"PGCOPY\n\xff\r\n\0";
    if &data[0..11] != signature {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid PGCOPY signature",
        ));
    }
    pos = 11;

    // Skip flags (4 bytes)
    pos += 4;

    // Read header extension length (4 bytes, big-endian)
    let ext_len =
        i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
    pos += 4;

    // Skip header extension
    pos += ext_len;

    // Read tuples
    while pos < data.len() {
        // Read field count (2 bytes, big-endian) - -1 indicates file trailer
        if pos + 2 > data.len() {
            break;
        }
        let field_count = i16::from_be_bytes([data[pos], data[pos + 1]]);
        pos += 2;

        if field_count == -1 {
            // File trailer
            break;
        }

        // Read each field
        for field_idx in 0..field_count {
            if pos + 4 > data.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Unexpected end of data",
                ));
            }

            // Field length (4 bytes, big-endian) - -1 indicates NULL
            let field_len =
                i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;

            if field_len == -1 {
                // NULL value
                continue;
            }

            let field_len = field_len as usize;
            if pos + field_len > data.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Field length exceeds data",
                ));
            }

            // First field is sort_key
            if field_idx == 0 {
                keys.push(data[pos..pos + field_len].to_vec());
            }

            pos += field_len;
        }
    }

    Ok(keys)
}
