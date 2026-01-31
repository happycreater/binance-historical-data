use anyhow::{Context, Result};
use polars::prelude::*;
use quick_xml::events::Event;
use quick_xml::Reader;
use rayon::prelude::*;
use regex::Regex;
use reqwest::blocking::{Client, ClientBuilder};
use reqwest::Proxy;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use urlencoding::encode;
use ::zip::ZipArchive;

const BASE_URL: &str = "https://data.binance.vision";
const CLEAN_ROOT: &str = "parquet.binance.vision";

fn wildcard_match(text: &str, pattern: &str) -> bool {
    let escaped = regex::escape(pattern);
    let regex_pattern = format!(
        "^{}$",
        escaped.replace(r"\*", ".*").replace(r"\?", ".")
    );
    Regex::new(&regex_pattern)
        .map(|re| re.is_match(text))
        .unwrap_or(false)
}

fn get_bucket_url_with_base(client: &Client, base_url: &str, prefix: &str) -> Result<String> {
    let listing_url = format!("{}/?prefix={}", base_url, encode(prefix));
    let html = client.get(listing_url).send()?.text()?;
    let re = Regex::new(r"var BUCKET_URL = '(.*?)';")?;
    let caps = re
        .captures(&html)
        .context("BUCKET_URL not found in index page")?;
    Ok(caps.get(1).context("BUCKET_URL missing")?.as_str().to_string())
}

fn parse_listing(prefix: &str, xml_content: &str) -> Result<(Vec<(String, bool)>, bool, Option<String>)> {
    let mut entries: Vec<(String, bool)> = Vec::new();
    let mut reader = Reader::from_str(xml_content);
    reader.trim_text(true);
    let mut buf = Vec::new();
    let mut current_tag = String::new();
    let mut is_truncated = false;
    let mut next_marker: Option<String> = None;
    let mut last_key: Option<String> = None;
    let mut in_common_prefix = false;
    while let Ok(event) = reader.read_event_into(&mut buf) {
        match event {
            Event::Start(e) => {
                current_tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if current_tag.ends_with("CommonPrefixes") {
                    in_common_prefix = true;
                }
            }
            Event::End(e) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if tag.ends_with("CommonPrefixes") {
                    in_common_prefix = false;
                }
            }
            Event::Text(e) => {
                let text = e.unescape()?.to_string();
                if current_tag.ends_with("Prefix") && in_common_prefix {
                    let name = text.trim_start_matches(prefix).trim_matches('/');
                    if !name.is_empty() {
                        entries.push((name.to_string(), true));
                    }
                } else if current_tag.ends_with("Key") {
                    last_key = Some(text.clone());
                    if text.ends_with(".zip") {
                        let name = text.trim_start_matches(prefix);
                        if !name.is_empty() {
                            entries.push((name.to_string(), false));
                        }
                    }
                } else if current_tag.ends_with("IsTruncated") {
                    is_truncated = text.to_lowercase() == "true";
                } else if current_tag.ends_with("NextMarker") {
                    next_marker = Some(text);
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    entries.sort_by_key(|entry| (!entry.1, entry.0.clone()));
    let continuation = if is_truncated {
        next_marker.or(last_key)
    } else {
        None
    };
    Ok((entries, is_truncated, continuation))
}

fn list_prefix_with_base(client: &Client, base_url: &str, prefix: &str) -> Result<Vec<(String, bool)>> {
    let bucket_url = get_bucket_url_with_base(client, base_url, prefix)?;
    let mut entries: Vec<(String, bool)> = Vec::new();
    let mut continuation: Option<String> = None;

    loop {
        let mut params = format!("delimiter=/&prefix={}", encode(prefix));
        if let Some(marker) = &continuation {
            params.push_str(&format!("&marker={}", encode(marker)));
        }
        let request_url = format!("{}?{}", bucket_url, params);
        let xml_content = client.get(request_url).send()?.text()?;
        let (mut batch, is_truncated, next_marker) = parse_listing(prefix, &xml_content)?;
        entries.append(&mut batch);

        if !is_truncated {
            break;
        }
        continuation = next_marker;
        if continuation.is_none() {
            break;
        }
    }

    entries.sort_by_key(|entry| (!entry.1, entry.0.clone()));
    Ok(entries)
}

fn encoded_url(path: &str, file_name: &str) -> String {
    let encoded_path = encode(path).replace("%2F", "/");
    let encoded_name = encode(file_name);
    format!("{}/{}/{}", BASE_URL, encoded_path.trim_end_matches('/'), encoded_name)
}

fn download_one(client: &Client, url: &str, chunk_bytes: usize) -> Result<Vec<u8>> {
    let mut response = client.get(url).send()?.error_for_status()?;
    let mut buffer = vec![0u8; chunk_bytes];
    let mut output: Vec<u8> = Vec::new();
    loop {
        let read = response.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        output.extend_from_slice(&buffer[..read]);
    }
    Ok(output)
}

fn has_header(csv_content: &str) -> bool {
    let first_line = csv_content.lines().next().unwrap_or("");
    let first_cell = first_line.split(',').next().unwrap_or("");
    first_cell.parse::<f64>().is_err()
}

fn normalize_frame(df: DataFrame) -> Result<DataFrame> {
    let first_column = df
        .get_column_names()
        .first()
        .context("dataframe has no columns")?
        .to_string();
    let normalized = df
        .lazy()
        .unique(None, UniqueKeepStrategy::First)
        .sort([first_column.clone()], SortMultipleOptions::default())
        .collect()?;
    Ok(normalized)
}

fn clean_zip_bytes(zip_bytes: &[u8], pattern: &str, symbol: &str) -> Result<()> {
    let cursor = Cursor::new(zip_bytes);
    let mut archive = ZipArchive::new(cursor)?;
    let mut zipped = archive.by_index(0)?;
    let mut csv_content = String::new();
    zipped.read_to_string(&mut csv_content)?;

    let mut df = CsvReadOptions::default()
        .with_has_header(has_header(&csv_content))
        .into_reader_with_file_handle(Cursor::new(csv_content.as_bytes()))
        .finish()
        .context("parse csv")?;

    df.with_column(Series::new("pattern", vec![pattern; df.height()]))?;
    df.with_column(Series::new("symbol", vec![symbol; df.height()]))?;
    let mut df = normalize_frame(df)?;

    let out_dir = PathBuf::from(CLEAN_ROOT)
        .join(pattern)
        .join(format!("symbol={}", symbol));
    fs::create_dir_all(&out_dir)?;
    let out_path = out_dir.join("data.parquet");
    if out_path.exists() {
        let existing = LazyFrame::scan_parquet(&out_path, Default::default())?;
        let combined = concat(
            [existing, df.lazy()],
            UnionArgs {
                parallel: true,
                rechunk: true,
                ..Default::default()
            },
        )?
        .collect()?;
        let mut combined = normalize_frame(combined)?;
        let mut file = fs::File::create(&out_path)?;
        ParquetWriter::new(&mut file).finish(&mut combined)?;
    } else {
        let mut file = fs::File::create(&out_path)?;
        ParquetWriter::new(&mut file).finish(&mut df)?;
    }

    Ok(())
}

fn processed_path(pattern: &str) -> PathBuf {
    PathBuf::from(CLEAN_ROOT).join(pattern).join("processed.txt")
}

fn load_processed_urls(path: &PathBuf) -> Result<HashSet<String>> {
    if !path.exists() {
        return Ok(HashSet::new());
    }
    let contents = fs::read_to_string(path)?;
    let mut urls = HashSet::new();
    for line in contents.lines() {
        for token in line.split_whitespace() {
            let trimmed = token.trim();
            if !trimmed.is_empty() {
                urls.insert(trimmed.to_string());
            }
        }
    }
    Ok(urls)
}

fn open_processed_writer(path: &PathBuf) -> Result<Arc<Mutex<fs::File>>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    Ok(Arc::new(Mutex::new(file)))
}

fn record_processed(writer: &Arc<Mutex<fs::File>>, url: &str) -> Result<()> {
    use std::io::Write;
    let mut handle = writer.lock().expect("processed writer lock");
    if let Some(file_name) = extract_zip_name(url) {
        writeln!(handle, "{} {}", url, file_name)?;
    } else {
        writeln!(handle, "{}", url)?;
    }
    Ok(())
}

fn extract_zip_name(url: &str) -> Option<String> {
    let trimmed = url.split('?').next().unwrap_or(url);
    trimmed.rsplit('/').next().map(|name| name.to_string())
}

fn build_listing_client(proxy_url: Option<&str>) -> Result<Client> {
    let mut builder = ClientBuilder::new();
    if let Some(proxy_url) = proxy_url {
        builder = builder.proxy(Proxy::all(proxy_url)?);
    }
    builder.build().context("listing client build")
}

fn build_urls(
    listing_client: &Client,
    pattern: &str,
    symbol_glob: &str,
) -> Result<HashMap<String, Vec<String>>> {
    build_urls_with_base(listing_client, BASE_URL, pattern, symbol_glob)
}

fn build_urls_with_base(
    listing_client: &Client,
    base_url: &str,
    pattern: &str,
    symbol_glob: &str,
) -> Result<HashMap<String, Vec<String>>> {
    let endpoint = pattern.split("SYMBOL").next().unwrap_or("");
    let entries = list_prefix_with_base(listing_client, base_url, endpoint)?;
    let symbols: Vec<String> = entries
        .iter()
        .filter(|entry| entry.1 && wildcard_match(&entry.0, symbol_glob))
        .map(|entry| entry.0.clone())
        .collect();

    let mut urls: HashMap<String, Vec<String>> = HashMap::new();
    for symbol in symbols {
        let path = pattern.replace("SYMBOL", &symbol);
        let all_zip = list_prefix_with_base(listing_client, base_url, &path)?;
        for entry in all_zip {
            if !entry.1 {
                let url = encoded_url(&path, &entry.0);
                urls.entry(symbol.clone()).or_default().push(url);
            }
        }
    }
    Ok(urls)
}

fn main() -> Result<()> {
    let pattern = env::var("BINANCE_PATTERN")
        .unwrap_or_else(|_| "data/spot/daily/klines/SYMBOL/1m/".to_string());
    let symbol_glob = env::var("BINANCE_SYMBOL_GLOB").unwrap_or_else(|_| "*USDT".to_string());
    let chunk_bytes: usize = env::var("BINANCE_DOWNLOAD_CHUNK_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024 * 1024);
    let listing_proxy = env::var("BINANCE_S3_PROXY").ok();

    println!(
        "Starting download: pattern={}, symbol_glob={}, chunk_bytes={}, listing_proxy={}",
        pattern,
        symbol_glob,
        chunk_bytes,
        listing_proxy.as_deref().unwrap_or("none")
    );

    let download_client = Client::builder().build().context("build download client")?;
    let listing_client = build_listing_client(listing_proxy.as_deref())
        .context("build listing client")?;
    println!("Listing symbols and files from Binance...");
    let urls = build_urls(&listing_client, &pattern, &symbol_glob)?;
    let total_symbols = urls.len();
    let total_urls: usize = urls.values().map(|entries| entries.len()).sum();
    println!(
        "Discovered {} symbols with {} files.",
        total_symbols, total_urls
    );

    let meta_info_path = processed_path(&pattern);
    let processed_urls = load_processed_urls(&meta_info_path)?;
    println!(
        "Loaded {} processed entries for incremental download.",
        processed_urls.len()
    );

    let mut pending_urls: HashMap<String, Vec<String>> = HashMap::new();
    let mut skipped = 0usize;
    for (symbol, symbol_urls) in urls {
        for url in symbol_urls {
            let file_name = extract_zip_name(&url);
            let already_processed = processed_urls.contains(&url)
                || file_name
                    .as_ref()
                    .map(|name| processed_urls.contains(name))
                    .unwrap_or(false);
            if already_processed {
                skipped += 1;
            } else {
                pending_urls.entry(symbol.clone()).or_default().push(url);
            }
        }
    }
    let pending_count: usize = pending_urls.values().map(|entries| entries.len()).sum();
    println!(
        "Pending downloads: {} (skipped: {}).",
        pending_count, skipped
    );
    let processed_writer = open_processed_writer(&meta_info_path)?;
    let downloaded = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);

    pending_urls.par_iter().for_each(|(symbol, symbol_urls)| {
        println!("Processing symbol {} with {} files.", symbol, symbol_urls.len());
        for url in symbol_urls {
            println!("Downloading {}", url);
            match download_one(&download_client, url, chunk_bytes) {
                Ok(zip_bytes) => {
                    if clean_zip_bytes(&zip_bytes, &pattern, symbol).is_ok() {
                        if record_processed(&processed_writer, url).is_ok() {
                            println!("Processed {}", url);
                        }
                        downloaded.fetch_add(1, Ordering::Relaxed);
                    } else {
                        println!("Failed to process {}", url);
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    println!("Failed to download {}", url);
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });

    println!(
        "Processed: {}, Failed: {}, Skipped: {}",
        downloaded.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        skipped
    );

    Ok(())
}

#[allow(dead_code)]
fn sequential_download_and_clean(
    client: &Client,
    urls: &HashMap<String, Vec<String>>,
    pattern: &str,
    chunk_bytes: usize,
) -> Result<()> {
    let meta_info_path = processed_path(pattern);
    let processed_urls = load_processed_urls(&meta_info_path)?;
    let processed_writer = open_processed_writer(&meta_info_path)?;
    let downloaded = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);
    let mut skipped = 0usize;
    for (symbol, symbol_urls) in urls {
        for url in symbol_urls {
            let file_name = extract_zip_name(url);
            let already_processed = processed_urls.contains(url)
                || file_name
                    .as_ref()
                    .map(|name| processed_urls.contains(name))
                    .unwrap_or(false);
            if already_processed {
                skipped += 1;
                continue;
            }
            match download_one(client, url, chunk_bytes) {
                Ok(zip_bytes) => {
                    clean_zip_bytes(&zip_bytes, pattern, symbol)?;
                    record_processed(&processed_writer, url)?;
                    downloaded.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
    println!(
        "Processed: {}, Failed: {}, Skipped: {}",
        downloaded.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        skipped
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::thread;

    fn serve_once(listener: TcpListener, handler: Arc<dyn Fn(String) -> String + Send + Sync>) -> String {
        let addr = listener.local_addr().expect("addr");
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                handle_stream(stream, handler.clone());
            }
        });
        format!("http://{}", addr)
    }

    fn handle_stream(mut stream: TcpStream, handler: Arc<dyn Fn(String) -> String + Send + Sync>) {
        let mut buffer = [0u8; 2048];
        let read = stream.read(&mut buffer).unwrap_or(0);
        let request = String::from_utf8_lossy(&buffer[..read]).to_string();
        let path_line = request.lines().next().unwrap_or_default();
        let path = path_line
            .split_whitespace()
            .nth(1)
            .unwrap_or("/")
            .to_string();
        let response_body = handler(path);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        let _ = stream.write_all(response.as_bytes());
    }

    #[test]
    fn detects_header() {
        let csv_with_header = "open_time,open,high\n1,2,3\n";
        let csv_without_header = "1,2,3\n4,5,6\n";
        assert!(has_header(csv_with_header));
        assert!(!has_header(csv_without_header));
    }

    #[test]
    fn matches_wildcards() {
        assert!(wildcard_match("BTCUSDT", "*USDT"));
        assert!(wildcard_match("ETHBTC", "ETH*"));
        assert!(!wildcard_match("BNBUSDT", "BTC*"));
    }

    #[test]
    fn parses_listing_entries() {
        let prefix = "data/spot/daily/klines/SYMBOL/1m/";
        let xml = r#"
            <ListBucketResult>
              <CommonPrefixes><Prefix>data/spot/daily/klines/SYMBOL/1m/BTCUSDT/</Prefix></CommonPrefixes>
              <Contents><Key>data/spot/daily/klines/SYMBOL/1m/BTCUSDT/BTCUSDT-1m-2024-01-01.zip</Key></Contents>
              <IsTruncated>false</IsTruncated>
            </ListBucketResult>
        "#;
        let (entries, truncated, next_marker) = parse_listing(prefix, xml).unwrap();
        assert!(!truncated);
        assert!(next_marker.is_none());
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "BTCUSDT");
        assert!(entries[0].1);
    }

    #[test]
    fn encodes_url() {
        let url = encoded_url("data/spot/daily/klines/SYMBOL/1m/", "BTCUSDT-1m-2024-01-01.zip");
        assert!(url.contains("data/spot/daily/klines/SYMBOL/1m/BTCUSDT-1m-2024-01-01.zip"));
    }

    #[test]
    fn normalizes_frames() {
        let df = df![
            "open_time" => [2i64, 1i64, 1i64],
            "price" => [10i64, 20i64, 20i64]
        ]
        .unwrap();
        let normalized = normalize_frame(df).unwrap();
        let times = normalized.column("open_time").unwrap().i64().unwrap();
        assert_eq!(times.get(0), Some(1));
        assert_eq!(times.len(), 2);
    }

    #[test]
    fn loads_processed_urls_from_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("processed.txt");
        fs::write(&path, "http://example.com/a file-a.zip\nnot-a-url\nhttp://example.com/b\n").unwrap();
        let urls = load_processed_urls(&path).unwrap();
        assert!(urls.contains("http://example.com/a"));
        assert!(urls.contains("file-a.zip"));
        assert!(urls.contains("http://example.com/b"));
        assert_eq!(urls.len(), 4);
    }

    #[test]
    fn records_processed_url() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("processed.txt");
        let writer = open_processed_writer(&path).unwrap();
        record_processed(&writer, "http://example.com/a.zip").unwrap();
        record_processed(&writer, "http://example.com/b").unwrap();
        let contents = fs::read_to_string(&path).unwrap();
        assert!(contents.contains("http://example.com/a.zip"));
        assert!(contents.contains("a.zip"));
        assert!(contents.contains("http://example.com/b"));
    }

    #[test]
    fn extracts_zip_name_from_url() {
        let name = extract_zip_name("http://example.com/path/data.zip?foo=bar").unwrap();
        assert_eq!(name, "data.zip");
    }

    #[test]
    fn builds_listing_client_with_proxy() {
        let client = build_listing_client(Some("http://127.0.0.1:1234")).unwrap();
        let url = encoded_url("data/spot/daily/klines/SYMBOL/1m/", "BTCUSDT-1m-2024-01-01.zip");
        assert!(client.get(url).build().is_ok());
    }

    #[test]
    fn downloads_from_local_server() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = serve_once(listener, Arc::new(|_path| "zip-bytes".to_string()));
        let client = ClientBuilder::new().no_proxy().build().unwrap();
        let bytes = download_one(&client, &format!("{}/file.zip", base_url), 4).unwrap();
        assert_eq!(bytes, b"zip-bytes");
    }

    #[test]
    fn gets_bucket_url_from_listing_page() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let handler = Arc::new(move |path: String| {
            if path.starts_with("/?prefix=") {
                format!("var BUCKET_URL = '{}/bucket';", base_url)
            } else {
                "".to_string()
            }
        });
        let base_url = serve_once(listener, handler);
        let client = ClientBuilder::new().no_proxy().build().unwrap();
        let prefix = "data/spot/";
        let url = get_bucket_url_with_base(&client, &base_url, prefix).unwrap();
        assert_eq!(url, format!("{}/bucket", base_url));
    }

    #[test]
    fn lists_prefix_entries() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let handler = Arc::new(move |path: String| {
            if path.starts_with("/?prefix=") {
                format!("var BUCKET_URL = '{}/bucket';", base_url)
            } else if path.starts_with("/bucket") {
                r#"<ListBucketResult>
                        <CommonPrefixes><Prefix>data/spot/daily/klines/SYMBOL/1m/BTCUSDT/</Prefix></CommonPrefixes>
                        <Contents><Key>data/spot/daily/klines/SYMBOL/1m/BTCUSDT/BTCUSDT-1m-2024-01-01.zip</Key></Contents>
                        <IsTruncated>false</IsTruncated>
                    </ListBucketResult>"#
                    .to_string()
            } else {
                "".to_string()
            }
        });
        let base_url = serve_once(listener, handler);
        let client = ClientBuilder::new().no_proxy().build().unwrap();
        let prefix = "data/spot/daily/klines/SYMBOL/1m/";
        let entries = list_prefix_with_base(&client, &base_url, prefix).unwrap_or_default();
        assert!(!entries.is_empty());
    }

    #[test]
    fn builds_urls_from_listing() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let listing_page = format!("var BUCKET_URL = '{}/bucket';", base_url);
        let symbols_xml = r#"<ListBucketResult>
                <CommonPrefixes><Prefix>data/spot/daily/klines/SYMBOL/1m/BTCUSDT/</Prefix></CommonPrefixes>
                <IsTruncated>false</IsTruncated>
            </ListBucketResult>"#
            .to_string();
        let zips_xml = r#"<ListBucketResult>
                <Contents><Key>data/spot/daily/klines/SYMBOL/1m/BTCUSDT/BTCUSDT-1m-2024-01-01.zip</Key></Contents>
                <IsTruncated>false</IsTruncated>
            </ListBucketResult>"#
            .to_string();
        let handler = Arc::new(move |path: String| {
            if path.starts_with("/?prefix=") {
                listing_page.clone()
            } else if path.starts_with("/bucket") {
                if path.contains("BTCUSDT") {
                    zips_xml.clone()
                } else {
                    symbols_xml.clone()
                }
            } else {
                "".to_string()
            }
        });
        let base_url = serve_once(listener, handler);
        let client = ClientBuilder::new().no_proxy().build().unwrap();
        let pattern = "data/spot/daily/klines/SYMBOL/1m/";
        let urls = build_urls_with_base(&client, &base_url, pattern, "*USDT").unwrap_or_default();
        assert!(urls.values().flatten().any(|url| url.contains("BTCUSDT-1m-2024-01-01.zip")));
    }
}
