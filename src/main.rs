use anyhow::{Context, Result};
use polars::prelude::*;
use quick_xml::events::Event;
use quick_xml::Reader;
use rayon::prelude::*;
use regex::Regex;
use reqwest::blocking::Client;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Cursor, Read};
use std::path::PathBuf;
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

fn get_bucket_url(client: &Client, prefix: &str) -> Result<String> {
    let listing_url = format!("{}/?prefix={}", BASE_URL, encode(prefix));
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

fn list_prefix(client: &Client, prefix: &str) -> Result<Vec<(String, bool)>> {
    let bucket_url = get_bucket_url(client, prefix)?;
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

    let meta_info_path = PathBuf::from(CLEAN_ROOT).join(pattern).join("processed.txt");
    fs::create_dir_all(meta_info_path.parent().unwrap())?;
    let mut meta_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(meta_info_path)?;
    use std::io::Write;
    writeln!(meta_file, "{}:{}", symbol, pattern)?;
    Ok(())
}

fn build_urls(
    client: &Client,
    pattern: &str,
    symbol_glob: &str,
) -> Result<HashMap<String, Vec<String>>> {
    let endpoint = pattern.split("SYMBOL").next().unwrap_or("");
    let entries = list_prefix(client, endpoint)?;
    let symbols: Vec<String> = entries
        .iter()
        .filter(|entry| entry.1 && wildcard_match(&entry.0, symbol_glob))
        .map(|entry| entry.0.clone())
        .collect();

    let mut urls: HashMap<String, Vec<String>> = HashMap::new();
    for symbol in symbols {
        let path = pattern.replace("SYMBOL", &symbol);
        let all_zip = list_prefix(client, &path)?;
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

    let client = Client::builder().build().context("build client")?;
    let urls = build_urls(&client, &pattern, &symbol_glob)?;
    let downloaded = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);

    urls.par_iter().for_each(|(symbol, symbol_urls)| {
        for url in symbol_urls {
            match download_one(&client, url, chunk_bytes) {
                Ok(zip_bytes) => {
                    if clean_zip_bytes(&zip_bytes, &pattern, symbol).is_ok() {
                        downloaded.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });

    println!(
        "Processed: {}, Failed: {}",
        downloaded.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed)
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
    let downloaded = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);
    for (symbol, symbol_urls) in urls {
        for url in symbol_urls {
            match download_one(client, url, chunk_bytes) {
                Ok(zip_bytes) => {
                    clean_zip_bytes(&zip_bytes, pattern, symbol)?;
                    downloaded.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
    println!(
        "Processed: {}, Failed: {}",
        downloaded.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed)
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_header() {
        let csv_with_header = "open_time,open,high\n1,2,3\n";
        let csv_without_header = "1,2,3\n4,5,6\n";
        assert!(has_header(csv_with_header));
        assert!(!has_header(csv_without_header));
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
}
