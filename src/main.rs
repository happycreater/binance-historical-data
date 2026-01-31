use anyhow::{Context, Result};
use downloader::{download_one, encoded_url, list_prefix, wildcard_match};
use rayon::prelude::*;
use reqwest::blocking::Client;
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};

mod cleaner;
mod downloader;

fn build_urls(
    client: &Client,
    pattern: &str,
    symbol_glob: &str,
) -> Result<Vec<(String, String)>> {
    let endpoint = pattern.split("SYMBOL").next().unwrap_or("");
    let entries = list_prefix(client, endpoint)?;
    let symbols: Vec<String> = entries
        .iter()
        .filter(|entry| entry.is_dir && wildcard_match(&entry.name, symbol_glob))
        .map(|entry| entry.name.clone())
        .collect();

    let mut urls = Vec::new();
    for symbol in symbols {
        let path = pattern.replace("SYMBOL", &symbol);
        let all_zip = list_prefix(client, &path)?;
        for entry in all_zip {
            if !entry.is_dir {
                urls.push((symbol.clone(), encoded_url(&path, &entry.name)));
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

    urls.par_iter().for_each(|(symbol, url)| {
        match download_one(&client, url, chunk_bytes) {
            Ok(zip_bytes) => {
                if cleaner::clean_zip_bytes(&zip_bytes, &pattern, symbol).is_ok() {
                    downloaded.fetch_add(1, Ordering::Relaxed);
                } else {
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                failed.fetch_add(1, Ordering::Relaxed);
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
    urls: &[(String, String)],
    pattern: &str,
    chunk_bytes: usize,
) -> Result<()> {
    let downloaded = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);
    for (symbol, url) in urls {
        match download_one(client, url, chunk_bytes) {
            Ok(zip_bytes) => {
                cleaner::clean_zip_bytes(&zip_bytes, pattern, symbol)?;
                downloaded.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                failed.fetch_add(1, Ordering::Relaxed);
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
