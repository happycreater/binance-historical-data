use anyhow::{Context, Result};
use quick_xml::events::Event;
use quick_xml::Reader;
use regex::Regex;
use reqwest::blocking::Client;
use std::io::Read;
use urlencoding::encode;

const BASE_URL: &str = "https://data.binance.vision";

pub fn wildcard_match(text: &str, pattern: &str) -> bool {
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

pub fn list_prefix(client: &Client, prefix: &str) -> Result<Vec<(String, bool)>> {
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
        let mut reader = Reader::from_str(&xml_content);
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

        if !is_truncated {
            break;
        }
        continuation = next_marker.or(last_key);
        if continuation.is_none() {
            break;
        }
    }

    entries.sort_by_key(|entry| (!entry.1, entry.0.clone()));
    Ok(entries)
}

pub fn download_one(
    client: &Client,
    url: &str,
    chunk_bytes: usize,
) -> Result<Vec<u8>> {
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

pub fn encoded_url(path: &str, file_name: &str) -> String {
    let encoded_path = encode(path).replace("%2F", "/");
    let encoded_name = encode(file_name);
    format!("{}/{}/{}", BASE_URL, encoded_path.trim_end_matches('/'), encoded_name)
}
