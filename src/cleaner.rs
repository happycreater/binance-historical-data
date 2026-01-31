use anyhow::{Context, Result};
use polars::functions::concat;
use polars::prelude::*;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::fs::OpenOptions;
use std::io::Cursor;
use zip::ZipArchive;

const CLEAN_ROOT: &str = "parquet.binance.vision";

pub fn clean_zip_bytes(zip_bytes: &[u8], pattern: &str, symbol: &str) -> Result<()> {
    let cursor = Cursor::new(zip_bytes);
    let mut archive = ZipArchive::new(cursor)?;
    let mut zipped = archive.by_index(0)?;
    let mut csv_content = String::new();
    zipped.read_to_string(&mut csv_content)?;

    let first_line = csv_content.lines().next().unwrap_or("");
    let first_cell = first_line.split(',').next().unwrap_or("");
    let has_header = first_cell.parse::<f64>().is_err();
    let mut reader = CsvReader::new(csv_content.as_bytes());
    let mut df = reader
        .has_header(has_header)
        .finish()
        .context("parse csv")?;

    df.with_column(Series::new("pattern", vec![pattern; df.height()]))?;
    df.with_column(Series::new("symbol", vec![symbol; df.height()]))?;

    let out_dir = PathBuf::from(CLEAN_ROOT)
        .join(pattern)
        .join(format!("symbol={}", symbol));
    fs::create_dir_all(&out_dir)?;
    let out_path = out_dir.join("data.parquet");
    if out_path.exists() {
        let existing = LazyFrame::scan_parquet(&out_path, Default::default())?;
        let combined = concat([existing, df.lazy()], true, true)?.collect()?;
        let mut file = fs::File::create(&out_path)?;
        ParquetWriter::new(&mut file).finish(&combined)?;
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
