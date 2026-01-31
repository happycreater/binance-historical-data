# Binance Historical Data Collector

A CLI utility to easily download large amounts of historical trading data from [Binance](https://www.binance.com/en). Each file is verified with the checksum.

## What's New

**v1.2.0**
- Downloaded files are now saved to directories that mirror their Binance URL path (excluding the hostname).
- Before downloading, the tool now checks if a file already exists and will skip the download if present. Skipped files are reported in the summary.

## Why

Binance offers two methods to access the historical data: through [their API](https://binance-docs.github.io/apidocs) in `JSON` format or [this webpage](https://www.binance.com/en/landing/data) in `CSV` format. It's impossible to *quickly* get historical data for data types such as `trades` and `aggTrades` with the first method, and it would still require some manual labor to fetch a lot of files with the second method. 

This library allows to collect data in `CSV` format for any date range, any number of symbols and intervals (if present) with a single command.

## Installation

Install globally:

```shell
npm i -g binance-historical-data
```

Run: 

```shell
binance-fetch --help
```

Or install locally:

```shell
npm i binance-historical-data
```

And run:

```shell
npx binance-fetch --help
```

## Rust 重构版本

本仓库已提供 Rust 实现的高性能下载/清洗入口（用于替代 Python 流程以提升速度）。使用方式：

```shell
cargo build --release
BINANCE_PATTERN="data/spot/daily/klines/SYMBOL/1m/" \
BINANCE_SYMBOL_GLOB="*USDT" \
BINANCE_DOWNLOAD_WORKERS=32 \
BINANCE_DOWNLOAD_CHUNK_BYTES=1048576 \
./target/release/binance-fast
```

Rust 版流程不会保留 ZIP 文件，仅保留清洗后的 Parquet 结果。

## Usage

### Trading data params

Download `daily` `klines` data for `spot` market:

```shell
binance-fetch -d 2020-01-01 -p spot -t klines -s btcusdt -i 1h
```

If `--symbols (-s)` is not provided, the CLI will fetch all available symbols for the selected product using the Binance API. You can also use wildcards such as `*USDT` to download all symbols matching the pattern:

```shell
binance-fetch -d 2024-01-01 -p spot -t klines -s "*USDT" -i 1h
```

`YYYY-MM-DD` date format is used for `daily` data. Use `YYYY-MM` for `monthly` data.

To get data for a range of `dates`, provide two `date` strings separated by a space. Multiple `symbols` and `intervals` can also be provided separated by a space.

```shell
binance-fetch -d 2021-01 2023-12 -p spot -t klines -s btcusdt ethusdt -i 1s 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1mo
```

This command downloads `monthly` data for two `symbols` and all `intervals` from `2021-01` to `2023-12` (3 years), which will result in 1152 downloaded files.

更多路径速查见：`docs/data_binance_vision_paths.md`。

#### Possible values

##### `--product (-p)`

- spot
- usd-m
- coin-m
- option

##### `--data-type (-t)` (spot)

- klines
- aggTrades
- trades

##### `--data-type (-t)` (usd-m/coin-m monthly)

- aggTrades
- bookTicker
- fundingRate
- indexPriceKlines
- klines
- markPriceKlines
- premiumIndexKlines
- trades

##### `--data-type (-t)` (usd-m/coin-m daily)

- aggTrades
- bookDepth
- bookTicker
- indexPriceKlines
- klines
- liquidationSnapshot
- markPriceKlines
- metrics
- premiumIndexKlines
- trades

##### `--data-type (-t)` (option)

- BVOLIndex
- EOHSummary

##### `--intervals (-i)`

1s 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1mo.

### Output directory

By default, all data is saved mirroring the Binance URL relative to your current directory. For example, a file from:
```
https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2021-01-01.zip
```
will be saved to:
```
./data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2021-01-01.zip
```

You may pass `-o` or `--output` followed by a relative or absolute path to change the root directory. If a file already exists at the expected path, it will be skipped.

Data is loaded with a stream. Until the file is fully downloaded and verified, it will look like this: `<symbol>...UNVERIFIED.zip`.

To reduce 404s, the downloader attempts to fetch the available ZIP listings for the requested symbol paths from `data.binance.vision` and caches them under `.binance-index-cache` in the output directory. Use `--no-remote-index` to disable this behavior and fall back to URL probing.

### Concurrency

By default 5 files are downloaded at a time. Use `-P` to change the number (pass `-P 1` to download each file sequentially). Already existing files are automatically skipped and reported.

## Debug

If you get `(no data)` for a file, it's likely that Binance does not have the data for the chosen market/data-type/date/symbol/interval. You can verify what data is available [here](https://data.binance.vision/?prefix=data/). Product `usd-m` corresponds to `futures/um`, `coin-m` corresponds to `futures/cm`.

## License

MIT
