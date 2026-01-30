#!/usr/bin/env python3
"""Standalone Binance historical data downloader.

This script mirrors the CLI behavior of the Node-based tool and provides:
- Symbol discovery (optional wildcard support) via Binance API.
- Downloading daily/monthly datasets with checksum verification.
- Remote index discovery with local caching to reduce 404s.
- Logging to both stdout and a file in the output directory.
"""
import argparse
import datetime as dt
import hashlib
import logging
import os
import re
import sys
import textwrap
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, List, Optional, Set, TextIO
from urllib import request, error, parse


BINANCE_DATA_BASE_URL = "https://data.binance.vision"
LOG_FILE_NAME = "binance-fetch.log"
INDEX_CACHE_DIR = ".binance-index-cache"

PRODUCTS = ["spot", "usd-m", "coin-m", "option"]
SPOT_DATA_TYPES = ["klines", "aggTrades", "trades"]
FUTURES_DAILY_DATA_TYPES = [
    "aggTrades",
    "bookDepth",
    "bookTicker",
    "indexPriceKlines",
    "klines",
    "liquidationSnapshot",
    "markPriceKlines",
    "metrics",
    "premiumIndexKlines",
    "trades",
]
FUTURES_MONTHLY_DATA_TYPES = [
    "aggTrades",
    "bookTicker",
    "fundingRate",
    "indexPriceKlines",
    "klines",
    "markPriceKlines",
    "premiumIndexKlines",
    "trades",
]
OPTIONS_DATA_TYPES = ["BVOLIndex", "EOHSummary"]
DATATYPES_WITH_INTERVAL = [
    "klines",
    "indexPriceKlines",
    "markPriceKlines",
    "premiumIndexKlines",
]
INTERVAL_LIST = [
    "1s",
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1mo",
]

SYMBOLS_API = {
    "spot": "https://api.binance.com/api/v3/exchangeInfo",
    "usd-m": "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "coin-m": "https://dapi.binance.com/dapi/v1/exchangeInfo",
    "option": "https://eapi.binance.com/eapi/v1/exchangeInfo",
}

MONTH_REGEX = re.compile(r"^20\d{2}-(?:0[1-9]|1[0-2])$")
DAY_REGEX = re.compile(r"^20\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])$")


class IncorrectParamError(ValueError):
    pass


class ProgressBar:
    """Simple terminal progress bar with current URL display."""

    def __init__(
        self,
        label: str,
        total: int,
        width: int = 32,
        stream: Optional[TextIO] = None,
    ) -> None:
        self.label = label
        self.total = max(total, 0)
        self.width = width
        self.stream = stream or sys.stderr
        self.current = 0
        self.finished = False

    def update(self, current: int, current_url: Optional[str] = None) -> None:
        """Render progress bar with the current URL at the end."""
        self.current = min(max(current, 0), self.total) if self.total else current
        total = self.total if self.total else max(current, 1)
        filled = int(self.width * (self.current / total))
        bar = "#" * filled + "-" * (self.width - filled)
        url_text = current_url or ""
        if len(url_text) > 120:
            url_text = url_text[:117] + "..."
        line = f"{self.label} [{bar}] {self.current}/{total}"
        if url_text:
            line = f"{line} {url_text}"
        self.stream.write("\r" + line)
        self.stream.flush()

    def finish(self) -> None:
        """Finalize the progress bar line with a newline."""
        if self.finished:
            return
        self.finished = True
        self.stream.write("\n")
        self.stream.flush()

def parse_args() -> argparse.Namespace:
    """Parse command-line options for the downloader."""
    epilog = textwrap.dedent(
        """\
        Parameter reference:
          --product (-p):
            spot | usd-m | coin-m | option

          --data-type (-t) for spot:
            klines | aggTrades | trades

          --data-type (-t) for usd-m/coin-m monthly:
            aggTrades | bookTicker | fundingRate | indexPriceKlines | klines
            markPriceKlines | premiumIndexKlines | trades

          --data-type (-t) for usd-m/coin-m daily:
            aggTrades | bookDepth | bookTicker | indexPriceKlines | klines
            liquidationSnapshot | markPriceKlines | metrics | premiumIndexKlines | trades

          --data-type (-t) for option:
            BVOLIndex | EOHSummary

          --intervals (-i):
            1s 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1mo

        Date format:
          daily data   -> YYYY-MM-DD
          monthly data -> YYYY-MM
          range        -> pass two dates, e.g. 2021-01 2023-12
        """
    )
    parser = argparse.ArgumentParser(
        description="Binance historical data fetcher",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=epilog,
    )
    parser.add_argument(
        "-d",
        "--date",
        nargs="+",
        required=True,
        help=(
            "Date or date range. Use YYYY-MM for monthly data or YYYY-MM-DD for daily data. "
            "Provide two dates to define a range (e.g., '2024-01 2024-08')."
        ),
    )
    parser.add_argument(
        "-p",
        "--product",
        required=True,
        choices=PRODUCTS,
        help="Product type: spot, usd-m, coin-m, option.",
    )
    parser.add_argument(
        "-t",
        "--data-type",
        required=True,
        help="Data type (see parameter reference in help footer).",
    )
    parser.add_argument(
        "-s",
        "--symbols",
        nargs="*",
        help=(
            "Symbols (e.g., BTCUSDT). Omit to fetch all symbols via Binance API. "
            "Wildcards like '*USDT' are supported. Multiple symbols can be separated by space."
        ),
    )
    parser.add_argument(
        "-i",
        "--intervals",
        nargs="*",
        help="Intervals (required for klines/indexPriceKlines/markPriceKlines/premiumIndexKlines).",
    )
    parser.add_argument(
        "-o",
        "--output-path",
        default="data.binance.vision",
        help=(
            "Root path for downloads. Defaults to data.binance.vision. "
            "The Binance URL path is mirrored under this directory."
        ),
    )
    parser.add_argument(
        "-P",
        "--parallel",
        type=int,
        default=5,
        help="Number of concurrent downloads (default: 5). Use -P 1 for sequential.",
    )
    parser.add_argument(
        "--no-validate-params",
        dest="validate_params",
        action="store_false",
        help="Skip validation of product/data type/symbols/intervals (use with care).",
    )
    parser.add_argument(
        "--api-proxy",
        dest="api_proxy",
        help="Proxy URL for Binance API requests (symbol discovery), e.g. http://localhost:7890.",
    )
    parser.add_argument(
        "--no-remote-index",
        dest="remote_index",
        action="store_false",
        help="Disable remote index listing from data.binance.vision (falls back to URL probing).",
    )
    parser.set_defaults(validate_params=True)
    parser.set_defaults(remote_index=True)
    return parser.parse_args()


def setup_logging(output_path: Path) -> logging.Logger:
    """Configure logging to stdout and a file under the output directory."""
    output_path.mkdir(parents=True, exist_ok=True)
    log_path = output_path / LOG_FILE_NAME
    logger = logging.getLogger("binance-fetch")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stdout_stream = open(sys.stdout.fileno(), mode="w", encoding="utf-8", closefd=False)
    stream_handler = logging.StreamHandler(stdout_stream)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def validate_dates(date_list: List[str]) -> tuple[bool, str, Optional[str]]:
    """Validate date inputs and return (by_day, start_date, end_date)."""
    if len(date_list) > 2:
        raise IncorrectParamError(
            f"only one or two date strings expected, received: {date_list}"
        )
    start_date = date_list[0]
    end_date = date_list[1] if len(date_list) == 2 else None

    if DAY_REGEX.match(start_date):
        by_day = True
    elif MONTH_REGEX.match(start_date):
        by_day = False
    else:
        raise IncorrectParamError(
            f"incorrect start date '{start_date}'. Use YYYY-MM or YYYY-MM-DD."
        )

    if end_date:
        if by_day and not DAY_REGEX.match(end_date):
            raise IncorrectParamError("end date format must be YYYY-MM-DD.")
        if not by_day and not MONTH_REGEX.match(end_date):
            raise IncorrectParamError("end date format must be YYYY-MM.")
        if start_date >= end_date:
            raise IncorrectParamError("end date should be greater than start date.")

    return by_day, start_date, end_date


def generate_dates(by_day: bool, start_date: str, end_date: Optional[str]) -> List[str]:
    """Generate a list of date strings covering the requested range."""
    dates = [start_date]
    if not end_date:
        return dates

    if by_day:
        current = dt.date.fromisoformat(start_date)
        end = dt.date.fromisoformat(end_date)
        while current.isoformat() != end.isoformat():
            current += dt.timedelta(days=1)
            dates.append(current.isoformat())
    else:
        current = dt.date.fromisoformat(start_date + "-01")
        end = dt.date.fromisoformat(end_date + "-01")
        while current.strftime("%Y-%m") != end.strftime("%Y-%m"):
            month = current.month + 1
            year = current.year
            if month == 13:
                month = 1
                year += 1
            current = dt.date(year, month, 1)
            dates.append(current.strftime("%Y-%m"))
    return dates


def validate_params(args: argparse.Namespace, by_day: bool) -> None:
    """Validate product/data-type/interval constraints unless disabled."""
    if not args.validate_params:
        return

    if args.product == "spot":
        if args.data_type not in SPOT_DATA_TYPES:
            raise IncorrectParamError(
                f"--data-type for 'spot' should be one of: {SPOT_DATA_TYPES}"
            )
    elif args.product == "option":
        if args.data_type not in OPTIONS_DATA_TYPES:
            raise IncorrectParamError(
                f"--data-type for 'option' should be one of: {OPTIONS_DATA_TYPES}"
            )
        if not by_day:
            raise IncorrectParamError("only daily data is available for 'option'")
    else:
        if by_day and args.data_type not in FUTURES_DAILY_DATA_TYPES:
            raise IncorrectParamError(
                f"--data-type for daily futures should be one of: {FUTURES_DAILY_DATA_TYPES}"
            )
        if not by_day and args.data_type not in FUTURES_MONTHLY_DATA_TYPES:
            raise IncorrectParamError(
                f"--data-type for monthly futures should be one of: {FUTURES_MONTHLY_DATA_TYPES}"
            )

    if args.data_type in DATATYPES_WITH_INTERVAL:
        if not args.intervals:
            raise IncorrectParamError(
                f"at least one interval is required for '{args.data_type}'"
            )
        invalid = [interval for interval in args.intervals if interval not in INTERVAL_LIST]
        if invalid:
            raise IncorrectParamError(
                f"incorrect intervals provided: {invalid}. Accepted: {INTERVAL_LIST}"
            )
    else:
        args.intervals = None


def build_proxy_opener(proxy_url: str) -> request.OpenerDirector:
    """Build a urllib opener using an explicit HTTP/HTTPS proxy."""
    proxy_handler = request.ProxyHandler({"http": proxy_url, "https": proxy_url})
    return request.build_opener(proxy_handler)


def fetch_symbols(product: str, proxy_url: Optional[str]) -> List[str]:
    """Fetch available symbols for a product via Binance exchangeInfo."""
    api_url = SYMBOLS_API.get(product)
    if not api_url:
        raise IncorrectParamError(f"Unsupported product for symbol lookup: {product}")
    opener = build_proxy_opener(proxy_url) if proxy_url else request.build_opener()
    with opener.open(api_url) as response:
        payload = response.read().decode("utf-8")
    data = json_load(payload)
    symbols = data.get("symbols")
    if not isinstance(symbols, list):
        raise IncorrectParamError("Unexpected response from Binance API.")
    return [item.get("symbol") for item in symbols if isinstance(item, dict)]


def json_load(payload: str) -> dict:
    """Parse JSON payload into a dictionary."""
    import json

    return json.loads(payload)


def resolve_symbols(symbols: Optional[List[str]], product: str, proxy_url: Optional[str]) -> List[str]:
    """Resolve explicit/wildcard symbols or fetch all symbols when omitted."""
    if symbols:
        symbols = [sym.upper() for sym in symbols]
    needs_fetch = not symbols or any("*" in sym for sym in symbols)
    if not needs_fetch:
        return symbols

    all_symbols = fetch_symbols(product, proxy_url)
    if not symbols:
        return all_symbols

    resolved: set[str] = set()
    for symbol in symbols:
        if "*" in symbol:
            pattern = "^" + re.escape(symbol).replace("\\*", ".*") + "$"
            regex = re.compile(pattern, re.IGNORECASE)
            for candidate in all_symbols:
                if regex.match(candidate):
                    resolved.add(candidate)
        else:
            resolved.add(symbol)
    return sorted(resolved)


def build_urls(
    product: str,
    data_type: str,
    symbols: Iterable[str],
    intervals: Optional[List[str]],
    dates: Iterable[str],
    by_day: bool,
    available_keys: Optional[Set[str]] = None,
) -> List[str]:
    """Construct download URLs for all symbol/date/interval combinations."""
    urls = []
    for symbol in symbols:
        for interval in intervals or [None]:
            for date in dates:
                parts = [BINANCE_DATA_BASE_URL, "data"]
                if product == "usd-m":
                    parts += ["futures", "um"]
                elif product == "coin-m":
                    parts += ["futures", "cm"]
                else:
                    parts.append(product)
                parts.append("daily" if by_day else "monthly")
                parts.append(data_type)
                parts.append(symbol)
                if interval:
                    parts.append(interval)
                    filename = f"{symbol}-{interval}-{date}.zip"
                else:
                    filename = f"{symbol}-{data_type}-{date}.zip"
                url_path = "/".join(parts[1:] + [filename])
                if available_keys is not None and url_path not in available_keys:
                    continue
                url = "/".join(parts + [filename])
                urls.append(url)
    return urls


def encode_url(url: str) -> str:
    """Percent-encode non-ASCII characters for safe HTTP requests."""
    parsed = parse.urlsplit(url)
    path = parse.quote(parsed.path, safe="/")
    query = parse.quote(parsed.query, safe="=&") if parsed.query else ""
    fragment = parse.quote(parsed.fragment) if parsed.fragment else ""
    return parse.urlunsplit((parsed.scheme, parsed.netloc, path, query, fragment))


def fetch_checksum(url: str) -> str:
    """Fetch and validate the checksum for a given data URL."""
    checksum_url = encode_url(f"{url}.CHECKSUM")
    with request.urlopen(checksum_url) as response:
        data = response.read().decode("utf-8")
    checksum = data[:64]
    if not re.match(r"^[0-9a-f]{64}$", checksum):
        raise IncorrectParamError("Invalid checksum response.")
    return checksum


def build_prefix(product: str, data_type: str, symbol: str, interval: Optional[str], by_day: bool) -> str:
    """Build a data.binance.vision prefix for directory listing."""
    parts = ["data"]
    if product == "usd-m":
        parts += ["futures", "um"]
    elif product == "coin-m":
        parts += ["futures", "cm"]
    else:
        parts.append(product)
    parts.append("daily" if by_day else "monthly")
    parts.append(data_type)
    parts.append(symbol)
    if interval:
        parts.append(interval)
    return "/".join(parts) + "/"


def cache_path_for_prefix(output_root: Path, prefix: str) -> Path:
    """Resolve cache file path for a given prefix."""
    digest = hashlib.sha256(prefix.encode("utf-8")).hexdigest()[:16]
    cache_dir = output_root / INDEX_CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"index_{digest}.json"


def load_cached_index(output_root: Path, prefix: str) -> Optional[Set[str]]:
    """Load cached index keys for a prefix, if available."""
    cache_path = cache_path_for_prefix(output_root, prefix)
    if not cache_path.exists():
        return None
    try:
        data = json_load(cache_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None
    keys = data.get("keys")
    if not isinstance(keys, list):
        return None
    return {key for key in keys if isinstance(key, str)}


def store_cached_index(output_root: Path, prefix: str, keys: Set[str]) -> None:
    """Persist index keys for a prefix."""
    cache_path = cache_path_for_prefix(output_root, prefix)
    payload = {
        "prefix": prefix,
        "cached_at": dt.datetime.now(dt.UTC).isoformat(),
        "keys": sorted(keys),
    }
    cache_path.write_text(json_dump(payload), encoding="utf-8")


def json_dump(payload: dict) -> str:
    """Serialize a dictionary as JSON."""
    import json

    return json.dumps(payload, ensure_ascii=False, indent=2)


def fetch_remote_index(prefix: str, logger: Optional[logging.Logger] = None) -> Set[str]:
    """Fetch available keys from data.binance.vision prefix listing."""
    listing_url = f"{BINANCE_DATA_BASE_URL}/?prefix={parse.quote(prefix)}"
    if logger:
        logger.info("Requesting index landing page: %s", listing_url)
    with request.urlopen(listing_url) as response:
        html_content = response.read().decode("utf-8")
    match = re.search(r"var BUCKET_URL = '(.*?)';", html_content)
    if not match:
        raise IncorrectParamError("BUCKET_URL not found in index page.")
    bucket_url = f"{match.group(1)}?delimiter=/&prefix={parse.quote(prefix)}"
    if logger:
        logger.info("Requesting index listing: %s", bucket_url)
    with request.urlopen(bucket_url) as response:
        xml_content = response.read().decode("utf-8")
    root = ET.fromstring(xml_content)
    namespace = {"s3": root.tag.split("}")[0].strip("{")}
    keys = {
        element.text
        for element in root.findall(".//s3:Key", namespace)
        if element.text and element.text.endswith(".zip")
    }
    if logger:
        logger.info("Index listing returned %s zip files for prefix %s", len(keys), prefix)
    return keys


def build_available_keys(
    output_root: Path,
    product: str,
    data_type: str,
    symbols: Iterable[str],
    intervals: Optional[List[str]],
    by_day: bool,
    logger: logging.Logger,
) -> Optional[Set[str]]:
    """Build a set of available zip keys using the remote index."""
    available: set[str] = set()
    for symbol in symbols:
        for interval in intervals or [None]:
            prefix = build_prefix(product, data_type, symbol, interval, by_day)
            cached = load_cached_index(output_root, prefix)
            if cached is None:
                try:
                    cached = fetch_remote_index(prefix)
                    store_cached_index(output_root, prefix, cached)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Failed to load remote index for %s: %s", prefix, exc)
                    return None
            available.update(cached)
    return available


def build_available_keys_for_symbol(
    output_root: Path,
    product: str,
    data_type: str,
    symbol: str,
    intervals: Optional[List[str]],
    by_day: bool,
    logger: logging.Logger,
) -> Optional[Set[str]]:
    """Build a set of available zip keys for a single symbol using the remote index."""
    available: set[str] = set()
    for interval in intervals or [None]:
        prefix = build_prefix(product, data_type, symbol, interval, by_day)
        cached = load_cached_index(output_root, prefix)
        if cached is not None:
            logger.info("Index cache hit for %s (%s keys).", prefix, len(cached))
        else:
            logger.info("Index cache miss for %s; fetching remote index.", prefix)
            try:
                cached = fetch_remote_index(prefix, logger)
                store_cached_index(output_root, prefix, cached)
                logger.info("Stored index cache for %s (%s keys).", prefix, len(cached))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to load remote index for %s: %s", prefix, exc)
                return None
        available.update(cached)
    return available


def download_file(url: str, output_root: Path, logger: logging.Logger) -> str:
    """Download a single file and verify checksum, returning a status message."""
    filename = url.split("/")[-1]
    url_path = url.replace(BINANCE_DATA_BASE_URL + "/", "")
    # Build local file paths for verified/unverified artifacts.
    verified_path = output_root / url_path
    unverified_path = verified_path.with_name(verified_path.stem + "_UNVERIFIED.zip")
    done_path = verified_path.with_suffix(verified_path.suffix + ".done")
    verified_path.parent.mkdir(parents=True, exist_ok=True)

    # Skip existing or completed files to avoid re-downloads.
    if verified_path.exists():
        return f"skipped (exists): {filename}"
    if done_path.exists():
        return f"skipped (done): {filename}"

    try:
        # Stream the download and compute SHA256 on the fly.
        response = request.urlopen(encode_url(url))
        content_type = response.headers.get("Content-Type", "")
        if "xml" in content_type:
            # Binance returns XML for missing data; mark it as not found.
            return f"no data: {filename}"
        sha256 = hashlib.sha256()
        with unverified_path.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                sha256.update(chunk)
                handle.write(chunk)
        # Validate downloaded file using Binance-provided checksum.
        checksum = fetch_checksum(url)
        if checksum == sha256.hexdigest():
            unverified_path.replace(verified_path)
            return f"downloaded: {filename}"
        # Mark checksum mismatch as missing to avoid repeated failures.
        return f"checksum mismatch: {filename}"
    except error.HTTPError as exc:
        if exc.code == 404:
            return f"no data (404): {filename}"
        logger.error("HTTP error for %s: %s", filename, exc)
        return f"failed: {filename}"
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to download %s: %s", filename, exc)
        return f"failed: {filename}"


def main() -> int:
    """Entry point for CLI execution."""
    args = parse_args()
    by_day, start_date, end_date = validate_dates(args.date)
    validate_params(args, by_day)

    output_path = Path(args.output_path).resolve()
    logger = setup_logging(output_path)

    symbols = resolve_symbols(args.symbols, args.product, args.api_proxy)
    if not symbols:
        raise IncorrectParamError("no symbols resolved for the request")

    dates = generate_dates(by_day, start_date, end_date)

    urls: List[str] = []
    logger.info("Saving to '%s'", output_path)
    logger.info(
        "Downloading '%s' %s data for %s symbols",
        args.data_type,
        "daily" if by_day else "monthly",
        len(symbols),
    )
    total_files = 0

    # Track result counts for the final summary.
    results = {"downloaded": 0, "skipped": 0, "no_data": 0, "failed": 0}
    with ThreadPoolExecutor(max_workers=args.parallel) as executor:
        future_map = {}
        listing_progress = ProgressBar("Listing", len(symbols))
        if args.remote_index:
            with ThreadPoolExecutor(max_workers=1) as index_executor:
                index_future = index_executor.submit(
                    build_available_keys_for_symbol,
                    output_path,
                    args.product,
                    args.data_type,
                    symbols[0],
                    args.intervals,
                    by_day,
                    logger,
                )
                for idx, symbol in enumerate(symbols):
                    available_keys = index_future.result()
                    if idx + 1 < len(symbols):
                        index_future = index_executor.submit(
                            build_available_keys_for_symbol,
                            output_path,
                            args.product,
                            args.data_type,
                            symbols[idx + 1],
                            args.intervals,
                            by_day,
                            logger,
                        )
                    if available_keys is None:
                        logger.info(
                            "Remote index unavailable for %s; falling back to URL probing.",
                            symbol,
                        )
                    symbol_urls = build_urls(
                        args.product,
                        args.data_type,
                        [symbol],
                        args.intervals,
                        dates,
                        by_day,
                        available_keys,
                    )
                    listing_progress.update(
                        idx + 1,
                        symbol_urls[0] if symbol_urls else f"{symbol} (no urls)",
                    )
                    total_files += len(symbol_urls)
                    for url in symbol_urls:
                        future_map[executor.submit(download_file, url, output_path, logger)] = url
        else:
            urls = []
            for idx, symbol in enumerate(symbols):
                symbol_urls = build_urls(
                    args.product,
                    args.data_type,
                    [symbol],
                    args.intervals,
                    dates,
                    by_day,
                )
                urls.extend(symbol_urls)
                listing_progress.update(
                    idx + 1,
                    symbol_urls[0] if symbol_urls else f"{symbol} (no urls)",
                )
            total_files = len(urls)
            future_map = {
                executor.submit(download_file, url, output_path, logger): url
                for url in urls
            }
        listing_progress.finish()
        logger.info("Total number of files to load: %s", total_files)
        download_progress = ProgressBar("Downloading", total_files)
        completed_files = 0
        for future in as_completed(future_map):
            message = future.result()
            completed_files += 1
            download_progress.update(completed_files, future_map[future])
            if message.startswith("downloaded"):
                results["downloaded"] += 1
            elif message.startswith("skipped"):
                results["skipped"] += 1
            elif message.startswith("no data"):
                results["no_data"] += 1
            else:
                results["failed"] += 1
            logger.info(message)
        download_progress.finish()

    logger.info(
        "Downloaded: %s/%s; not found: %s/%s; skipped: %s/%s; failed: %s/%s",
        results["downloaded"],
        total_files,
        results["no_data"],
        total_files,
        results["skipped"],
        total_files,
        results["failed"],
        total_files,
    )
    if results["failed"] > 0 or (
        results["downloaded"] == 0 and results["skipped"] == 0
    ):
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except IncorrectParamError as exc:
        print(f"Error: {exc}")
        sys.exit(2)
