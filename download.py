import re
from urllib import request, parse
from typing import Optional
from pathlib import Path
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import threading
import logging


@dataclass
class RemoteEntry:
    name: str
    is_dir: bool


BASE_URL = "https://data.binance.vision"
PROXY = "http://localhost:7890"
LOCAL_ROOT = "data.binance.vision"
LOG_FILE = "download.log"

_LOGGER: Optional[logging.Logger] = None


def get_logger() -> logging.Logger:
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER
    logger = logging.getLogger("binance_downloader")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(threadName)s %(message)s"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    _LOGGER = logger
    return logger


def debug_log(enabled: bool, title: str, content: str) -> None:
    if not enabled:
        return
    print(f"[DEBUG] {title}: {content}")


def build_proxy_opener(proxy_url: Optional[str] = None) -> request.OpenerDirector:
    if not proxy_url:
        return request.build_opener()
    proxy_handler = request.ProxyHandler({"http": proxy_url, "https": proxy_url})
    return request.build_opener(proxy_handler)


def get_bucket_url(prefix: str, opener: request.OpenerDirector, debug: bool) -> str:
    listing_url = f"{BASE_URL}/?prefix={parse.quote(prefix)}"
    debug_log(debug, "request", listing_url)
    with opener.open(listing_url) as response:
        html_content = response.read().decode("utf-8")
    debug_log(debug, "response", html_content)
    match = re.search(r"var BUCKET_URL = '(.*?)';", html_content)
    if not match:
        raise RuntimeError("BUCKET_URL not found in index page.")
    return match.group(1)


def list_prefix(
    prefix: str, opener: request.OpenerDirector, debug: bool
) -> list[RemoteEntry]:
    bucket_url = get_bucket_url(prefix, opener, debug)
    entries: list[RemoteEntry] = []
    continuation: Optional[str] = None

    while True:
        params = f"delimiter=/&prefix={parse.quote(prefix)}"
        if continuation:
            params += f"&marker={parse.quote(continuation)}"
        request_url = f"{bucket_url}?{params}"
        debug_log(debug, "request", request_url)
        with opener.open(request_url) as response:
            xml_content = response.read().decode("utf-8")
        debug_log(debug, "response", xml_content)
        root = ET.fromstring(xml_content)
        namespace = {"s3": root.tag.split("}")[0].strip("{")}

        for element in root.findall(".//s3:CommonPrefixes/s3:Prefix", namespace):
            if not element.text:
                continue
            name = element.text[len(prefix) :].strip("/")
            if name:
                entries.append(RemoteEntry(name=name, is_dir=True))

        for element in root.findall(".//s3:Contents/s3:Key", namespace):
            if not element.text:
                continue
            key = element.text
            if not key.endswith(".zip"):
                continue
            name = key[len(prefix) :]
            if name:
                entries.append(RemoteEntry(name=name, is_dir=False))

        is_truncated = root.findtext(
            ".//s3:IsTruncated", default="false", namespaces=namespace
        )
        if is_truncated.lower() != "true":
            break
        continuation = root.findtext(".//s3:NextMarker", namespaces=namespace)
        if not continuation:
            last_key = root.findtext(
                ".//s3:Contents/s3:Key[last()]", namespaces=namespace
            )
            if not last_key:
                break
            continuation = last_key

    return sorted(entries, key=lambda entry: (not entry.is_dir, entry.name))


def wildcard_match(text: str, pattern: str) -> bool:
    escaped = re.escape(pattern)
    regex_pattern = "^" + escaped.replace(r"\*", ".*").replace(r"\?", ".") + "$"
    return re.match(regex_pattern, text) is not None


def download_one(url: str) -> tuple[str, str]:
    local_path = parse.unquote(url.replace(BASE_URL, LOCAL_ROOT))
    if Path(local_path).exists():
        return "skipped", f"{local_path} local file exists, skip."
    parent_dir = Path(local_path).parent
    parent_dir.mkdir(parents=True, exist_ok=True)
    opener = build_proxy_opener()
    with opener.open(url) as response:
        with open(local_path, "wb") as f:
            f.write(response.read())
    return "downloaded", f"Downloaded to {local_path}"


def download_one_pattern(pattern, symbol_glob="*USDT") -> int:

    endpoint = pattern.split("SYMBOL")[0]
    logger = get_logger()
    proxy = build_proxy_opener(PROXY)
    print(f"Listing all symbols under {endpoint} ...")
    logger.info("Listing all symbols under %s ...", endpoint)
    all_symbols = list_prefix(endpoint, proxy, False)
    n = len(all_symbols)
    process_symbols = []
    for idx in range(n):
        symbol = all_symbols[idx].name
        # 如果匹配symbol通配符
        if not wildcard_match(symbol, symbol_glob):
            continue
        process_symbols.append(symbol)
    n = len(process_symbols)
    # 生产者-消费者：边列举边下载
    url_queue: Queue[Optional[str]] = Queue(maxsize=2048)
    stop_token = None
    total_urls = 0
    total_lock = threading.Lock()
    stats_lock = threading.Lock()
    success_count = 0
    failed_count = 0
    skipped_count = 0

    def producer(symbol: str, path: str, idx: int, total: int) -> None:
        nonlocal total_urls
        try:
            all_zip = list_prefix(path, proxy, False)
            if not all_zip:
                print(f"process {idx + 1} / {total}  symbol: {symbol} (no zip files)")
                logger.info(
                    "process %s / %s  symbol: %s (no zip files)",
                    idx + 1,
                    total,
                    symbol,
                )
                return
            print(f"process {idx + 1} / {total}  symbol: {symbol}")
            logger.info("process %s / %s  symbol: %s", idx + 1, total, symbol)
            print(
                f"{symbol}: {len(all_zip)} zip files: from {all_zip[0].name} to {all_zip[-1].name}"
            )
            logger.info(
                "%s: %s zip files: from %s to %s",
                symbol,
                len(all_zip),
                all_zip[0].name,
                all_zip[-1].name,
            )
            for entry in all_zip:
                encoded_path = parse.quote(path, safe="/")
                encoded_name = parse.quote(entry.name)
                url = f"{BASE_URL}/{encoded_path}{encoded_name}"
                url_queue.put(url)
                with total_lock:
                    total_urls += 1
        except Exception as exc:
            print(f"process {idx + 1} / {total}  symbol: {symbol}")
            print(f"Failed to list {symbol}: {exc}")
            logger.exception("Failed to list %s", symbol)

    def consumer(worker_id: int) -> None:
        nonlocal success_count, failed_count, skipped_count
        while True:
            try:
                url = url_queue.get(timeout=1)
            except Empty:
                continue
            if url is stop_token:
                url_queue.task_done()
                break
            try:
                status, result = download_one(url)
                logger.info("Worker %s: %s", worker_id, result)
                with stats_lock:
                    if status == "downloaded":
                        success_count += 1
                    elif status == "skipped":
                        skipped_count += 1
            except Exception as exc:
                print(f"Worker {worker_id}: {url}")
                print(f"Failed: {exc}")
                logger.exception("Worker %s failed: %s", worker_id, url)
                with stats_lock:
                    failed_count += 1
            finally:
                url_queue.task_done()

    list_workers = min(8, (n or 1))
    download_workers = 16

    consumers: list[threading.Thread] = []
    for i in range(download_workers):
        t = threading.Thread(target=consumer, args=(i + 1,), daemon=True)
        t.start()
        consumers.append(t)

    with ThreadPoolExecutor(max_workers=list_workers) as executor:
        futures = []
        for idx in range(n):
            symbol = process_symbols[idx]
            path = pattern.replace("SYMBOL", symbol)
            futures.append(executor.submit(producer, symbol, path, idx, n))
        for future in as_completed(futures):
            _ = future.result()

    for _ in range(download_workers):
        url_queue.put(stop_token)

    url_queue.join()
    for t in consumers:
        t.join()

    need_download = max(0, total_urls - skipped_count)
    print(f"Total {total_urls} zip files.")
    print(f"Need download: {need_download}")
    print(f"Downloaded: {success_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Failed: {failed_count}")
    logger.info("Total %s zip files.", total_urls)
    logger.info("Need download: %s", need_download)
    logger.info("Downloaded: %s", success_count)
    logger.info("Skipped: %s", skipped_count)
    logger.info("Failed: %s", failed_count)
    return 0


if __name__ == "__main__":
    patterns = [
        # "data/spot/daily/klines/SYMBOL/1d/",
        "data/spot/daily/klines/SYMBOL/1m/",
        # "data/futures/um/monthly/fundingRate/SYMBOL/"
    ]
    for pattern in patterns:
        download_one_pattern(pattern, symbol_glob="*USDT")
