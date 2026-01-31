import os
from pathlib import Path
from download import LOCAL_ROOT, wildcard_match
import zipfile
import pandas as pd

CLEAN_ROOT = "parquet.binance.vision"
META_INFO = "processed.txt"

RAW_KLINES_COLS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]


HEADERS = {
    "data/spot/daily/klines/SYMBOL/1d/": RAW_KLINES_COLS,
    "data/spot/daily/klines/SYMBOL/1m/": RAW_KLINES_COLS,
}

INT_COLS = ["open_time", "close_time", "number_of_trades"]


def clean_one_zip(zip_path: Path, pattern: str, symbol: str) -> None:
    # print(zip_path, pattern, symbol)
    df = None
    with zipfile.ZipFile(zip_path, "r") as f:
        file_list = f.namelist()
        member = file_list[0]
        with f.open(member) as member_file:
            df = pd.read_csv(
                member_file,
                header=None,
                names=HEADERS[pattern],
                on_bad_lines="skip",
                dtype="float64",
            )
    if df is None:
        print(f"Failed to read {zip_path}, skip.")
        return
    df["pattern"] = pattern
    df["symbol"] = symbol
    # 特殊处理，对于2025年以后的现货数据，修正open_time和close_time
    # if pattern == "data/spot/daily/klines/SYMBOL/1d/":
    if "spot" in pattern and "klines" in pattern:
        # **Note**: The timestamp for SPOT Data from January 1st 2025 onwards will be in microseconds.
        if df["open_time"].min() >= pd.Timestamp("2025-01-01").timestamp() * 1000:
            df["open_time"] = df["open_time"] / 1000
            df["close_time"] = df["close_time"] / 1000
    else:
        raise ValueError(f"Unknown pattern {pattern}")
    for col in df.columns:
        if col in INT_COLS:
            df[col] = df[col].astype("int64")
    out_dir = Path(CLEAN_ROOT) / pattern / f"symbol={symbol}" / "data.parquet"
    insert(out_dir, df)
    # 将元信息写入processed.txt
    meta_info_path = Path(CLEAN_ROOT) / pattern / META_INFO
    with open(meta_info_path, "a") as f:
        # 写入完整路径
        f.write(f"{zip_path}\n")


def insert(out_path: Path, df: pd.DataFrame) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        existing_df = pd.read_parquet(out_path)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df.drop_duplicates(inplace=True)
        # 按照第一列排序
        combined_df.sort_values(by=df.columns[0], inplace=True)
        combined_df.to_parquet(out_path, index=False)
    else:
        df.sort_values(by=df.columns[0], inplace=True)
        df.to_parquet(out_path, index=False)


def clean_one_pattern(pattern: str, symbol_glob: str = "*") -> int:
    endpoint = pattern.split("SYMBOL")[0]
    # 获取所有符号
    all_symbols = os.listdir(Path(LOCAL_ROOT) / endpoint)
    process_symbols = []
    for symbol in all_symbols:
        if not wildcard_match(symbol, symbol_glob):
            continue
        process_symbols.append(symbol)
    process_symbols.sort()
    n = len(process_symbols)
    print(f"Cleaning pattern {pattern} with {n} symbols ...")
    # 获取已经处理的zip文件列表
    meta_info_path = Path(CLEAN_ROOT) / pattern / META_INFO
    processed_zips = set()
    if meta_info_path.exists():
        with open(meta_info_path, "r") as f:
            for line in f:
                processed_zips.add(line.strip())
    for idx in range(n):
        # 处理每个符号
        symbol = process_symbols[idx]
        local_path = Path(LOCAL_ROOT) / pattern.replace("SYMBOL", symbol)
        zip_files = [file for file in os.listdir(local_path) if file.endswith(".zip")]
        zip_files.sort()
        print(
            f"process {idx + 1} / {n}  symbol: {symbol} with {len(zip_files)} zip files"
        )
        for file in zip_files:
            zip_path = local_path / file
            if str(zip_path) in processed_zips:
                # print(f"  skipped {zip_path}, already processed.")
                continue
            clean_one_zip(zip_path, pattern, symbol)
    return 0


if __name__ == "__main__":
    patterns = [
        # "data/spot/daily/klines/SYMBOL/1d/",
        "data/spot/daily/klines/SYMBOL/1m/",
    ]
    for pattern in patterns:
        clean_one_pattern(pattern, symbol_glob="*USDT")
