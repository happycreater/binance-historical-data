"""
定时任务，获取标的的开收盘时间并生成shell脚本模板
支持命令行参数：--product 和 --base
"""
import time
from binance import Client
import pandas as pd
import os
from pathlib import Path
from binance.enums import HistoricalKlinesType as LT
from datetime import datetime, timezone
import argparse
import sys

line_type_map = {
    "spot": LT.SPOT,
    "usd-m": LT.FUTURES,
}

exe = "node bin/binance-fetch.js"


def get_start_end_time(client, symbol, line_type):
    api_res = client.get_historical_klines(
        symbol, "1d", start_str="2010-01-01", limit=1, klines_type=line_type
    )
    start_date = pd.to_datetime(api_res[0][0], unit="ms")
    api_res = client.get_historical_klines(symbol, "1d", limit=1, klines_type=line_type)
    end_date = pd.to_datetime(api_res[-1][0], unit="ms")
    return (start_date, end_date)


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="获取标的的开收盘时间并生成 daily / monthly 下载脚本模板"
    )
    p.add_argument(
        "--product",
        "-p",
        choices=list(line_type_map.keys()),
        default="spot",
        help="市场类型: 'spot' 或 'usd-m' (默认: spot)",
    )
    p.add_argument(
        "--base",
        "-b",
        default="USDT",
        help="交易对的基准币，例如 USDT、BUSD 等 (默认: USDT)",
    )
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    product = args.product
    base = args.base.upper()

    client = Client()

    # 获取交易对信息
    if product == "spot":
        exg_info = client.get_exchange_info()
    elif product == "usd-m":
        exg_info = client.futures_exchange_info()
    else:
        print(f"Unsupported product: {product}", file=sys.stderr)
        return 1

    symbols = []
    for symbol_info in exg_info["symbols"]:
        symbol = symbol_info["symbol"]
        if symbol.endswith(base):
            symbols.append(symbol)

    print(f"Get {len(symbols)} symbols for {base} {product}")

    start_end_info = []
    for idx, symbol in enumerate(symbols, start=1):
        print(f"request for {symbol}, {idx} / {len(symbols)}")
        try:
            start, end = get_start_end_time(client, symbol, line_type_map[product])
            start_end_info.append(
                {
                    "symbol": symbol,
                    "start": start,
                    "end": end,
                }
            )
            time.sleep(2)
        except Exception as e:
            print(f"Error for {symbol}: {e}")

    if not start_end_info:
        print("没有获取到任何 start/end 信息，退出。")
        client.close_connection()
        return 1

    start_end_info = pd.DataFrame(start_end_info)
    utc_now = datetime.now(timezone.utc)
    # 昨天（UTC）
    utc_date = pd.Timestamp(utc_now - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    # 上一个月（同一天的上一月），格式为 YYYY-MM
    utc_month = pd.Timestamp(utc_now - pd.DateOffset(months=1)).strftime("%Y-%m")
    start_end_info["download_start_date_str"] = start_end_info["start"].dt.strftime(
        "%Y-%m-%d"
    )
    start_end_info["download_end_date_str"] = (
        start_end_info["end"].dt.strftime("%Y-%m-%d").clip(upper=utc_date)
    )
    start_end_info["download_start_month_str"] = start_end_info["start"].dt.strftime(
        "%Y-%m"
    )
    start_end_info["download_end_month_str"] = (
        start_end_info["end"].dt.strftime("%Y-%m").clip(upper=utc_month)
    )

    # Ensure output directories exist
    info_dir = Path("info")
    scripts_dir = Path("scripts")
    info_dir.mkdir(parents=True, exist_ok=True)
    scripts_dir.mkdir(parents=True, exist_ok=True)

    start_end_info.to_csv(
        info_dir / f"start_end_info_{product}_{utc_date}.csv", index=False
    )

    # daily
    output_file = scripts_dir / f"download_daily_{product}_{utc_date}.sh"
    with output_file.open("w", encoding="utf-8") as f:
        f.write("#!/bin/bash\n")
        for idx, row in start_end_info.iterrows():
            start = str(row["download_start_date_str"])
            end = str(row["download_end_date_str"])
            symbol = str(row["symbol"])
            if start < end:
                command = f"{exe} -d {start} {end} -p {product} -t $data_type -s {symbol} -i $interval\n"
                f.write(command)
            elif start == end:
                command = f"{exe} -d {start} -p {product} -t $data_type -s {symbol} -i $interval\n"
                f.write(command)

    print(f"脚本已写入：{output_file.resolve()}")

    # monthly
    output_file = scripts_dir / f"download_monthly_{product}_{utc_date}.sh"
    with output_file.open("w", encoding="utf-8") as f:
        f.write("#!/bin/bash\n")
        for idx, row in start_end_info.iterrows():
            start = str(row["download_start_month_str"])
            end = str(row["download_end_month_str"])
            symbol = str(row["symbol"])
            if start < end:
                command = f"{exe} -d {start} {end} -p {product} -t $data_type -s {symbol} -i $interval\n"
                f.write(command)
            elif start == end:
                command = f"{exe} -d {start} -p {product} -t $data_type -s {symbol} -i $interval\n"
                f.write(command)

    print(f"脚本已写入：{output_file.resolve()}")

    client.close_connection()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
