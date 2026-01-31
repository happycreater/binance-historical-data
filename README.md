# Binance Historical Data Collector (Rust)

一个使用 Rust 编写的 CLI 工具，用于从 [Binance](https://www.binance.com/en) 批量下载历史交易数据并清洗为 Parquet。该工具会自动遍历 Binance 数据目录，按需下载 ZIP 文件并转换为结构化数据，适合做大规模数据拉取与离线分析。

## 功能概览

- 支持从 `data.binance.vision` 自动枚举可用的交易对目录。
- 支持通配符过滤交易对（例如 `*USDT`）。
- 按 ZIP 文件流式下载并解析 CSV。
- 自动追加到已有 Parquet（同一 `pattern` + `symbol`）。
- 支持多线程并发下载与清洗。

## 安装与构建

确保已安装 Rust（建议使用稳定版）：

```shell
rustup install stable
```

构建可执行文件：

```shell
cargo build --release
```

## 快速开始

默认行为会下载 `data/spot/daily/klines/SYMBOL/1m/` 中所有 `*USDT` 的数据：

```shell
./target/release/binance-fast
```

使用环境变量进行自定义：

```shell
BINANCE_PATTERN="data/spot/daily/klines/SYMBOL/1m/" \
BINANCE_SYMBOL_GLOB="*USDT" \
BINANCE_DOWNLOAD_CHUNK_BYTES=1048576 \
BINANCE_S3_PROXY="http://127.0.0.1:7890" \
./target/release/binance-fast
```

## 环境变量说明

| 变量名 | 说明 | 默认值 |
| --- | --- | --- |
| `BINANCE_PATTERN` | 数据目录路径模板，`SYMBOL` 会被实际交易对替换 | `data/spot/daily/klines/SYMBOL/1m/` |
| `BINANCE_SYMBOL_GLOB` | 交易对通配符过滤（支持 `*`、`?`） | `*USDT` |
| `BINANCE_DOWNLOAD_CHUNK_BYTES` | 单次下载的读取块大小（字节） | `1048576` |
| `BINANCE_S3_PROXY` | 目录枚举使用的代理（仅用于 S3 listing 请求，ZIP 下载不走代理） | 空 |

更多路径速查见：`docs/data_binance_vision_paths.md`。

## 输出目录

清洗结果默认写入：

```
parquet.binance.vision/<pattern>/symbol=<symbol>/data.parquet
```

其中 `<pattern>` 即 `BINANCE_PATTERN` 的值，`<symbol>` 为实际交易对。例如：

```
parquet.binance.vision/data/spot/daily/klines/SYMBOL/1m/symbol=BTCUSDT/data.parquet
```

## 运行逻辑说明

1. 通过 `BINANCE_PATTERN` 获取 Binance 目录索引并枚举可用交易对。
2. 根据 `BINANCE_SYMBOL_GLOB` 进行匹配筛选。
3. 下载每个 ZIP 文件并解析为 CSV。
4. 合并写入对应的 Parquet 文件（如已存在则追加）。

## 许可证

MIT
