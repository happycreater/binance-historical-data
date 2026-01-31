# data.binance.vision 路径速查

下列路径用于快速定位 Binance 历史数据的主要目录结构，便于在下载时设置 `prefix` 或 `pattern`。

## 现货 (Spot)

- `data/spot/daily/klines/<SYMBOL>/<INTERVAL>/`
- `data/spot/monthly/klines/<SYMBOL>/<INTERVAL>/`
- `data/spot/daily/aggTrades/<SYMBOL>/`
- `data/spot/monthly/aggTrades/<SYMBOL>/`
- `data/spot/daily/trades/<SYMBOL>/`
- `data/spot/monthly/trades/<SYMBOL>/`

## U 本位合约 (USD-M)

- `data/futures/um/daily/klines/<SYMBOL>/<INTERVAL>/`
- `data/futures/um/monthly/klines/<SYMBOL>/<INTERVAL>/`
- `data/futures/um/daily/trades/<SYMBOL>/`
- `data/futures/um/monthly/trades/<SYMBOL>/`
- `data/futures/um/daily/aggTrades/<SYMBOL>/`
- `data/futures/um/monthly/aggTrades/<SYMBOL>/`
- `data/futures/um/daily/fundingRate/<SYMBOL>/`
- `data/futures/um/monthly/fundingRate/<SYMBOL>/`
- `data/futures/um/daily/markPriceKlines/<SYMBOL>/<INTERVAL>/`
- `data/futures/um/monthly/markPriceKlines/<SYMBOL>/<INTERVAL>/`
- `data/futures/um/daily/indexPriceKlines/<SYMBOL>/<INTERVAL>/`
- `data/futures/um/monthly/indexPriceKlines/<SYMBOL>/<INTERVAL>/`
- `data/futures/um/daily/premiumIndexKlines/<SYMBOL>/<INTERVAL>/`
- `data/futures/um/monthly/premiumIndexKlines/<SYMBOL>/<INTERVAL>/`
- `data/futures/um/daily/bookTicker/<SYMBOL>/`
- `data/futures/um/daily/bookDepth/<SYMBOL>/`

## 币本位合约 (COIN-M)

- `data/futures/cm/daily/klines/<SYMBOL>/<INTERVAL>/`
- `data/futures/cm/monthly/klines/<SYMBOL>/<INTERVAL>/`
- `data/futures/cm/daily/trades/<SYMBOL>/`
- `data/futures/cm/monthly/trades/<SYMBOL>/`
- `data/futures/cm/daily/aggTrades/<SYMBOL>/`
- `data/futures/cm/monthly/aggTrades/<SYMBOL>/`
- `data/futures/cm/daily/fundingRate/<SYMBOL>/`
- `data/futures/cm/monthly/fundingRate/<SYMBOL>/`
- `data/futures/cm/daily/markPriceKlines/<SYMBOL>/<INTERVAL>/`
- `data/futures/cm/monthly/markPriceKlines/<SYMBOL>/<INTERVAL>/`
- `data/futures/cm/daily/indexPriceKlines/<SYMBOL>/<INTERVAL>/`
- `data/futures/cm/monthly/indexPriceKlines/<SYMBOL>/<INTERVAL>/`
- `data/futures/cm/daily/premiumIndexKlines/<SYMBOL>/<INTERVAL>/`
- `data/futures/cm/monthly/premiumIndexKlines/<SYMBOL>/<INTERVAL>/`

## 期权 (Options)

- `data/option/daily/BVOLIndex/`
- `data/option/daily/EOHSummary/`

## 说明

- `<SYMBOL>` 示例：`BTCUSDT`、`ETHUSDT`。
- `<INTERVAL>` 示例：`1m`、`1h`、`1d`。
- 若需确认某目录是否存在，可访问：`https://data.binance.vision/?prefix=<PATH>`。
