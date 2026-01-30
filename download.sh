export start=2017-08-01
export end=2026-01-01
export clash_proxy=http://127.0.0.1:7890

python binance_fetch.py -d $start $end -p spot -t klines -s '*USDT' -i 1d --api-proxy $clash_proxy