"""선물 시세 공개 API 도달 점검 (v2.77) — 급등 숏 알림을 어디서 계산할 수 있는지 고르기 위한 1회성 진단.
Railway(미국)는 binance fapi가 451이다(v2.39). 다른 거래소 공개 시세가 닿는지 실제로 찔러 본다. 읽기 전용."""
import requests

URLS = {
    "binance_fapi": "https://fapi.binance.com/fapi/v1/ticker/price",
    "okx": "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
    "bitget": "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
    "mexc": "https://contract.mexc.com/api/v1/contract/ticker",
    "gate": "https://api.gateio.ws/api/v4/futures/usdt/tickers",
    "bybit": "https://api.bybit.com/v5/market/tickers?category=linear",
    "kucoin": "https://api-futures.kucoin.com/api/v1/allTickers",
    "binance_s3_list": "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/daily/klines/",
    "bitget_5m": "https://api.bitget.com/api/v2/mix/market/candles?symbol=BTCUSDT&productType=USDT-FUTURES&granularity=5m&limit=15",
    "bitget_1d": "https://api.bitget.com/api/v2/mix/market/candles?symbol=BTCUSDT&productType=USDT-FUTURES&granularity=1Dutc&limit=25",
}


def probe(get=requests.get):
    out = {}
    for name, url in URLS.items():
        try:
            r = get(url, timeout=8)
            out[name] = {"status": r.status_code, "bytes": len(r.content)}
        except Exception as e:                      # noqa: BLE001 — 진단이라 실패 사유를 그대로 돌려준다
            out[name] = {"status": 0, "error": type(e).__name__}
    return out


if __name__ == "__main__":
    import json
    print(json.dumps(probe(), indent=1))
