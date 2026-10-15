"""코인 선물 RSI 신호 — 지표·목표/손절·상위 주기 맥락 계산 (v2.37).

바이낸스 USDⓈ-M 선물 공개 API(fapi.binance.com, 키 불필요)에서 봉만 읽는다.
주문 기능은 없고 넣지 않는다.

봉(bar)은 dict: {"t": 시작 ms, "T": 마감 ms, "o","h","l","c": float}.
계산 함수는 전부 순수 함수(네트워크 없음)라 고정 배열로 테스트한다.
"""
import os

import requests


def _env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return float(default)


def _env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return int(default)


def _env_bool(name, default):
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


def _parse_targets(raw):
    """"BTCUSDT:5m,BTCUSDT:1m" → [("BTCUSDT","5m"), ...]. 빈 항목·형식 오류는 무시."""
    out = []
    for part in (raw or "").split(","):
        sym, _, tf = part.strip().partition(":")
        if sym and tf:
            out.append((sym.strip().upper(), tf.strip()))
    return out


def _parse_dedup(raw, default=12):
    """"5m:12,1m:30" → {"5m": 12, "1m": 30}. 숫자 하나("12")면 모든 주기에 적용
    ("*" 키). 형식 오류 항목은 무시."""
    out = {"*": default}
    for part in (raw or "").split(","):
        tf, sep, n = part.strip().partition(":")
        try:
            if sep:
                out[tf.strip()] = int(n)
            elif tf:
                out["*"] = int(tf)
        except ValueError:
            continue
    return out


# 모든 상수는 여기 한 곳. Railway 환경변수로 덮어쓸 수 있다.
CONFIG = {
    # 신호 규칙
    "RSI_LEN": _env_int("COIN_RSI_LEN", "14"),
    "RSI_LOW": _env_float("COIN_RSI_LOW", "30"),     # 직전 봉 RSI < 이 값 → 다음 양봉에 롱
    "RSI_HIGH": _env_float("COIN_RSI_HIGH", "70"),   # 직전 봉 RSI > 이 값 → 다음 음봉에 숏
    # 같은 심볼·주기·방향 N봉 안 재신호는 무시. 주기별(1m은 신호가 잦아 30봉), 목록에 없는 주기는 "*"
    "DEDUP_BARS": _parse_dedup(os.environ.get("COIN_DEDUP_BARS", "5m:12,1m:30")),
    # 신호 경로
    "SELF_ENABLED": _env_bool("COIN_SELF_ENABLED", "1"),        # B. 자체 판정
    "SELF_TARGETS": _parse_targets(os.environ.get("COIN_SELF_TARGETS", "BTCUSDT:5m,BTCUSDT:1m")),  # 실매매가 1m 신호로 들어감
    "WEBHOOK_ENABLED": _env_bool("COIN_WEBHOOK_ENABLED", "1"),  # A. 트레이딩뷰 Webhook
    # 목표/손절
    "SWING_K": _env_int("COIN_SWING_K", "3"),             # 양옆 K봉보다 낮/높아야 스윙
    "LEVEL_PAD_PCT": _env_float("COIN_LEVEL_PAD_PCT", "0.05"),  # 목표·손절 여유 %
    "LEVEL_LOOKBACK": _env_int("COIN_LEVEL_LOOKBACK", "200"),   # 스윙 탐색 봉 수
    # 상위 주기 맥락
    "CLOUD_TF": os.environ.get("COIN_CLOUD_TF", "30m"),
    "ICHI_TENKAN": 9,
    "ICHI_KIJUN": 26,
    "ICHI_SENKOU_B": 52,
    "ICHI_DISPLACEMENT": 26,  # "선행 26봉". 실제 이동은 26-1=25봉 — 트레이딩뷰 기본 Ichimoku와 동일
    "HTF_RSI_TF": os.environ.get("COIN_HTF_RSI_TF", "2h"),
    # 추적
    "EXPIRE_HOURS": _env_float("COIN_EXPIRE_HOURS", "24"),
    "SUMMARY_TIME_NZ": os.environ.get("COIN_SUMMARY_TIME", "09:00"),
}

TF_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "1d": 86_400_000,
}

BINANCE_FAPI = "https://fapi.binance.com/fapi/v1/klines"


def parse_kline_rows(rows):
    """바이낸스 klines 응답 배열 → 봉 dict 리스트."""
    return [{"t": int(r[0]), "o": float(r[1]), "h": float(r[2]), "l": float(r[3]),
             "c": float(r[4]), "T": int(r[6])} for r in rows]


def fetch_klines(symbol, tf, limit=500, end_ms=None, start_ms=None):
    """공개 klines 조회. 실패하면 예외 — 호출부에서 처리."""
    params = {"symbol": symbol, "interval": tf, "limit": min(int(limit), 1500)}
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    r = requests.get(BINANCE_FAPI, params=params, timeout=10)
    r.raise_for_status()
    return parse_kline_rows(r.json())


def dedup_bars(tf):
    d = CONFIG["DEDUP_BARS"]
    return d.get(tf, d.get("*", 12))


def closed_only(bars, now_ms):
    """마감된 봉만(마지막 미완성 봉 제거)."""
    return [b for b in bars if b["T"] < now_ms]


# ── 지표 ──────────────────────────────────────────────────────────────

def rsi_wilder(closes, length=14):
    """Wilder RSI. 트레이딩뷰 ta.rsi(기본 RSI 지표)와 같은 방식:
    변화량의 상승분/하락분을 ta.rma(첫 값은 length개 단순평균, 이후
    rma = (prev*(length-1) + x)/length)로 평활. 결과 리스트는 closes와
    길이가 같고 계산 불가 구간(앞 length개)은 None.
    (트레이딩뷰는 전체 이력으로 계산하므로 수백 봉 이상 넣어야 값이 수렴해 일치)"""
    n = len(closes)
    out = [None] * n
    if n <= length:
        return out
    gains, losses = [], []
    for i in range(1, n):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains[:length]) / length
    avg_l = sum(losses[:length]) / length

    def _val(g, l):
        if l == 0:
            return 100.0 if g > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + g / l)

    out[length] = _val(avg_g, avg_l)
    for i in range(length + 1, n):
        avg_g = (avg_g * (length - 1) + gains[i - 1]) / length
        avg_l = (avg_l * (length - 1) + losses[i - 1]) / length
        out[i] = _val(avg_g, avg_l)
    return out


def signal_at(bars, i, rsi, low=None, high=None):
    """용어: ①RSI봉(bars[i-1], 30 밑/70 위 마감) → ②확인봉(bars[i], 양봉/음봉
    마감 — 이때 신호 확정) → ③진입봉(bars[i+1]). "신호 봉 종가"·진입 참고가 =
    ②확인봉 종가. 저장·Webhook의 bar_open_ms도 ②확인봉 기준.
    bars[i] 마감 시점의 신호. 직전 봉 RSI < low 이고 bars[i] 양봉 → "long",
    직전 봉 RSI > high 이고 bars[i] 음봉 → "short", 아니면 None.
    도지(종가 == 시가)는 양봉도 음봉도 아님."""
    low = CONFIG["RSI_LOW"] if low is None else low
    high = CONFIG["RSI_HIGH"] if high is None else high
    if i < 1 or rsi[i - 1] is None:
        return None
    b = bars[i]
    if rsi[i - 1] < low and b["c"] > b["o"]:
        return "long"
    if rsi[i - 1] > high and b["c"] < b["o"]:
        return "short"
    return None


def find_swings(bars, k=None):
    """스윙 저점/고점. 저점 = 양옆 K봉 저가보다 모두 엄격히 낮은 저가.
    양 끝 K봉은 한쪽 이웃이 모자라 제외. 반환 ([(idx, low)], [(idx, high)])."""
    k = CONFIG["SWING_K"] if k is None else k
    lows, highs = [], []
    for i in range(k, len(bars) - k):
        nb = bars[i - k:i] + bars[i + 1:i + 1 + k]
        if all(bars[i]["l"] < b["l"] for b in nb):
            lows.append((i, bars[i]["l"]))
        if all(bars[i]["h"] > b["h"] for b in nb):
            highs.append((i, bars[i]["h"]))
    return lows, highs


def compute_levels(bars, side, entry, k=None, pad_pct=None):
    """목표1/2·손절·손익비.
    숏: 진입가 아래 스윙 저점을 가까운 순으로 목표1·목표2(저점 +여유 — 닿기
    직전에 익절), 진입가 위 가장 가까운 스윙 고점 +여유 = 손절(고점 바깥).
    롱은 대칭(목표 = 고점 −여유, 손절 = 저점 −여유).
    여유를 적용한 목표가 진입가를 넘어가 버리는 스윙(진입가 바로 옆)은 건너뜀.
    없는 값은 None."""
    k = CONFIG["SWING_K"] if k is None else k
    pad = (CONFIG["LEVEL_PAD_PCT"] if pad_pct is None else pad_pct) / 100.0
    lows, highs = find_swings(bars, k)
    low_px = sorted({p for _, p in lows})
    high_px = sorted({p for _, p in highs})
    if side == "short":
        targets = [p * (1 + pad) for p in reversed(low_px) if p * (1 + pad) < entry]
        stops = [p * (1 + pad) for p in high_px if p > entry]
    else:
        targets = [p * (1 - pad) for p in high_px if p * (1 - pad) > entry]
        stops = [p * (1 - pad) for p in reversed(low_px) if p < entry]
    t1 = targets[0] if targets else None
    t2 = targets[1] if len(targets) > 1 else None
    stop = stops[0] if stops else None
    rr = None
    if t1 is not None and stop is not None and stop != entry:
        rr = abs(entry - t1) / abs(stop - entry)
    return {"target1": t1, "target2": t2, "stop": stop, "rr": rr}


def ichimoku_cloud(bars, tenkan=None, kijun=None, senkou_b=None, displacement=None):
    """마지막 봉 위치의 구름(선행스팬 A/B). 트레이딩뷰 기본 Ichimoku처럼
    (displacement-1)봉 앞에서 계산한 값이 현재 봉에 그려진다.
    반환 (span_a, span_b) 또는 데이터 부족이면 None."""
    tenkan = tenkan or CONFIG["ICHI_TENKAN"]
    kijun = kijun or CONFIG["ICHI_KIJUN"]
    senkou_b = senkou_b or CONFIG["ICHI_SENKOU_B"]
    displacement = displacement or CONFIG["ICHI_DISPLACEMENT"]
    j = len(bars) - 1 - (displacement - 1)   # 구름 값이 계산된 봉
    if j + 1 < max(tenkan, kijun, senkou_b):
        return None

    def mid(n):
        w = bars[j - n + 1:j + 1]
        return (max(b["h"] for b in w) + min(b["l"] for b in w)) / 2.0

    span_a = (mid(tenkan) + mid(kijun)) / 2.0
    span_b = mid(senkou_b)
    return span_a, span_b


def cloud_context(price, cloud, side):
    """구름 대비 위치·두께·신호 방향 일치 여부.
    위치 above/inside/below, 두께 = |A−B|/가격 %, 편향 "편"(신호와 같은 쪽)/
    "역"(반대)/"중립"(구름 안)."""
    if cloud is None:
        return None
    a, b = cloud
    top, bot = max(a, b), min(a, b)
    if price > top:
        pos = "above"
    elif price < bot:
        pos = "below"
    else:
        pos = "inside"
    if pos == "inside":
        bias = "중립"
    elif (pos == "above") == (side == "long"):
        bias = "편"
    else:
        bias = "역"
    return {"pos": pos, "thick_pct": abs(a - b) / price * 100.0, "bias": bias,
            "span_a": a, "span_b": b}


def htf_rsi(bars, length=None):
    """상위 주기 RSI 현재값과 방향(직전 봉 대비 ↑/↓/→). 미완성 봉 포함 —
    신호 순간 차트에 보이는 값과 같게."""
    length = length or CONFIG["RSI_LEN"]
    r = rsi_wilder([b["c"] for b in bars], length)
    if len(r) < 2 or r[-1] is None or r[-2] is None:
        return None
    d = r[-1] - r[-2]
    arrow = "↑" if d > 0 else ("↓" if d < 0 else "→")
    return {"value": r[-1], "dir": arrow}


def build_signal_context(symbol, tf, side, entry, bar_close_ms, fetch=fetch_klines):
    """신호 하나에 필요한 레벨·맥락 전부. 네트워크 실패한 부분은 None으로 두고
    나머지는 계속 계산(알림 자체는 나가게)."""
    out = {"target1": None, "target2": None, "stop": None, "rr": None,
           "cloud": None, "htf_rsi": None, "errors": []}
    try:
        bars = fetch(symbol, tf, limit=CONFIG["LEVEL_LOOKBACK"], end_ms=bar_close_ms)
        bars = [b for b in bars if b["T"] <= bar_close_ms]   # 신호 봉까지만
        out.update(compute_levels(bars, side, entry))
    except Exception as e:
        out["errors"].append(f"levels: {e}")
    try:
        cb = fetch(symbol, CONFIG["CLOUD_TF"], limit=120, end_ms=bar_close_ms)
        out["cloud"] = cloud_context(entry, ichimoku_cloud(cb), side)
    except Exception as e:
        out["errors"].append(f"cloud: {e}")
    try:
        hb = fetch(symbol, CONFIG["HTF_RSI_TF"], limit=300, end_ms=bar_close_ms)
        out["htf_rsi"] = htf_rsi(hb)
    except Exception as e:
        out["errors"].append(f"htf_rsi: {e}")
    return out
