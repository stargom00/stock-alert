"""코인 선물 RSI 신호 — 지표·목표(v2.44 고정 TP)·상위 주기 맥락 계산 (v2.37).

바이낸스 공개 klines(키 불필요)에서 봉만 읽는다. v2.39부터 기본은 현물
data-api.binance.vision(Railway 미국 리전에서 fapi는 451), COIN_KLINES_BASE로 변경.
주문 기능은 없고 넣지 않는다.

봉(bar)은 dict: {"t": 시작 ms, "T": 마감 ms, "o","h","l","c": float}.
계산 함수는 전부 순수 함수(네트워크 없음)라 고정 배열로 테스트한다.
"""
import os
from datetime import datetime, timezone

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
    # v2.44 목표 = 고정 TP(채널 방식). 스윙 목표1/2 계산 삭제
    "TP_PCT": _env_float("COIN_TP_PCT", "0.5"),                  # 진입가 ± 이 %
    "TP_PCT_WEEKEND": _env_float("COIN_TP_PCT_WEEKEND", "0.3"),  # 신호 시각이 UTC 토·일이면
    "TP_ROI_LEV": _env_float("COIN_TP_ROI_LEV", "20"),           # 메시지 ROI·청산가 배율(격리)
    "SWING_K": _env_int("COIN_SWING_K", "3"),             # 양옆 K봉보다 낮/높아야 스윙 (참고 1m 직전 고점/저점)
    "LEVEL_LOOKBACK": _env_int("COIN_LEVEL_LOOKBACK", "200"),   # 스윙 탐색 봉 수
    # v2.44 알림 억제(저장·추적은 함): ② 확인봉 (고가−저가)/시가 < 이 % → "미동 확인봉"
    "STILL_BAR_PCT": _env_float("COIN_STILL_BAR_PCT", "0.02"),
    "CROSS_DUP_MIN": _env_float("COIN_CROSS_DUP_MIN", "10"),  # v2.40 다른 주기 같은 방향 발송 후 이 분 이내면 중복
    # (v2.42: 짧은 주기 발송 뒤 긴 주기는 중복 아님 → 확인 신호로 발송, 같은 포지션에 묶음)
    # 상위 주기 맥락
    "CLOUD_TF": os.environ.get("COIN_CLOUD_TF", "30m"),
    "ICHI_TENKAN": 9,
    "ICHI_KIJUN": 26,
    "ICHI_SENKOU_B": 52,
    "ICHI_DISPLACEMENT": 26,  # "선행 26봉". 실제 이동은 26-1=25봉 — 트레이딩뷰 기본 Ichimoku와 동일
    "HTF_RSI_TF": os.environ.get("COIN_HTF_RSI_TF", "2h"),
    # v2.43 신저점/신고점 경고(통계용, 억제 안 함): 5m 최근 N봉 최저(롱)/최고(숏) 대비 되돌림
    "PULLBACK_TF": os.environ.get("COIN_PULLBACK_TF", "5m"),
    "PULLBACK_BARS": _env_int("COIN_PULLBACK_BARS", "24"),
    "PULLBACK_MIN_PCT": _env_float("COIN_PULLBACK_MIN_PCT", "0.1"),
    # v2.49(사용자 지시 2026-10-20): 큰 주기 반대 신호 경고 — 1m 신호 때 15m 최근 N봉에 반대방향
    # 신호(같은 규칙)가 있으면 "반등/되돌림 안의 잡음"으로 ⚠️. 억제 안 함, 통계용.
    # 경위: 15m ▲ 롱 두 개(01:00·01:15 UTC) 30분 뒤 1m ▼ 숏에 들어갔다가 +0.5% 역행.
    "HTF_SIGNAL_TF": os.environ.get("COIN_HTF_SIGNAL_TF", "15m"),
    "HTF_SIGNAL_BARS": _env_int("COIN_HTF_SIGNAL_BARS", "3"),
    # 추적
    "EXPIRE_HOURS": _env_float("COIN_EXPIRE_HOURS", "24"),    # 신호 단위 기록(조용히)
    # v2.41 포지션(채널 방식, 손절 없음) — 봇은 주문 안 함, 가상 수량으로 추적만
    "LIQ_PCT": _env_float("COIN_LIQ_PCT", "4.5"),                  # 청산가 참고: 20배(TP_ROI_LEV) 격리 ≈ 진입가 ∓4.5%
    "ADD_THRESHOLD_PCT": _env_float("COIN_ADD_THRESHOLD_PCT", "0.5"),  # 평단 대비 이만큼 역행 중 같은 방향 신호 → 추가
    "MAX_ADDS": _env_int("COIN_MAX_ADDS", "2"),
    "ADD_QTY": _env_float("COIN_ADD_QTY", "1"),                    # 추가 1회 수량 (1차 = 1단위)
    "GIVEUP_PCT": _env_float("COIN_GIVEUP_PCT", "3"),              # 평단 대비 역행 포기선
    "POS_EXPIRE_HOURS": _env_float("COIN_POS_EXPIRE_HOURS", "72"),
    "SUMMARY_TIME_NZ": os.environ.get("COIN_SUMMARY_TIME", "09:00"),
}

TF_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "1d": 86_400_000,
}

# v2.39: Railway(미국 리전) IP는 fapi.binance.com이 전부 451(지역 차단) — 기본을
# 지역 제한 없는 현물 공개 데이터로. 현물/선물 차이는 수십 달러라 스윙 자리엔 영향 없음.
KLINES_BASE = os.environ.get("COIN_KLINES_BASE", "https://data-api.binance.vision/api/v3/klines")
KLINES_FALLBACK = "https://api.binance.com/api/v3/klines"
SPOT_MAX_LIMIT = 1000   # 현물 klines 최대 1000 (선물 1500)


def klines_hosts():
    """조회 순서: 설정 호스트 1회 → 폴백 1회. 같은 주소면 한 번만."""
    return [KLINES_BASE] + ([KLINES_FALLBACK] if KLINES_FALLBACK != KLINES_BASE else [])


def price_basis():
    """메시지 하단 표기용. /fapi/ 경로면 선물, 아니면 현물."""
    return "선물" if "/fapi/" in KLINES_BASE else "현물"


class KlinesError(Exception):
    """모든 호스트 실패. str()은 메시지용 짧은 사유(HTTP 코드 + 호스트)."""


def _host(url):
    return url.split("//", 1)[-1].split("/", 1)[0]


def _fail_reason(url, e):
    resp = getattr(e, "response", None)
    if resp is not None and getattr(resp, "status_code", None):
        return f"HTTP {resp.status_code} ({_host(url)})"
    return f"{type(e).__name__} ({_host(url)})"


def parse_kline_rows(rows):
    """바이낸스 klines 응답 배열 → 봉 dict 리스트 (현물·선물 형식 동일)."""
    return [{"t": int(r[0]), "o": float(r[1]), "h": float(r[2]), "l": float(r[3]),
             "c": float(r[4]), "T": int(r[6])} for r in rows]


def fetch_klines(symbol, tf, limit=500, end_ms=None, start_ms=None):
    """공개 klines 조회. 호스트마다 정확히 1회 — 451 등 실패는 재시도 없이
    바로 다음 호스트로(같은 IP면 계속 451이라 재시도 무의미). 전부 실패하면
    KlinesError("HTTP 451 (data-api.binance.vision) → HTTP 451 (api.binance.com)")."""
    params = {"symbol": symbol, "interval": tf, "limit": min(int(limit), SPOT_MAX_LIMIT)}
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    reasons = []
    for url in klines_hosts():
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            return parse_kline_rows(r.json())
        except Exception as e:
            reasons.append(_fail_reason(url, e))
            print(f"[코인봉] {symbol} {tf} {reasons[-1]}")
    raise KlinesError(" → ".join(reasons))


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


def tp_pct_for(bar_close_ms):
    """v2.44 TP %. 신호 시각(② 마감)이 UTC 토·일이면 주말 값."""
    wd = datetime.fromtimestamp((bar_close_ms + 1) / 1000, timezone.utc).weekday()
    return CONFIG["TP_PCT_WEEKEND"] if wd >= 5 else CONFIG["TP_PCT"]


def tp_price(side, entry, pct):
    return entry * (1 + pct / 100.0) if side == "long" else entry * (1 - pct / 100.0)


def ref_swing(bars, side, entry, k=None):
    """v2.44 참고용 직전 스윙: 롱 = 진입가보다 위인 가장 최근 스윙 고점, 숏 = 아래인
    가장 최근 스윙 저점. 없으면 None."""
    lows, highs = find_swings(bars, k)
    if side == "long":
        cands = [p for _, p in sorted(highs, reverse=True) if p > entry]
    else:
        cands = [p for _, p in sorted(lows, reverse=True) if p < entry]
    return cands[0] if cands else None


def bar_range_pct(b):
    """v2.44 봉 변동폭 (고가−저가)/시가 %."""
    return (b["h"] - b["l"]) / b["o"] * 100.0 if b["o"] else None


def liquidation_price(side, entry, pct=None):
    """v2.41 청산가 참고(격리, 진입가 ∓LIQ_PCT%). 롱은 아래, 숏은 위.
    v2.44: 배율은 TP_ROI_LEV(20배)와 통일, 기본 4.5%."""
    pct = CONFIG["LIQ_PCT"] if pct is None else pct
    return entry * (1 - pct / 100.0) if side == "long" else entry * (1 + pct / 100.0)


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


def _err_text(e):
    if isinstance(e, KlinesError):
        return str(e)
    return f"{type(e).__name__}: {e}"[:80]


def pullback_check(bars, side, entry, n=None, min_pct=None):
    """v2.43 최근 n봉(마감분) 최저가(롱)/최고가(숏) 대비 진입가가 min_pct 이상
    되돌렸는지. dist_pct = 유리한 방향 거리(롱 = 진입이 최저보다 위, 숏 = 최고보다
    아래면 +). 봉 없으면 None."""
    n = CONFIG["PULLBACK_BARS"] if n is None else n
    min_pct = CONFIG["PULLBACK_MIN_PCT"] if min_pct is None else min_pct
    w = bars[-n:]
    if not w:
        return None
    if side == "long":
        ref = min(b["l"] for b in w)
        dist = (entry - ref) / ref * 100.0
    else:
        ref = max(b["h"] for b in w)
        dist = (ref - entry) / ref * 100.0
    return {"pullback_ok": dist >= min_pct - 1e-9, "ref_extreme": ref, "dist_pct": dist}


def htf_opposite_check(bars, side, n=None, low=None, high=None):
    """큰 주기 봉(마감분, 시간순)의 마지막 n봉 안에 side와 반대방향 신호(signal_at 규칙)가 있나.
    반환 {"opposite": bool, "at_ms": 가장 최근 반대 신호의 확인봉 마감 ms|None, "n": n}.
    RSI는 전체 bars로 계산(수렴 위해 수백 봉 필요)."""
    n = CONFIG["HTF_SIGNAL_BARS"] if n is None else n
    want = "short" if side == "long" else "long"
    rsi = rsi_wilder([b["c"] for b in bars], CONFIG["RSI_LEN"])
    at = None
    for i in range(max(1, len(bars) - n), len(bars)):
        if signal_at(bars, i, rsi, low, high) == want:
            at = bars[i]["T"]
    return {"opposite": at is not None, "at_ms": at, "n": n}


def build_signal_context(symbol, tf, side, entry, bar_close_ms, fetch=fetch_klines, spot_entry=False):
    """신호 하나에 필요한 레벨·맥락 전부. 네트워크 실패한 부분은 None으로 두고
    나머지는 계속 계산(알림 자체는 나가게).
    v2.44: 목표 = 진입가 ± TP(조회 불필요). 신호 주기 봉에서 ② 확인봉 변동폭,
    1m 봉에서 참고 직전 고점/저점.
    v2.43.1 spot_entry(Webhook = 선물 진입가): 되돌림 판정만 현물 환산가로 비교.
    환산가 = 신호 봉 마감 시각의 현물 5m 종가(이미 받은 5m 봉). 5m 봉이 그 시각에
    안 끝나면(1m 신호 대부분) 이미 받은 신호 주기 현물 봉의 종가. 둘 다 없으면
    선물가 그대로 비교하고 entry_spot = None."""
    pct = tp_pct_for(bar_close_ms)
    out = {"tp": tp_price(side, entry, pct), "tp_pct": pct,
           "confirm_range_pct": None, "ref_swing": None,
           "cloud": None, "htf_rsi": None, "pullback": None, "entry_spot": None,
           "htf_signal": None,   # v2.49 {"tf","opposite","at_ms","n"} — 신호 주기가 HTF 이상이면 None(해당 없음)
           "errors": {}}   # {"bar"|"ref"|"cloud"|"htf_rsi"|"pullback"|"htf_signal": 사유}
    bars = []
    try:
        bars = fetch(symbol, tf, limit=CONFIG["LEVEL_LOOKBACK"], end_ms=bar_close_ms)
        bars = [b for b in bars if b["T"] <= bar_close_ms]   # 신호 봉까지만
        conf = [b for b in bars if b["T"] == bar_close_ms]
        out["confirm_range_pct"] = bar_range_pct(conf[0]) if conf else None
    except Exception as e:
        out["errors"]["bar"] = _err_text(e)
    try:
        if tf == "1m":
            if "bar" in out["errors"]:
                raise KlinesError(out["errors"]["bar"])
            b1 = bars
        else:
            b1 = [b for b in fetch(symbol, "1m", limit=CONFIG["LEVEL_LOOKBACK"], end_ms=bar_close_ms)
                  if b["T"] <= bar_close_ms]
        out["ref_swing"] = ref_swing(b1, side, entry)
    except Exception as e:
        out["errors"]["ref"] = _err_text(e)
    try:
        cb = fetch(symbol, CONFIG["CLOUD_TF"], limit=120, end_ms=bar_close_ms)
        out["cloud"] = cloud_context(entry, ichimoku_cloud(cb), side)
    except Exception as e:
        out["errors"]["cloud"] = _err_text(e)
    try:
        hb = fetch(symbol, CONFIG["HTF_RSI_TF"], limit=300, end_ms=bar_close_ms)
        out["htf_rsi"] = htf_rsi(hb)
    except Exception as e:
        out["errors"]["htf_rsi"] = _err_text(e)
    try:
        # 신호 시각까지 마감된 5m 봉만(1m 신호면 진행 중인 5m 봉은 제외)
        pb = fetch(symbol, CONFIG["PULLBACK_TF"], limit=CONFIG["PULLBACK_BARS"] + 2, end_ms=bar_close_ms)
        closed = [b for b in pb if b["T"] <= bar_close_ms]
        cmp_entry = entry
        if spot_entry:
            same = [b["c"] for b in closed + bars if b["T"] == bar_close_ms]
            if same:
                out["entry_spot"] = cmp_entry = same[0]
        out["pullback"] = pullback_check(closed, side, cmp_entry)
    except Exception as e:
        out["errors"]["pullback"] = _err_text(e)
    htf = CONFIG["HTF_SIGNAL_TF"]
    if TF_MS.get(tf, 0) < TF_MS.get(htf, 0):
        try:
            hb = fetch(symbol, htf, limit=300, end_ms=bar_close_ms)
            hclosed = [b for b in hb if b["T"] <= bar_close_ms]     # 1m 신호 시각까지 마감된 HTF 봉만
            out["htf_signal"] = {"tf": htf, **htf_opposite_check(hclosed, side)}
        except Exception as e:
            out["errors"]["htf_signal"] = _err_text(e)
    return out
