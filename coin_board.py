"""BTC 상황판 (v2.81) — 알림 없이, "상황"이라고 물으면 한 번 답한다. 주문 없음.

사용자 지시 2026-11-15 "1번부터 5번까지 모두 다 해줘"의 5번: 채널(투결혼)이 판단에 쓰는 것을 한 화면에 —
1h·2h·4h·1d 스토캐스틱 0/100 연속 마감, 일목 구름(지금 구름·26봉 뒤에 그려질 구름) 방향·두께, 가격이
구름 위/안/아래, 20선·99선 대비 위치. 판정·추천은 하지 않는다(묘사만).

7년 검증(프로젝트 문서 `코인-채널방식-검증`, 2026-11-15): 구름·위치 단독으로는 매매 우위가 거의 없었다 — 2h 하락 구름이
두꺼울 때 다음 24h가 평균보다 약했던 것(7년 중 5년)이 가장 일관된 차이였고, 그것도 숏 보유 규칙으로 바꾸면 7년 합 +1%.
그래서 이 판은 참고용이고, 그 사실을 판 맨 아래 한 줄로 적는다.

두께 "두꺼움/얇음" = 그 주기 직전 봉들(최대 DEPTH_RANK_BARS개) 안에서 지금 두께가 상위 1/3 / 하위 1/3 (백테스트와 같은
정의, 거기선 500봉). 구름은 signal_levels.ichimoku_cloud(트레이딩뷰 기본과 같은 위치)를 그대로 쓴다.
"""
from datetime import datetime, timezone, timedelta

import signal_levels as lv

SYMBOL = "BTCUSDT"
TFS = ("1h", "2h", "4h", "1d")
FETCH = 600
DEPTH_RANK_BARS = 400
STOCH = (14, 14, 3, 3)     # 트레이딩뷰 Stoch RSI 기본: RSI 14, Stoch 14, K 3, D 3
EXTREME = 1.0              # K ≤ 1 → "0", K ≥ 99 → "100" (채널의 "0값/100값")
NZ = timezone(timedelta(hours=13))
MINUS = "−"


def _sma(xs, n):
    out = [None] * len(xs)
    for i in range(len(xs)):
        w = xs[i - n + 1:i + 1] if i >= n - 1 else []
        if len(w) == n and all(v is not None for v in w):
            out[i] = sum(w) / n
    return out


def stoch_rsi_k(closes, rsi_len=STOCH[0], st_len=STOCH[1], k=STOCH[2]):
    """트레이딩뷰 Stoch RSI의 K: sma(100×(rsi−min14)/(max14−min14), 3). 계산 불가 구간은 None."""
    r = lv.rsi_wilder(closes, rsi_len)
    st = [None] * len(r)
    for i in range(len(r)):
        w = r[i - st_len + 1:i + 1] if i >= st_len - 1 else []
        if len(w) == st_len and all(v is not None for v in w):
            hi, lo = max(w), min(w)
            st[i] = 0.0 if hi == lo else 100.0 * (r[i] - lo) / (hi - lo)
    return _sma(st, k)


def stoch_streak(ks):
    """마감 봉 K의 끝에서부터 0(≤1) 또는 100(≥99)이 몇 번 이어졌나. 반환 (0|100|None, 횟수)."""
    vals = [v for v in ks if v is not None]
    if not vals:
        return None, 0
    side = 0 if vals[-1] <= EXTREME else (100 if vals[-1] >= 100 - EXTREME else None)
    if side is None:
        return None, 0
    n = 0
    for v in reversed(vals):
        if (v <= EXTREME) if side == 0 else (v >= 100 - EXTREME):
            n += 1
        else:
            break
    return side, n


def _cloud_at(bars):
    c = lv.ichimoku_cloud(bars)
    if c is None:
        return None
    a, b = c
    return {"bear": a < b, "thick": abs(a - b) / bars[-1]["c"] * 100.0, "top": max(a, b), "bot": min(a, b)}


def _lead_cloud(bars):
    """마지막 봉까지로 계산한 선행스팬 = 앞으로 그려질 구름의 끝(displacement 1 → 계산 봉 = 마지막 봉)."""
    c = lv.ichimoku_cloud(bars, displacement=1)
    if c is None:
        return None
    a, b = c
    return {"bear": a < b, "thick": abs(a - b) / bars[-1]["c"] * 100.0}


def _rank(bars, now_thick, fn):
    """지금 두께가 직전 봉들의 같은 두께 중 어디쯤인지(0~1). 표본 부족이면 None."""
    hist = []
    start = max(60, len(bars) - DEPTH_RANK_BARS)
    for i in range(start, len(bars) - 1):
        c = fn(bars[:i + 1])
        if c:
            hist.append(c["thick"])
    if len(hist) < 100:
        return None
    return sum(1 for x in hist if x < now_thick) / len(hist)


def _label(cloud, rank):
    if cloud is None:
        return "구름 계산 불가"
    kind = "하락" if cloud["bear"] else "상승"
    size = "" if rank is None else (" 두꺼움" if rank >= 2 / 3 else (" 얇음" if rank < 1 / 3 else " 보통"))
    return f"{kind} 구름{size} {cloud['thick']:.1f}%"


def _pct(v):
    return ("+" if v >= 0 else MINUS) + f"{abs(v):.1f}%"


def tf_state(bars):
    """마감 봉들로 한 주기의 상태 dict."""
    closes = [b["c"] for b in bars]
    px = closes[-1]
    side, n = stoch_streak(stoch_rsi_k(closes))
    cur = _cloud_at(bars)
    lead = _lead_cloud(bars)
    out = {"close": px, "stoch": side, "stoch_n": n, "cur": cur, "lead": lead,
           "cur_rank": _rank(bars, cur["thick"], _cloud_at) if cur else None,
           "lead_rank": _rank(bars, lead["thick"], _lead_cloud) if lead else None}
    if cur:
        out["pos"] = "위" if px > cur["top"] else ("아래" if px < cur["bot"] else "안")
    for n_ in (20, 99):
        if len(closes) >= n_:
            ma = sum(closes[-n_:]) / n_
            out[f"ma{n_}"] = (px / ma - 1) * 100
    return out


def tf_line(tf, s):
    parts = []
    if s["stoch"] is not None:
        parts.append(f"Stoch {s['stoch']}" + (f" {s['stoch_n']}연속" if s["stoch_n"] >= 2 else " 마감"))
    parts.append(_label(s["cur"], s["cur_rank"]) + (f", 가격 {s['pos']}" if s.get("pos") else ""))
    mas = " ".join(f"{n}선 {_pct(s[f'ma{n}'])}" for n in (20, 99) if f"ma{n}" in s)
    if mas:
        parts.append(mas)
    return f"{tf:>3}  " + " · ".join(parts)


def board(fetch=lv.fetch_klines, now_ms=None):
    import time
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    lines, leads, fails, px = [], [], [], None
    for tf in TFS:
        try:
            bars = lv.closed_only(fetch(SYMBOL, tf, limit=FETCH), now_ms)
            if len(bars) < 120:
                raise ValueError(f"봉 {len(bars)}개")
            s = tf_state(bars)
            px = px or s["close"]
            lines.append(tf_line(tf, s))
            if s["lead"]:
                leads.append(f"{tf} {_label(s['lead'], s['lead_rank'])}")
        except Exception as e:                  # noqa: BLE001 — 한 주기 실패가 판 전체를 막지 않게
            fails.append(tf)
            print(f"[상황판] {tf} 실패: {type(e).__name__}: {e}")
    when = datetime.fromtimestamp(now_ms / 1000, NZ).strftime("%m-%d %H:%M")
    head = f"📋 <b>BTC 상황</b> {px:,.0f} ({when} NZ, 마감 봉 기준)" if px else f"📋 <b>BTC 상황</b> ({when} NZ)"
    out = [head] + lines
    if leads:
        out.append("뒤 구름: " + " · ".join(leads))
    if fails:
        out.append("⚠️ 조회 실패: " + ", ".join(fails))
    out.append("참고용 · 7년 검증에서 구름·스토캐스틱 단독은 우위 거의 없음")
    return "\n".join(out)
