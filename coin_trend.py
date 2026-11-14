"""BTC 일봉 50일선 알림 (v2.71) — 하루 한 번, 선 위/아래 한 줄. 선을 넘은 날은 다른 모양으로.

사용자 지시 2026-11-14 "알림을 다 빼고 50일선 위/아래만 보내줘. 그리고 넘는날엔 따로 알림보내주고."
규칙(현물): UTC 일봉 종가가 50일 평균 위면 들고 있기, 아래면 팔고 쉬기. 장중에는 아무것도 안 한다.
근거: BTCUSDT 5분봉 2020-01 ~ 2026-10 백테스트(프로젝트 문서 `코인-50일선`) — 1배 18.4배 / 최대 낙폭 −58%,
그냥 들고 있기 11.3배 / −77%. 앞뒤 절반 모두 들고 있기보다 나았지만 20·100일선은 갈렸다 — 확실한 장점은
낙폭 감소다. 펀딩비 미반영이라 선물이 아니라 현물 기준이다.

상태는 매번 봉에서 다시 계산한다(어제 상태도 봉으로) — 재시작해도 "넘음" 판정이 틀어지지 않는다.
파일에 남기는 건 마지막으로 보낸 일봉 하나뿐(같은 날 두 번 안 보내려고).
"""
import json
import os
from datetime import datetime, timezone

import signal_levels as lv

SYMBOL = "BTCUSDT"
MA = 50
SETTLE_MS = 60_000      # 일봉 마감 후 이만큼 지나야 확정으로 본다
MINUS = "−"


def _path():
    d = os.environ.get("DATA_DIR", "/tmp")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = "/tmp"
    return os.path.join(d, "coin_trend_sent.json")


def state(day_bars, now_ms):
    """마감된 일봉으로 오늘·어제 상태. 봉이 MA+1개 미만이면 None."""
    closed = sorted((b for b in day_bars if b["T"] + SETTLE_MS <= now_ms), key=lambda b: b["t"])
    if len(closed) < MA + 1:
        return None
    cl = [b["c"] for b in closed]
    ma, ma_prev = sum(cl[-MA:]) / MA, sum(cl[-MA - 1:-1]) / MA
    above, prev_above = cl[-1] > ma, cl[-2] > ma_prev
    return {"day_open_ms": closed[-1]["t"], "close": cl[-1], "ma": ma,
            "above": above, "crossed": above != prev_above, "gap_pct": (cl[-1] / ma - 1) * 100}


def _px(v):
    return f"{v:,.0f}"


def _pct(v):
    return ("+" if v >= 0 else MINUS) + f"{abs(v):.1f}%"


def format_message(s):
    day = datetime.fromtimestamp(s["day_open_ms"] / 1000, timezone.utc).strftime("%m/%d")
    nums = f"{day} 종가 {_px(s['close'])} · 50일선 {_px(s['ma'])} ({_pct(s['gap_pct'])})"
    if s["crossed"]:
        head = "🔔 <b>BTC 50일선 위로 넘음 → 사기</b>" if s["above"] else "🔔 <b>BTC 50일선 아래로 내려감 → 팔기</b>"
    else:
        head = "🟢 BTC 50일선 위 · 들고 있기" if s["above"] else "⚪ BTC 50일선 아래 · 쉬기"
    return f"{head}\n{nums}"


def check(send, fetch=lv.fetch_klines, path=None, now_ms=None):
    """새로 마감된 일봉이 있으면 한 번 보낸다. 몇 분마다 불러도 된다(재시작 뒤 놓친 날도 여기서 보냄)."""
    import time
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    path = path or _path()
    s = state(fetch(SYMBOL, "1d", limit=MA + 5), now_ms)
    if s is None:
        print(f"[50일선] 일봉 부족 — 보내지 않음")
        return None
    try:
        with open(path, encoding="utf-8") as f:
            last = json.load(f).get("day_open_ms")
    except (OSError, ValueError):
        last = None
    if last == s["day_open_ms"]:
        return None
    msg = format_message(s)
    send(msg)
    try:
        with open(path + ".tmp", "w", encoding="utf-8") as f:
            json.dump({"day_open_ms": s["day_open_ms"]}, f)
        os.replace(path + ".tmp", path)
    except OSError as e:
        print(f"[50일선] 저장 실패: {e}")
    return msg
