"""coin_board.py — BTC 상황판(네트워크 없음)."""
import math

import coin_board as cb
import signal_levels as lv

H = 3_600_000


def bars_from(closes, step=H, t0=0, wiggle=0.002):
    return [{"t": t0 + i * step, "T": t0 + (i + 1) * step - 1, "o": c, "h": c * (1 + wiggle), "l": c * (1 - wiggle), "c": c}
            for i, c in enumerate(closes)]


def test_stoch_matches_tradingview_definition():
    closes = [100 + 10 * math.sin(i / 7) + i * 0.05 for i in range(200)]
    k = cb.stoch_rsi_k(closes)
    r = lv.rsi_wilder(closes, 14)
    i = 150
    st = []
    for j in (i - 2, i - 1, i):
        w = r[j - 13:j + 1]
        st.append(100 * (r[j] - min(w)) / (max(w) - min(w)))
    assert abs(k[i] - sum(st) / 3) < 1e-9
    assert all(v is None for v in k[:29])                     # RSI 14 + Stoch 14 + K 3 앞부분은 계산 불가


def test_streak_counts_consecutive_extremes():
    assert cb.stoch_streak([50, 0.5, 0.0, 0.9]) == (0, 3)
    assert cb.stoch_streak([0.0, 30, 0.2]) == (0, 1)
    assert cb.stoch_streak([99.5, 100.0]) == (100, 2)
    assert cb.stoch_streak([5.0, 50.0]) == (None, 0)
    assert cb.stoch_streak([None, None]) == (None, 0)


def test_falling_market_reads_bearish_and_below():
    closes = [100 * (0.995 ** i) for i in range(300)]          # 계속 내림
    s = cb.tf_state(bars_from(closes))
    assert s["cur"]["bear"] is True and s["pos"] == "아래"
    assert s["ma10"] < 0 and s["ma20"] < 0 and s["ma99"] < 0
    assert abs(s["close"] / (1 + s["ma10"] / 100) - sum(closes[-10:]) / 10) < 1e-6
    assert s["stoch"] == 0 and s["stoch_n"] >= 2               # 쭉 내리면 Stoch RSI는 바닥에 붙음
    line = cb.tf_line("4h", s)
    assert line.startswith("4h 🔴 구름 아래") and "Stoch 0 ×" in line and "선" not in line   # v2.84 짧은 줄


def test_rising_market_reads_bullish_and_above():
    closes = [100 * (1.004 ** i) for i in range(300)]
    s = cb.tf_state(bars_from(closes))
    assert s["cur"]["bear"] is False and s["pos"] == "위" and s["ma99"] > 0


def test_lead_cloud_is_computed_from_latest_bar():
    up = [100 * (1.004 ** i) for i in range(250)]
    down = [up[-1] * (0.985 ** i) for i in range(1, 20)]        # 최근에 꺾임 → 앞 구름이 먼저 하락으로 바뀜
    s = cb.tf_state(bars_from(up + down))
    assert s["lead"]["bear"] is True and s["cur"]["bear"] is False


def test_board_uses_closed_bars_and_survives_one_failure():
    now = 400 * H + 30 * 60_000
    def fetch(sym, tf, limit=0):
        if tf == "2h":
            raise RuntimeError("down")
        closes = [100_000 * (0.995 ** i) for i in range(401)]
        b = bars_from(closes)
        b[-1]["c"] = 9999.0                                      # 마감 전 봉(T ≥ now) — 쓰이면 안 됨
        b[-1]["T"] = now + 1
        return b
    out = cb.board(fetch=fetch, now_ms=now)
    lines = out.split("\n")
    assert lines[0].startswith("📋 <b>BTC ") and "9,999" not in lines[0] and len(lines[0]) < 40
    assert [l[:3] for l in lines[1:4]] == ["1h ", "4h ", "1d "]                   # 2h는 실패
    assert all(len(l) <= 40 for l in lines), [l for l in lines if len(l) > 40]   # 휴대폰 한 줄
    assert "⚠️ 조회 실패: 2h" in out and out.endswith("참고용 · 7년 검증 우위 없음")
    assert "뒤 구름: 전부 하락" in out
    import re
    m = re.search(r"🎯 일봉 10선 ([\d,]+) \(\+([\d.]+)%\)", out)
    closes = [100_000 * (0.995 ** i) for i in range(401)][:-1]
    ma = sum(closes[-10:]) / 10
    assert m and int(m.group(1).replace(",", "")) == round(ma)
    assert float(m.group(2)) == round((ma / closes[-1] - 1) * 100, 1)


def test_lead_summary():
    assert cb._lead_summary([("1h", True), ("4h", True)]) == "전부 하락"
    assert cb._lead_summary([("1h", False)]) == "전부 상승"
    assert cb._lead_summary([("1h", True), ("4h", False)]) == "1h 하락 · 4h 상승"
    assert cb._lead_summary([]) is None


def test_command_wired():
    src = open("main.py", encoding="utf-8").read()
    assert 'if text in ("상황", "상황판", "코인상황", "비트상황"):' in src and "coin_board.board()" in src
