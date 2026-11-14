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
    assert s["ma20"] < 0 and s["ma99"] < 0
    assert s["stoch"] == 0 and s["stoch_n"] >= 2               # 쭉 내리면 Stoch RSI는 바닥에 붙음
    line = cb.tf_line("4h", s)
    assert line.startswith(" 4h") and "하락 구름" in line and "가격 아래" in line and "Stoch 0" in line and "99선 −" in line


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
        closes = [100 * (0.995 ** i) for i in range(401)]
        b = bars_from(closes)
        b[-1]["c"] = 9999.0                                      # 마감 전 봉(T ≥ now) — 쓰이면 안 됨
        b[-1]["T"] = now + 1
        return b
    out = cb.board(fetch=fetch, now_ms=now)
    lines = out.split("\n")
    assert lines[0].startswith("📋 <b>BTC 상황</b>") and "9,999" not in lines[0]
    assert any(l.startswith(" 1h") for l in lines) and any(l.startswith(" 4h") for l in lines)
    assert "⚠️ 조회 실패: 2h" in out and out.endswith("참고용 · 7년 검증에서 구름·스토캐스틱 단독은 우위 거의 없음")
    assert out.count("뒤 구름:") == 1


def test_command_wired():
    src = open("main.py", encoding="utf-8").read()
    assert 'if text in ("상황", "상황판", "코인상황", "비트상황"):' in src and "coin_board.board()" in src
