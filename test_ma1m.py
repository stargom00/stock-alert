"""v2.79 1분봉 20선 확인 — 숏은 종가가 20선 아래, 롱은 위여야 ok. v2.80부터 기록만 하고 알림에는 안 쓴다."""
import signal_levels as lv
import signal_tracker as tr


def bars(closes):
    return [{"t": i * 60_000, "T": i * 60_000 + 59_999, "o": c, "h": c, "l": c, "c": c} for i, c in enumerate(closes)]


def test_check_uses_last_20_closed_bars():
    b = bars([500.0] * 30 + [100.0] * 19 + [99.0])          # 앞의 500은 20개 밖 — 평균에 안 들어간다
    r = lv.ma1m_check(b, "short")
    assert r["ma"] == (100.0 * 19 + 99.0) / 20 and r["close"] == 99.0
    assert r["ok"] is True and lv.ma1m_check(b, "long")["ok"] is False
    up = bars([100.0] * 19 + [101.0])
    assert lv.ma1m_check(up, "short")["ok"] is False and lv.ma1m_check(up, "long")["ok"] is True
    assert lv.ma1m_check(bars([100.0] * 19), "short") is None
    assert lv.ma1m_check(list(reversed(b)), "short")["close"] == 99.0   # 순서가 뒤집혀 와도 시간순으로


def test_context_records_ma1m():
    def fetch(symbol, tf, limit=0, end_ms=None, start_ms=None):
        return bars([100.0] * 199 + [99.0]) if tf == "1m" else []
    ctx = lv.build_signal_context("BTCUSDT", "1m", "short", 99.0, 199 * 60_000 + 59_999, fetch=fetch)
    assert ctx["ma1m"]["ok"] is True


def test_recorded_but_never_shown_in_alert():
    # v2.80 사용자 지시 "알림은 보내지마. 그냥 데이터에만 기억해줘"
    base = {"side": "short", "tf": "5m", "entry": 100.0, "tp": 99.5, "tp_pct": 0.5}
    bad = {**base, "ma1m": {"ma": 99, "close": 100, "ok": False}}
    assert tr.warn_items(bad) == []
    assert "20선" not in tr.format_signal_message(bad)
