"""v2.79 1분봉 20선 확인 — 숏은 종가가 20선 아래, 롱은 위여야 통과. 아니면 알림 둘째 줄에 경고."""
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


def test_warning_only_when_not_confirmed():
    base = {"side": "short", "tf": "5m", "entry": 100.0, "tp": 99.5}
    assert "⚠️ 1분 20선 위" in tr.warn_items({**base, "ma1m": {"ma": 99, "close": 100, "ok": False}})
    assert "⚠️ 1분 20선 아래" in tr.warn_items({**base, "side": "long", "ma1m": {"ma": 101, "close": 100, "ok": False}})
    assert tr.warn_items({**base, "ma1m": {"ma": 101, "close": 100, "ok": True}}) == []
    assert tr.warn_items({**base, "ma1m": None}) == []     # 옛 기록·조회 실패는 경고 없음(실패는 '일부 조회 실패'가 따로 알림)
