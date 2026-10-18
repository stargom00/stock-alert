"""v2.45 — coin_signal_track: 보낸 신호별 72h 독립 추적(TP·MAE·−1/−2% 선행)."""
from datetime import datetime, timezone

import pytest

import coin_signal_track as st

MIN = 60_000


def utc_ms(*a):
    return int(datetime(*a, tzinfo=timezone.utc).timestamp() * 1000)


T0 = utc_ms(2026, 10, 15, 8, 0)      # 목요일 → TP 0.5%
FAR = utc_ms(2030, 1, 1)


def sig(id_, side="long", pullback_ok=True, entry=100.0, tp=100.5, sent=True):
    return {"id": id_, "symbol": "BTCUSDT", "tf": "5m", "side": side, "entry": entry, "tp": tp,
            "tp_pct": 0.5, "pullback_ok": pullback_ok, "sent": sent,
            "bar_open_ms": T0, "bar_close_ms": T0 + 299_999}


def m1(t, k, h, l):
    """추적 시작 후 k번째(1부터) 1m 봉."""
    s = t["start_ms"] + (k - 1) * MIN
    return {"t": s, "T": s + MIN - 1, "o": (h + l) / 2, "h": h, "l": l, "c": (h + l) / 2}


def test_tp_hit_records_minutes_and_mae():
    t = st.new_track(sig(1))
    bars = [m1(t, 1, 100.2, 99.6), m1(t, 2, 100.1, 99.3), m1(t, 3, 100.6, 99.9), m1(t, 4, 101, 90)]
    assert st.update_track(t, bars, FAR)
    assert (t["status"], t["result"], t["tp_minutes"]) == ("done", "hit_tp", 3)
    assert t["mae_pct"] == pytest.approx(0.7)                       # TP 도달 전 최대 역행
    assert (t["touch_1_ms"], t["touch_2_ms"]) == (None, None)        # 4번째 봉(−10%)은 TP 뒤라 안 봄


def test_no_tp_expires_after_72h():
    t = st.new_track(sig(2, side="short", tp=99.5))
    bars = [m1(t, k, 100.4, 99.8) for k in range(1, 72 * 60 + 5)]
    st.update_track(t, bars[:100], FAR)
    assert t["status"] == "open"
    st.update_track(t, bars, FAR)
    assert (t["status"], t["result"], t["tp_minutes"]) == ("done", "expired", None)
    assert t["mae_pct"] == pytest.approx(0.4)
    assert t["checked_until_ms"] + 1 == t["start_ms"] + 72 * 3_600_000


def test_minus2_touch_before_tp():
    t = st.new_track(sig(3, pullback_ok=False))
    bars = [m1(t, 1, 100.0, 98.9), m1(t, 2, 99.5, 97.8), m1(t, 3, 100.5, 99.0)]
    st.update_track(t, bars, FAR)
    assert t["result"] == "hit_tp" and t["tp_minutes"] == 3
    assert t["touch_1_ms"] == t["start_ms"] + MIN                    # 1번째 봉 −1.1%
    assert t["touch_2_ms"] == t["start_ms"] + 2 * MIN                # 2번째 봉 −2.2%
    assert t["mae_pct"] == pytest.approx(2.2)


def test_register_only_sent_and_summary_lines(tmp_path):
    p = str(tmp_path / "track.json")
    assert st.register(sig(9, sent=False), p) is None
    for s_ in (sig(1), sig(2, pullback_ok=True), sig(3, pullback_ok=False), sig(4, pullback_ok=None)):
        st.register(s_, p)
    assert st.register(sig(1), p) is None                            # 같은 신호 두 번 등록 안 함
    ts = st.load_tracks(p)
    assert [t["signal_id"] for t in ts] == [1, 2, 3, 4]
    st.update_track(ts[0], [m1(ts[0], 1, 100.6, 99.8)], FAR)                           # TP, MAE 0.2
    st.update_track(ts[1], [m1(ts[1], k, 100.1, 99.4) for k in range(1, 4400)], FAR)   # 만료, MAE 0.6
    st.update_track(ts[2], [m1(ts[2], 1, 100.0, 97.9), m1(ts[2], 2, 100.6, 99)], FAR)  # −2% 선행 후 TP
    now = T0 + 4 * 86_400_000
    assert st.format_lines(ts, now, 7) == [
        "되돌림 ✓ 신호: TP 1/2 · MAE 평균 −0.40% · −2% 선행 0건",
        "⚠️ 신저점 신호: TP 1/1 · MAE 평균 −2.10% · −2% 선행 1건",
    ]
    ts[0]["status"] = "open"
    assert st.format_lines(ts, now, 7)[0] == "되돌림 ✓ 신호: TP 0/1 · MAE 평균 −0.60% · −2% 선행 0건 (추적 중 1)"


def test_track_open_fetches_and_saves(tmp_path):
    p = str(tmp_path / "track.json")
    st.register(sig(1), p)
    t = st.load_tracks(p)[0]
    calls = []

    def fetch(symbol, tf, limit=500, end_ms=None, start_ms=None):
        calls.append((symbol, tf, start_ms))
        return [m1(t, 1, 100.2, 99.5), m1(t, 2, 100.7, 99.9)]

    st.track_open(fetch, p, now_ms=FAR)
    assert calls == [("BTCUSDT", "1m", t["start_ms"])]
    assert st.load_tracks(p)[0]["result"] == "hit_tp"
