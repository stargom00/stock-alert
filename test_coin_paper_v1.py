"""설계 v1 종이 매매(v2.69). 규칙 숫자는 사전 등록값이라 테스트가 그대로 고정한다."""
import pytest

import coin_paper_v1 as pv
import signal_tracker as tr

H = 3_600_000
T0 = 1_800_000_000_000


@pytest.fixture(autouse=True)
def _data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))


def test_rule_is_the_registered_one():
    """판정 기간 중 숫자가 바뀌면 여기서 걸린다 — 바꾸려면 version과 문서를 같이 바꿀 것."""
    assert pv.RULE == {"version": "v1", "registered": "2026-11-14", "stop_pct": 3.0, "hold_hours": 24,
                       "run_max": 4, "cap_per_side": 2, "margin": 56.0, "leverage": 20,
                       "fee_tp_pct": 0.07, "fee_other_pct": 0.10}


def sig(i, side="long", fade=True, run_n=1, sent=True, entry=100.0, k=0):
    close = T0 + k * 60_000 - 1
    tp = entry * (1.005 if side == "long" else 0.995)
    return {"id": i, "symbol": "BTCUSDT", "tf": "1m", "side": side, "entry": entry, "tp": tp, "tp_pct": 0.5,
            "bar_close_ms": close, "sent": sent, "run_n": run_n,
            "prev_day": None if fade == "none" else {"dir": "down", "fade": fade, "day_open_ms": 0, "streak": 1}}


def bar(t, k, h, l, c=None):
    o = t["start_ms"] + (k - 1) * 60_000
    return {"t": o, "T": o + 59_999, "o": (h + l) / 2, "h": h, "l": l, "c": c if c is not None else (h + l) / 2}


def test_entry_rules(tmp_path):
    p = str(tmp_path / "p.json")
    assert pv.on_signal(sig(1, sent=False), p) is None and pv.load(p)["skips"] == []      # 미발송은 대상 아님
    assert pv.on_signal(sig(2, fade=False), p) is None                                     # 어제와 같은 방향
    assert pv.on_signal(sig(3, fade="none"), p) is None                                    # 일봉 모름
    assert pv.on_signal(sig(4, fade=None), p) is None                                      # 보합
    assert pv.on_signal(sig(5, run_n=pv.RULE["run_max"] + 1), p) is None                   # 5번째
    a = pv.on_signal(sig(6, run_n=pv.RULE["run_max"]), p)                                  # 4번째까지는 진입
    b = pv.on_signal(sig(7), p)
    assert a["id"] == 1 and b["id"] == 2
    assert pv.on_signal(sig(8), p) is None                                                 # 같은 방향 2개 참
    s = pv.on_signal(sig(9, side="short"), p)                                              # 반대 방향은 따로 센다
    assert s and s["stop"] == pytest.approx(103.0) and a["stop"] == pytest.approx(97.0)
    assert pv.on_signal(sig(6), p) is None and len(pv.load(p)["trades"]) == 3              # 같은 신호 두 번 안 받음
    assert [x["reason"] for x in pv.load(p)["skips"]] == ["prev_day", "no_prev_day", "no_prev_day", "run", "cap"]


def test_cap_frees_after_close(tmp_path):
    p = str(tmp_path / "p.json")
    a, b = pv.on_signal(sig(1), p), pv.on_signal(sig(2), p)
    pv.track_open(lambda *x, **kw: [bar(a, 1, 100.6, 100.0)], p, now_ms=T0 + 10 * H)     # 둘 다 TP
    assert pv.on_signal(sig(3, k=5), p) is not None


def test_exits_tp_stop_and_same_bar_is_stop():
    far = T0 + 100 * H
    t = pv.new_trade(sig(1), 1)
    assert pv.update_trade(t, [bar(t, 1, 100.2, 99.8), bar(t, 2, 100.5, 100.1)], far)
    assert (t["result"], t["exit_px"]) == ("tp", pytest.approx(100.5))
    assert t["pnl_pct"] == pytest.approx(0.5 - 0.07) and t["pnl_usdt"] == pytest.approx(0.43 / 100 * 1120, abs=0.01)
    t = pv.new_trade(sig(2, side="short"), 2)
    pv.update_trade(t, [bar(t, 1, 103.0, 100.0)], far)
    assert t["result"] == "stop" and t["pnl_pct"] == pytest.approx(-3.0 - 0.10)
    t = pv.new_trade(sig(3), 3)
    pv.update_trade(t, [bar(t, 1, 100.6, 96.9)], far)                                    # 한 봉에 둘 다 → 손절
    assert t["result"] == "stop"


def test_time_exit_at_24h_close_not_before():
    t = pv.new_trade(sig(1), 1)
    n = pv.RULE["hold_hours"] * 60
    pv.update_trade(t, [bar(t, k, 100.2, 99.0, c=99.3) for k in range(1, n)], T0 + 100 * H)
    assert t["status"] == "open"                                                           # 23:59까지는 열림
    pv.update_trade(t, [bar(t, n, 99.5, 99.0, c=99.2)], T0 + 100 * H)                    # 24시간째 봉 종가
    assert (t["result"], t["exit_px"], t["exit_ms"]) == ("time", 99.2, t["expire_ms"])
    assert t["pnl_pct"] == pytest.approx(-0.8 - 0.10)
    # 조회 공백: 24h를 넘긴 봉이 먼저 오면 마지막으로 본 종가로 정리
    g = pv.new_trade(sig(2), 2)
    pv.update_trade(g, [bar(g, 5, 100.2, 99.9, c=100.1)], T0 + 100 * H)
    pv.update_trade(g, [bar(g, n + 30, 90.0, 89.0)], T0 + 100 * H)
    assert (g["result"], g["exit_px"]) == ("time", 100.1)


def test_unclosed_bar_ignored():
    t = pv.new_trade(sig(1), 1)
    b = bar(t, 1, 100.6, 100.0)
    assert pv.update_trade(t, [b], b["T"]) is False and t["status"] == "open"            # 아직 진행 중인 봉


def test_summary_line_counts_and_says_when_empty():
    assert "아직 진입 없음" in pv.summary_line([], T0)
    t1 = pv.new_trade(sig(1), 1); pv.update_trade(t1, [bar(t1, 1, 100.6, 100.0)], T0 + H)
    t2 = pv.new_trade(sig(2), 2); pv.update_trade(t2, [bar(t2, 1, 100.0, 96.0)], T0 + H)
    t3 = pv.new_trade(sig(3), 3)
    line = pv.summary_line([t1, t2, t3], T0 + H)
    assert "누적 2건 −$29.9" in line and "TP 1 · 시간 0 · 손절 1" in line and "진행 1" in line and "주문 없음" in line
    assert "24h 0건" in pv.summary_line([t1, t2, t3], T0 + 49 * H)                         # 하루 지난 건 24h 집계에서 빠짐


def test_ingest_feeds_paper_and_summary_shows_it(tmp_path):
    D = 86_400_000
    t0 = T0 // 300_000 * 300_000
    def fetch(symbol, tf, limit=500, end_ms=None, start_ms=None):
        if tf == "1d":
            d0 = t0 // D * D - D
            return [{"t": d0, "T": d0 + D - 1, "o": 100.0, "h": 120.0, "l": 99.0, "c": 118.0}]   # 어제 양봉
        return []
    p, pp, sent = str(tmp_path / "s.json"), str(tmp_path / "p.json"), []
    ing = lambda k, side: tr.ingest_signal("BTCUSDT", "5m", side, 100.0, t0 + k * 12 * 300_000, "self",
                                           sent.append, fetch, p, paper_path=pp)
    ing(0, "short"); ing(1, "long")
    d = pv.load(pp)
    assert [(t["side"], t["signal_id"]) for t in d["trades"]] == [("short", 1)]           # 양봉 뒤 숏만 진입
    assert [(s["side"], s["reason"]) for s in d["skips"]] == [("long", "prev_day")]
    out = tr.format_summary([], [], t0, tracks=[], paper=d["trades"])
    assert "🧪 v1 가상(주문 없음)" in out and "진행 1" in out
    assert "v1 가상" not in tr.format_summary([], [], t0, tracks=[])                       # 안 넘기면 줄 없음
