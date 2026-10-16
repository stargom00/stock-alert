"""v2.41 — coin_positions: 채널 방식 가상 포지션(추가·덜어내기·목표·포기선·만료)."""
from datetime import datetime, timezone

import pytest

import coin_positions as cp

MIN = 60_000
T0 = int(datetime(2026, 10, 16, 8, 0, tzinfo=timezone.utc).timestamp() * 1000)   # 1차 신호 ② 마감
FAR = T0 + 10 * 86_400_000


@pytest.fixture
def pp(tmp_path):
    return str(tmp_path / "positions.json")


def sig(i, side, entry, close_ms, tp=None, bias=None):
    return {"id": i, "symbol": "BTCUSDT", "tf": "5m", "side": side, "entry": entry,
            "bar_close_ms": close_ms - 1, "tp": tp, "tp_pct": 1.0 if tp else None,
            "cloud": {"bias": bias} if bias else None}


def bar(k, h, l, c=None):
    """T0 이후 k번째(1부터) 1m 봉."""
    t = T0 + (k - 1) * MIN
    return {"t": t, "T": t + MIN - 1, "o": (h + l) / 2, "h": h, "l": l, "c": (h + l) / 2 if c is None else c}


def no_bars(*a, **kw):
    return []


def test_full_scenario_add_trim_tp(pp):
    # 1차 롱 100, TP 101 (v2.44 단일 TP)
    a, line, msgs, pid = cp.on_signal(sig(1, "long", 100.0, T0, 101.0), no_bars, path=pp, now_ms=T0)
    assert (a, line, msgs) == ("new", "포지션: 신규 1차 #P1", [])
    # 10분 동안 99.4까지 밀림(평단 대비 −0.6%)
    path_bars = [bar(k, 100.0 - 0.06 * (k - 1), 99.94 - 0.06 * (k - 1)) for k in range(1, 11)]
    later = [bar(11, 99.5, 99.3), bar(12, 99.75, 99.5),      # 12번 봉: 평단 99.7 닿음 → 덜어내기
             bar(13, 100.5, 99.8), bar(14, 101.1, 100.4),    # 14번: TP → 종료
             bar(15, 101.5, 100.9)]
    allb = path_bars + later

    def fetch(*a, **kw):
        return allb

    # 같은 방향 신호(② 마감 T0+10분, 99.4) → 따라잡기는 T0+10분까지만, 그다음 추가
    a, line, msgs, pid = cp.on_signal(sig(2, "long", 99.4, T0 + 10 * MIN), fetch, path=pp, now_ms=T0 + 10 * MIN + 5)
    assert a == "add" and msgs == []
    assert line == "포지션 #P1 추가 2회차 (평단 100.00 → 99.700)"
    p = cp.load_positions(pp)[0]
    assert (p["qty"], p["avg"]) == (2.0, pytest.approx(99.7))
    assert p["checked_until_ms"] == T0 + 10 * MIN - 1         # 신호 이후 봉은 아직 안 봄
    assert p["mae_pct"] == pytest.approx(0.6)                  # 추가 전 평단 100 기준 최저 99.4

    sent = []
    cp.track_positions(sent.append, fetch, path=pp, now_ms=FAR)
    p = cp.load_positions(pp)[0]
    assert sent == [
        "#P1 추가분 덜어내기 (평단 복귀, 추가분 +0.30%)",
        "#P1 종료 TP (+1.00%, 14분)",
    ]
    assert [e["kind"] for e in p["events"]] == ["open", "add", "trim", "hit_tp"]
    assert (p["status"], p["result"], p["trimmed"], p["closed_ms"]) == ("closed", "hit_tp", True, T0 + 14 * MIN)
    trim = p["events"][2]
    assert (trim["avg"], trim["qty"]) == (100.0, 1.0)          # 평단 1차 진입가로 복귀, 추가분 제거
    # 결과 = 청산 수량 가중: (덜어내기 1×0.3018% + TP 1×1%) / 2
    assert p["result_pct"] == pytest.approx((1 * (99.7 - 99.4) / 99.4 * 100 + 1 * 1) / 2)


def test_add_not_triggered_below_threshold(pp):
    cp.on_signal(sig(1, "long", 100.0, T0), no_bars, path=pp, now_ms=T0)
    a, line, _, _ = cp.on_signal(sig(2, "long", 99.7, T0 + 10 * MIN), no_bars, path=pp, now_ms=T0 + 10 * MIN)
    assert a == "hold"
    assert line == "포지션 #P1 보유 중, 추가 조건 미달 (평단 대비 −0.30%)"
    p = cp.load_positions(pp)[0]
    assert (p["qty"], p["avg"], p["adds"], p["signal_ids"]) == (1.0, 100.0, [], [1, 2])


def test_weighted_avg_and_third_add_refused(pp):
    cp.on_signal(sig(1, "long", 100.0, T0), no_bars, path=pp)
    assert cp.on_signal(sig(2, "long", 99.4, T0 + 5 * MIN), no_bars, path=pp)[0] == "add"
    a, line, _, _ = cp.on_signal(sig(3, "long", 99.0, T0 + 10 * MIN), no_bars, path=pp)
    p = cp.load_positions(pp)[0]
    # 수량 가중: (100 + 99.4 + 99.0) / 3 — 단순 평균((99.7 + 99.0)/2 = 99.35)이면 실패해야 함
    assert p["avg"] == pytest.approx((100.0 + 99.4 + 99.0) / 3)
    assert (a, p["qty"]) == ("add", 3.0)
    assert line == "포지션 #P1 추가 3회차 (평단 99.700 → 99.467)"
    # 세 번째 추가 시도(역행 1% 이상이어도) → 거절
    a, line, _, _ = cp.on_signal(sig(4, "long", 98.4, T0 + 15 * MIN), no_bars, path=pp)
    assert a == "hold" and "추가 2회 소진" in line
    assert cp.load_positions(pp)[0]["qty"] == 3.0


def test_giveup(pp):
    cp.on_signal(sig(1, "long", 100.0, T0, 101.0), no_bars, path=pp)
    sent = []
    cp.track_positions(sent.append, lambda *a, **kw: [bar(1, 100.1, 98.0), bar(2, 101.5, 96.9)], path=pp, now_ms=FAR)
    p = cp.load_positions(pp)[0]
    assert sent == ["#P1 포기선 (−3.00%)"]
    assert (p["result"], p["result_pct"]) == ("giveup", pytest.approx(-3.0))
    assert [e["kind"] for e in p["events"]] == ["open", "giveup"]   # 같은 봉 TP보다 포기선 먼저(보수적)


def test_short_giveup_and_trim_symmetric(pp):
    cp.on_signal(sig(1, "short", 100.0, T0, 99.0), no_bars, path=pp)
    cp.on_signal(sig(2, "short", 100.6, T0 + 5 * MIN), no_bars, path=pp)      # 역행 0.6% → 추가, 평단 100.3
    sent = []
    cp.track_positions(sent.append, lambda *a, **kw: [bar(6, 100.5, 100.25)], path=pp, now_ms=FAR)
    assert sent == ["#P1 추가분 덜어내기 (평단 복귀, 추가분 +0.30%)"]
    assert cp.load_positions(pp)[0]["avg"] == 100.0


def test_trim_once_and_no_add_after_trim(pp):
    cp.on_signal(sig(1, "long", 100.0, T0), no_bars, path=pp)
    cp.on_signal(sig(2, "long", 99.4, T0 + 5 * MIN), no_bars, path=pp)
    cp.track_positions(lambda m: None, lambda *a, **kw: [bar(6, 99.8, 99.5)], path=pp, now_ms=FAR)
    assert cp.load_positions(pp)[0]["trimmed"]
    a, line, _, _ = cp.on_signal(sig(3, "long", 99.3, T0 + 10 * MIN), no_bars, path=pp)
    assert a == "hold" and "덜어내기 후" in line


def test_expire_72h(pp):
    cp.on_signal(sig(1, "short", 100.0, T0, 99.0), no_bars, path=pp)
    bars = [bar(k, 100.3, 99.9, c=100.1) for k in range(1, 72 * 60 + 2)]
    sent = []
    cp.track_positions(sent.append, lambda *a, **kw: bars, path=pp, now_ms=FAR)
    p = cp.load_positions(pp)[0]
    assert (p["result"], p["closed_ms"]) == ("expired", T0 + 72 * 3_600_000)
    assert sent == ["#P1 만료 72시간 (종가 −0.10%)"]


def test_opposite_signal_no_position_action(pp):
    cp.on_signal(sig(1, "long", 100.0, T0), no_bars, path=pp)
    a, line, _, pid = cp.on_signal(sig(14, "short", 101.0, T0 + 5 * MIN), no_bars, path=pp)
    assert (a, line, pid) == ("opposite", "#P1 반대 신호 #0014 발생 (보유 중)", 1)   # v2.42 알림 한 줄
    ps = cp.load_positions(pp)
    assert len(ps) == 1 and ps[0]["signal_ids"] == [1] and ps[0]["qty"] == 1.0
    assert (ps[0]["events"][-1]["kind"], ps[0]["events"][-1]["signal_id"]) == ("opposite_signal", 14)


def test_catch_up_closes_then_new_position(pp):
    cp.on_signal(sig(1, "long", 100.0, T0), no_bars, path=pp)
    a, line, msgs, pid = cp.on_signal(sig(2, "long", 96.5, T0 + 5 * MIN),
                                      lambda *a, **kw: [bar(3, 99.0, 96.8)], path=pp, now_ms=FAR)
    assert msgs == ["#P1 포기선 (−3.00%)"] and (a, pid) == ("new", 2)


def test_position_summary():
    now = T0 + 86_400_000

    def P(result, pct, adds, bias, mae, trimmed=False, status="closed"):
        return {"opened_ms": T0, "status": status, "result": result, "result_pct": pct,
                "adds": [{}] * adds, "cloud_bias": bias, "mae_pct": mae, "trimmed": trimmed}

    # 두 번째 TP는 v2.44 이전 기록(hit_target2) — TP로 셈
    ps = [P("hit_tp", 1.0, 0, "편", 0.2), P("giveup", -3.0, 2, "역", 3.0, trimmed=False),
          P("hit_target2", 0.6, 1, "편", 0.8, trimmed=True), P("expired", -0.2, 0, None, 0.4),
          P(None, None, 0, "편", 0.1, status="open")]
    s = cp.summarize_positions(ps, now, 7)
    assert s["by_result"] == {"hit_tp": 2, "giveup": 1, "expired": 1}
    assert s["avg_result"] == pytest.approx((1.0 - 3.0 + 0.6 - 0.2) / 4)
    assert s["with_adds"] == (pytest.approx(-1.2), 2) and s["no_adds"] == (pytest.approx(0.4), 2)
    assert s["max_mae"] == 3.0
    assert cp.format_position_summary(s) == [
        "[최근 7일] 포지션 5 (종료 4: TP 2 · 포기선 1 · 만료 1 / 보유 1)",
        "평균 결과 −0.40% · 최대 MAE −3.00%",
        "종료 결과 — 추가 있음: −1.20% (2건) · 없음: +0.40% (2건)",
        "보유 포함 — 추가 2건 · 덜어내기 1건",
        "30m 구름 편: +0.80% (2건) · 역: −3.00% (1건)",
    ]


def test_summary_open_trimmed_position_counts_consistent(pp):
    # 실제 경로 재현: 1차 → 역행 추가 → 평단 복귀 덜어내기 → 아직 보유 중 + 추가 없이 종료된 포지션 1개
    cp.on_signal(sig(1, "long", 100.0, T0), no_bars, path=pp)
    assert cp.on_signal(sig(2, "long", 99.4, T0 + 5 * MIN), no_bars, path=pp)[0] == "add"
    cp.track_positions(lambda m: None, lambda *a, **kw: [bar(6, 99.8, 99.5)], path=pp, now_ms=T0 + 7 * MIN)
    ps = cp.load_positions(pp)
    assert ps[0]["trimmed"] and ps[0]["adds"] and ps[0]["status"] == "open"
    ps.append({"id": 2, "opened_ms": T0, "status": "closed", "result": "giveup", "result_pct": -3.0,
               "adds": [], "cloud_bias": None, "mae_pct": 3.0, "trimmed": False})
    s = cp.summarize_positions(ps, T0 + 3_600_000, 7)
    assert s["trims"] <= s["adds_n"]                    # 덜어내기는 추가분이 있어야만 생김
    lines = cp.format_position_summary(s)
    assert "보유 포함 — 추가 1건 · 덜어내기 1건" in lines
    assert "종료 결과 — 추가 있음: — (0건) · 없음: −3.00% (1건)" in lines


def test_legacy_position_without_tp_uses_target2(pp):
    # v2.44 배포 전에 열린 포지션(목표1/2, tp 없음)은 목표2에서 TP 종료
    legacy = {"id": 1, "symbol": "BTCUSDT", "side": "long", "status": "open", "result": None, "result_pct": None,
              "opened_ms": T0, "closed_ms": None, "signal_ids": [1], "entry1": 100.0,
              "target1": 100.5, "target2": 101.0, "qty": 1.0, "avg": 100.0, "adds": [], "trimmed": False,
              "t1_done": False, "realized": [], "events": [], "checked_until_ms": T0 - 1, "last_close": None,
              "mfe_pct": 0.0, "mae_pct": 0.0}
    cp.save_positions([legacy], pp)
    sent = []
    cp.track_positions(sent.append, lambda *a, **kw: [bar(1, 100.6, 100.0), bar(2, 101.1, 100.5)], path=pp, now_ms=FAR)
    assert sent == ["#P1 종료 TP (+1.00%, 2분)"]
