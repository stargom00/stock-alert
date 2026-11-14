"""tools/spike_paper.py — 급등 숏 v2 종이 매매 판정(네트워크 없음)."""
import sys

import pytest

sys.path.insert(0, "tools")
import spike_paper as sp

M5 = sp.M5


def bars_flat(n, px=1.0, t0=0):
    return [(t0 + i * M5, px, px, px, px) for i in range(n)]


def test_rule_is_frozen():
    r = sp.RULE
    assert (r["spike_pct"], r["lookback_bars"], r["bb_min"], r["dev_min"]) == (40.0, 12, 10.0, 30.0)
    assert (r["tp_pct"], r["sl_pct"], r["hold_hours"], r["block_hours"], r["fee_pct"]) == (20.0, 100.0, 48, 48, 0.10)
    assert r["start"] == "2026-11-15"


def test_spike_threshold_boundary_and_next_open_entry():
    b = bars_flat(20)
    b[14] = (14 * M5, 1.0, 1.399, 1.0, 1.3)
    b[15] = (15 * M5, 1.31, 1.35, 1.2, 1.25)
    assert sp.find_spikes(b, 0, 10**15) == []                       # +39.9%는 신호 아님
    b[14] = (14 * M5, 1.0, 1.401, 1.0, 1.3)
    assert sp.find_spikes(b, 0, 10**15) == [(14 * M5, 15 * M5, 1.31)]   # 진입 = 다음 봉 시가


def test_spike_uses_only_prior_12_bars_low():
    b = bars_flat(30)
    b[2] = (2 * M5, 1.0, 1.0, 0.5, 1.0)                              # 13봉보다 오래된 저점은 안 본다
    b[20] = (20 * M5, 1.0, 1.39, 1.0, 1.3)
    assert sp.find_spikes(b, 15 * M5, 10**15) == []
    b[9] = (9 * M5, 1.0, 1.0, 0.99, 1.0)                             # 12봉 안의 저점 0.99 → 1.39/0.99 = +40.4%
    assert [x[0] for x in sp.find_spikes(b, 15 * M5, 10**15)] == [20 * M5]


def test_gap_blocks_signal_and_window_and_block():
    b = bars_flat(20)
    b[14] = (14 * M5, 1.0, 1.5, 1.0, 1.4)
    assert sp.find_spikes(b, 15 * M5, 10**15) == []                 # 창 밖
    assert sp.find_spikes(b, 0, 14 * M5) == []
    assert sp.find_spikes(b, 0, 10**15, blocked_until=14 * M5) == []
    gap = [x for i, x in enumerate(b) if i != 10]                    # 직전 60분에 빈 봉
    assert sp.find_spikes(gap, 0, 10**15) == []
    two = bars_flat(40)
    two[14] = (14 * M5, 1.0, 1.5, 1.0, 1.0); two[30] = (30 * M5, 1.0, 1.5, 1.0, 1.0)
    assert len(sp.find_spikes(two, 0, 10**15)) == 1                  # 48시간 안 두 번째는 차단


def test_indicators_match_channel_definition():
    closes = [1.0] * 20
    bb, dev = sp.indicators(closes, 2.0)
    m = 21 / 20; sd = ((19 * (1 - m) ** 2 + (2 - m) ** 2) / 20) ** 0.5
    assert bb == pytest.approx((2 / (m + 2 * sd) - 1) * 100)
    assert dev == pytest.approx((2 / (6 / 5) - 1) * 100)


def trade(entry=1.0):
    return sp.new_trade("XUSDT", 0, M5, entry, 15.0, 40.0)


def test_tp_stop_same_bar_is_stop_and_fee():
    t = trade(); assert sp.update_trade(t, [(M5, 1.0, 1.1, 0.8, 0.9)])
    assert (t["result"], t["pnl_pct"]) == ("tp", pytest.approx(19.9))
    t = trade(); sp.update_trade(t, [(M5, 1.0, 2.0, 0.8, 1.0)])
    assert (t["result"], t["pnl_pct"]) == ("stop", pytest.approx(-100.1))
    t = trade(); assert not sp.update_trade(t, [(M5, 1.0, 1.99, 0.801, 1.5)])
    assert t["status"] == "open" and t["max_up_pct"] == pytest.approx(99.0)


def test_time_exit_at_last_close_within_48h():
    end = M5 + 48 * sp.H
    t = trade()
    assert not sp.update_trade(t, [(M5, 1.0, 1.1, 0.9, 1.05), (end, 1.0, 1.2, 0.9, 1.10)])
    assert sp.update_trade(t, [(end + M5, 0.5, 0.5, 0.5, 0.5)])     # 창 밖 봉의 값은 쓰지 않는다
    assert (t["result"], t["exit_px"], t["pnl_pct"]) == ("time", 1.10, pytest.approx(-10.1))


def test_update_is_incremental():
    t = trade()
    sp.update_trade(t, [(M5, 1.0, 1.1, 0.9, 1.0)])
    sp.update_trade(t, [(M5, 1.0, 9.9, 0.1, 1.0), (2 * M5, 1.0, 1.0, 0.79, 0.8)])   # 이미 본 봉은 다시 안 본다
    assert t["result"] == "tp"


def test_funding_sign_and_window_and_stats():
    assert sp.funding_pct([(10, 0.01), (20, -0.02), (30, 0.5)], 10, 20) == pytest.approx(-2.0)   # (entry, exit]
    a, b, c = trade(), trade(), trade()
    sp.update_trade(a, [(M5, 1, 1, 0.8, 0.8)]); sp.update_trade(b, [(M5, 1, 2, 1, 2)])
    a["fund_pct"] = -1.0
    s = sp.stats([a, b, c])
    assert (s["n"], s["open"], s["tp"], s["stop"]) == (2, 1, 1, 1)
    assert s["mean_pct"] == pytest.approx((19.9 - 100.1) / 2)
    assert (s["fund_known"], s["mean_with_fund_pct"]) == (1, pytest.approx(18.9))
    assert "mean_pct" not in sp.stats([c])


def test_run_end_to_end(tmp_path, monkeypatch):
    from datetime import date
    d0, d1 = date(2026, 11, 14), date(2026, 11, 15)
    t1 = sp.day_ms(d1)
    day = bars_flat(288, 1.0, t1)
    day[100] = (t1 + 100 * M5, 1.0, 1.5, 1.0, 1.45)
    day[101] = (t1 + 101 * M5, 1.44, 1.46, 1.1, 1.12)               # 진입 1.44 → 익절가 1.152 도달
    data = {("AUSDT", d1): day, ("AUSDT", d0): bars_flat(288, 1.0, sp.day_ms(d0)),
            ("BTCUSDT", d1): bars_flat(288, 9.0, t1), ("BTCUSDT", d0): bars_flat(288, 9.0, sp.day_ms(d0))}
    monkeypatch.setattr(sp, "s3_list", lambda p: ([f"x/{s}/" for s in ["AUSDT", "BTCUSDT"] + [f"Z{i}USDT" for i in range(250)]], []))
    monkeypatch.setattr(sp, "fetch_5m", lambda s, d: data.get((s, d)) or (bars_flat(288, 2.0, sp.day_ms(d)) if s.startswith("Z") and d <= d1 else []))
    monkeypatch.setattr(sp, "fetch_prev_closes", lambda s, d: [1.0] * 20)
    monkeypatch.setattr(sp, "fetch_funding", lambda s, a, b: [(t1 + 101 * M5 + 1, -0.003)])
    p = str(tmp_path / "ledger.json")
    sp.run(p, today=date(2026, 11, 17))
    import json
    led = json.load(open(p))
    assert led["last_day"] == "2026-11-15"                          # 11-16 파일 없음 → 거기서 멈춤
    assert len(led["trades"]) == 1
    t = led["trades"][0]
    assert (t["sym"], t["entry"], t["result"], t["fund_pct"]) == ("AUSDT", 1.44, "tp", pytest.approx(-0.3))
    sp.run(p, today=date(2026, 11, 17))                             # 다시 돌려도 중복 없음
    assert len(json.load(open(p))["trades"]) == 1
