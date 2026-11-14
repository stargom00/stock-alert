"""spike_alert.py — 급등 숏 알림. 응답 모양은 2026-11-14 실제 Bitget 응답(test_fixtures/bitget_sample.json)."""
import json

import pytest

import spike_alert as sa

M5, H, D = sa.M5, sa.H, 86_400_000
FX = json.load(open("test_fixtures/bitget_sample.json"))
NOW = 1_794_700_000_000 // M5 * M5 + 60_000


def test_parses_real_bitget_shapes():
    bars = sa.fetch_candles("BTCUSDT", "5m", 6, get=lambda url, params: FX["candles_5m"])
    assert len(bars) == 6 and bars[0][0] < bars[-1][0] and bars[1][0] - bars[0][0] == M5
    assert bars[0] == (1794623100000, 67785.0, 67879.8, 66866.5, 67248.8)
    days = sa.fetch_candles("BTCUSDT", "1Dutc", 6, get=lambda url, params: FX["candles_1Dutc"])
    assert days[1][0] - days[0][0] == D and days[0][0] % D == 0          # UTC 일봉
    assert sa.candidates([FX["ticker"]]) == []                              # BTC는 24시간 저가 대비 +40% 아님
    assert float(FX["ticker"]["fundingRate"]) == pytest.approx(0.000055)


def five(spike_high=1.5, base=1.0, cur_t=None, n=14):
    cur_t = cur_t if cur_t is not None else NOW // M5 * M5
    bars = [(cur_t - (n - 1 - i) * M5, base, base, base, base) for i in range(n)]
    bars[-1] = (cur_t, base, spike_high, base, spike_high)
    return bars


def days(close=1.0, now=NOW):
    d0 = now // D * D
    return [(d0 - (24 - i) * D, close, close, close, close) for i in range(25)]      # 마지막은 오늘(진행 중)


def tick(sym="AUSDT", last=1.5, low=1.0, fr="-0.005"):
    return {"symbol": sym, "lastPr": str(last), "low24h": str(low), "fundingRate": fr}


def many(*special):
    return list(special) + [tick(f"Z{i}USDT", 1.0, 0.99) for i in range(150)]


def run(tmp_path, tickers, candles, symbols={"AUSDT"} | {f"Z{i}USDT" for i in range(150)}, now=NOW):
    sent = []
    out = sa.check(sent.append, path=str(tmp_path / "s.json"), now_ms=now, tickers_f=lambda: tickers,
                   candles_f=candles, symbols_f=lambda now_ms: symbols)
    assert out == sent
    return sent


def test_spike_now_boundary_and_gap_and_stale():
    assert sa.spike_now(five(1.399), NOW) is None
    t, up = sa.spike_now(five(1.401), NOW)
    assert t == NOW // M5 * M5 and up == pytest.approx(40.1)
    b = five(1.5); del b[5]
    assert sa.spike_now(b, NOW) is None                                     # 직전 60분에 빈 봉
    assert sa.spike_now(five(1.5), NOW + 2 * M5) is None                    # 낡은 봉
    b = five(1.0); b[-2] = (b[-2][0], 1.0, 1.5, 1.0, 1.2)                   # 방금 마감된 봉이 신호
    assert sa.spike_now(b, NOW)[0] == b[-2][0]
    b = five(1.5, n=14); b[0] = (b[0][0], 1, 1, 0.1, 1)                     # 13봉보다 오래된 저점은 안 본다
    assert sa.spike_now(b, NOW)[1] == pytest.approx(50.0)


def test_prev20_excludes_today_and_requires_continuity():
    ds = days()
    assert sa.prev20_closes(ds, NOW) == [1.0] * 20
    ds2 = [(t, o, h, l, 9.9) if i == 24 else (t, o, h, l, c) for i, (t, o, h, l, c) in enumerate(ds)]
    assert 9.9 not in sa.prev20_closes(ds2, NOW)                            # 진행 중인 오늘 봉은 안 쓴다
    assert sa.prev20_closes(ds[:10], NOW) is None
    assert sa.prev20_closes(ds[:14] + ds[15:], NOW) is None                 # 중간에 하루 빔


def test_sends_once_and_blocks_48h_across_restart(tmp_path):
    c = lambda sym, g, n: five(1.5) if g == "5m" else days()
    sent = run(tmp_path, many(tick()), c)
    assert len(sent) == 1
    m = sent[0]
    assert "AUSDT" in m and "1시간 +50%" in m and "익절 1.2000" in m and "손절 3.0000" in m
    assert "펀딩 −0.500%" in m and "미확인" not in m and "Bitget" in m and "검증 전" in m
    assert run(tmp_path, many(tick()), c) == []                             # 같은 파일(재시작 뒤에도) → 다시 안 보냄
    assert run(tmp_path, many(tick()), c, now=NOW + 47 * H) == []
    c2 = lambda sym, g, n: five(1.5, cur_t=(NOW + 49 * H) // M5 * M5) if g == "5m" else days(now=NOW + 49 * H)
    assert len(run(tmp_path, many(tick()), c2, now=NOW + 49 * H)) == 1      # 48시간 뒤에는 다시


def test_filter_fail_is_silent_but_blocks(tmp_path):
    c = lambda sym, g, n: five(1.5) if g == "5m" else days(close=3.0)       # 일봉이 훨씬 위 → BB·5MA 둘 다 미달
    assert run(tmp_path, many(tick()), c) == []
    st = json.load(open(tmp_path / "s.json"))
    assert st["AUSDT"]["why"] == "filter"
    c_ok = lambda sym, g, n: five(1.5) if g == "5m" else days()
    assert run(tmp_path, many(tick()), c_ok) == []                          # 백테스트와 같게: 걸러져도 48시간 차단


def test_not_on_binance_skipped_and_unknown_list_marked(tmp_path):
    c = lambda sym, g, n: five(1.5) if g == "5m" else days()
    assert run(tmp_path, many(tick()), c, symbols={"BUSDT"} | {f"Q{i}" for i in range(300)}) == []
    assert json.load(open(tmp_path / "s.json"))["AUSDT"]["why"] == "not_binance"
    (tmp_path / "s.json").unlink()
    sent = run(tmp_path, many(tick()), c, symbols=None)                     # 목록을 못 읽음 → 보내되 표시
    assert len(sent) == 1 and "바이낸스 상장 미확인" in sent[0]


def test_failures_do_not_block_or_crash(tmp_path):
    def boom(sym, g, n):
        raise RuntimeError("x")
    assert run(tmp_path, many(tick()), boom) == []
    assert not (tmp_path / "s.json").exists() or "AUSDT" not in json.load(open(tmp_path / "s.json"))
    def daily_fails(sym, g, n):
        if g == "5m":
            return five(1.5)
        raise RuntimeError("x")
    assert run(tmp_path, many(tick()), daily_fails) == []
    c = lambda sym, g, n: five(1.5) if g == "5m" else days()
    assert len(run(tmp_path, many(tick()), c)) == 1                         # 실패는 차단을 남기지 않는다 → 다음 분에 발송
    def bad():
        raise RuntimeError("down")
    assert sa.check(lambda m: 1 / 0, path=str(tmp_path / "t.json"), now_ms=NOW, tickers_f=bad) == []
    (tmp_path / "s.json").unlink()
    assert run(tmp_path, [tick()], c) == []                          # 시세 종목이 너무 적으면 판정 안 함


def test_send_failure_retries_next_minute(tmp_path):
    c = lambda sym, g, n: five(1.5) if g == "5m" else days()
    def bad_send(m):
        raise RuntimeError("telegram")
    sa.check(bad_send, path=str(tmp_path / "s.json"), now_ms=NOW, tickers_f=lambda: many(tick()), candles_f=c,
             symbols_f=lambda n: None)
    assert len(run(tmp_path, many(tick()), c)) == 1


def test_candidates_precheck_only_fetches_movers(tmp_path):
    asked = []
    def c(sym, g, n):
        asked.append(sym)
        return five(1.0) if g == "5m" else days()
    run(tmp_path, many(tick("AUSDT", 1.5, 1.0), tick("CUSDT", 1.3, 1.0)), c)
    assert asked == ["AUSDT"]                                               # +30%짜리는 봉 조회도 안 함


def test_uses_rule_from_paper_module():
    import sys
    sys.path.insert(0, "tools")
    import spike_paper
    assert sa.RULE is spike_paper.RULE and sa.indicators is spike_paper.indicators
