"""v2.37 — signal_tracker: 메시지·중복 제거·결과 추적·요약·실데이터 재생 테스트."""
import json
import os
from datetime import datetime, timezone

import pytest

import signal_levels as lv
import signal_tracker as tr

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "btcusdt_5m_20261015_0830utc.json")
MIN = 60_000


def utc_ms(*a):
    return int(datetime(*a, tzinfo=timezone.utc).timestamp() * 1000)


def base_sig(**kw):
    s = {"id": 42, "symbol": "BTCUSDT", "tf": "5m", "side": "short", "entry": 100.0,
         "bar_open_ms": utc_ms(2026, 10, 15, 8, 0), "tp": 99.0, "tp_pct": 1.0, "ref_swing": None,
         "cloud": None, "htf_rsi": None, "source": "self",
         "status": "open", "result": None, "bars": 0,
         "max_high": None, "min_low": None, "last_close": None, "mfe_pct": 0.0, "mae_pct": 0.0,
         "result_ms": None, "result_bars": None, "result_pct": None}
    s.update(kw)
    s["bar_close_ms"] = s["bar_open_ms"] + 299_999
    s.setdefault("checked_until_ms", s["bar_close_ms"])
    return s


def m1(sig, k, h, l, c=None):
    """신호 마감 후 k번째(1부터) 1m 봉."""
    t = sig["bar_close_ms"] + 1 + (k - 1) * MIN
    return {"t": t, "T": t + MIN - 1, "o": (h + l) / 2, "h": h, "l": l, "c": (h + l) / 2 if c is None else c}


FAR = utc_ms(2030, 1, 1)


@pytest.fixture(autouse=True)
def _isolated_data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))


# ── 추적 ──

def test_tp_hit_single_target():
    s = base_sig()
    assert tr.update_signal(s, [m1(s, 1, 100.4, 99.2)], FAR) == []
    msgs = tr.update_signal(s, [m1(s, 1, 100.4, 99.2), m1(s, 2, 99.5, 98.9)], FAR)
    assert (s["result"], s["status"], s["result_bars"]) == ("hit_tp", "done", 2)
    assert s["result_ms"] == s["bar_close_ms"] + 1 + 2 * MIN
    assert msgs == ["#0042 숏 → TP (+1.00%, 2분)"]
    assert (s["mfe_pct"], s["mae_pct"]) == (pytest.approx(1.1), pytest.approx(0.4))


def test_expire_after_24h():
    s = base_sig()
    bars = [m1(s, k, 100.3, 99.8, c=100.0) for k in range(1, 1440)]
    bars.append(m1(s, 1440, 100.3, 99.8, c=100.2))
    bars.append(m1(s, 1441, 110, 90))   # 만료 이후 봉 — 무시돼야
    now = bars[-1]["T"] + 1
    assert tr.update_signal(s, bars[:1000], now) == []
    assert s["status"] == "open"
    msgs = tr.update_signal(s, bars, now)
    assert s["result"] == "expired" and s["status"] == "done" and s["bars"] == 1440
    assert s["result_pct"] == pytest.approx(-0.2)          # 숏인데 종가 100.2
    assert msgs == ["#0042 숏 → 만료 24시간 미도달 (종가 −0.20%)"]


def test_unclosed_bar_ignored():
    s = base_sig()
    b = m1(s, 1, 101.5, 99.5)
    assert tr.update_signal(s, [b], now_ms=b["T"] - 10) == []
    assert s["status"] == "open" and s["bars"] == 0


# ── 메시지 ──

def test_signal_message_format():
    s = base_sig(entry=82050.0, side="long", tp=82460.25, tp_pct=0.5, ref_swing=82300.0,
                 bar_open_ms=utc_ms(2026, 10, 15, 8, 0),
                 cloud={"pos": "above", "thick_pct": 1.5, "bias": "편"},
                 htf_rsi={"value": 38.2, "dir": "↓"},
                 position_note="포지션 #P12 추가 2회차 (평단 81,100 → 81,153)")
    assert tr.format_signal_message(s).split("\n") == [            # v2.49 3덩어리 배치
        "🔺 BTCUSDT <b>5m 롱</b> 82,050 · 08:05 UTC (21:05 NZ)",
        "🎯 TP 82,460 (+0.5% · 20배 ROI +10%)",
        "💀 청산 78,358 (−4.50%) · 참고 1m 직전 고점 82,300 (+0.30%)",
        "",
        "<b>판정</b>",
        "30m 구름 위·롱 편 (두께 1.5%) · 2h RSI 38↓",
        "포지션 #P12 추가 2회차 (평단 81,100 → 81,153)",
        "",
        "#0042 · 현물 기준",
    ]
    short = tr.format_signal_message(base_sig(tp=99.7, tp_pct=0.3)).split("\n")
    assert "🎯 TP 99.700 (−0.3% · 20배 ROI +6%)" in short and any("참고 1m 직전 저점 없음" in x for x in short)


def test_ingest_klines_451_reason_in_message(tmp_path):
    """v2.39 #0001 사고: 봉 조회 전부 451 → "없음/계산 실패" 대신 사유 한 줄."""
    def fetch_451(*a, **kw):
        raise lv.KlinesError("HTTP 451 (data-api.binance.vision) → HTTP 451 (api.binance.com)")

    sent = []
    sig = tr.ingest_signal("BTCUSDT", "1m", "long", 80855.7, utc_ms(2026, 10, 15, 10, 47),
                           "webhook", sent.append, fetch_451, str(tmp_path / "s.json"))
    assert set(sig["errors"]) == {"bar", "ref", "cloud", "htf_rsi", "pullback", "htf_signal"}   # v2.49 15m 반대 신호 포함
    assert (sig["tp"], sig["suppressed"]) == (pytest.approx(80855.7 * 1.005), False)   # TP는 조회 불필요
    assert (sig["pullback_ok"], sig["ref_extreme"], sig["dist_pct"]) == (None, None, None)
    lines = sent[0].split("\n")
    assert lines[1:] == [                                            # v2.49 배치
        "🎯 TP 81,260 (+0.5% · 20배 ROI +10%)",
        "💀 청산 77,217 (−4.50%)",
        "",
        "<b>판정</b>",
        "되돌림 —",
        "30m 구름 — · 2h RSI —",
        "⚠️ 확인봉·1m 참고·30m 구름·2h RSI·되돌림·15m 반대 신호 조회 실패: "
        "HTTP 451 (data-api.binance.vision) → HTTP 451 (api.binance.com)",
        "포지션: 신규 1차 #P1",
        "",
        "#0001 · 현물 기준",
    ]
    assert "없음" not in sent[0] and "계산 실패" not in sent[0]


# ── 수신·중복 ──

def _fake_fetch_levels(symbol, tf, limit=500, end_ms=None, start_ms=None):
    return []   # 봉 없음 → 레벨·맥락 None (억제 판정 대상 아님 → 발송)


def test_ingest_dedup_n_bars(tmp_path):
    p = str(tmp_path / "s.json")
    sent = []
    t0 = utc_ms(2026, 10, 15, 8, 0)
    a = tr.ingest_signal("BTCUSDT", "5m", "short", 100.0, t0, "webhook", sent.append, _fake_fetch_levels, p)
    assert a["id"] == 1
    assert tr.ingest_signal("BTCUSDT", "5m", "short", 100.0, t0, "self", sent.append, _fake_fetch_levels, p) is None
    assert tr.ingest_signal("BTCUSDT", "5m", "short", 99.0, t0 + 11 * 300_000, "self", sent.append, _fake_fetch_levels, p) is None
    opp = tr.ingest_signal("BTCUSDT", "5m", "long", 99.0, t0 + 300_000, "self", sent.append, _fake_fetch_levels, p)
    assert opp["id"] == 2 and opp["opposite_of"] == 1 and opp["sent"]       # v2.47 반대 방향도 발송
    assert opp["reason"] == "opposite" and len(sent) == 2
    assert "#P1 반대 보유" in sent[-1]
    assert tr.ingest_signal("BTCUSDT", "5m", "short", 99.0, t0 + 12 * 300_000, "self", sent.append, _fake_fetch_levels, p)["id"] == 3
    assert len(sent) == 3
    saved = tr.load_signals(p)
    assert [s["source"] for s in saved] == ["webhook", "self", "self"]
    assert saved[0]["status"] == "open" and saved[0]["tp"] == pytest.approx(99.5)


def test_dedup_per_timeframe(tmp_path):
    assert lv.dedup_bars("5m") == 12 and lv.dedup_bars("1m") == 30
    p = str(tmp_path / "s.json")
    sent = []
    t0 = utc_ms(2026, 10, 15, 8, 0)
    ing = lambda t: tr.ingest_signal("BTCUSDT", "1m", "short", 100.0, t, "self", sent.append, _fake_fetch_levels, p)
    assert ing(t0)["id"] == 1
    assert ing(t0 + 29 * MIN) is None        # 1m은 30봉 안이면 중복
    assert ing(t0 + 30 * MIN)["id"] == 2


def test_parse_dedup():
    assert lv._parse_dedup("5m:12,1m:30") == {"*": 12, "5m": 12, "1m": 30}
    assert lv._parse_dedup("20") == {"*": 20}
    assert lv._parse_dedup("5m:x,1m:30") == {"*": 12, "1m": 30}


# ── 실데이터 재생 (바이낸스 BTCUSDT 5m, 10-11 21:15 ~ 10-15 08:30 UTC) ──

def _fixture_bars():
    with open(FIXTURE) as f:
        return lv.parse_kline_rows(json.load(f))


def _replay(now_ms):
    """now_ms 시각에 self_detect를 돌렸을 때 발송된 신호들."""
    bars = _fixture_bars()

    def fetch(symbol, tf, limit=500, end_ms=None, start_ms=None):
        if tf != "5m":
            raise RuntimeError("context not in fixture")
        hi = end_ms if end_ms is not None else now_ms
        return [b for b in bars if b["t"] <= hi][-limit:]

    sent, got = [], []
    tr._self_next_due.clear()
    tr._self_last_bar.clear()
    orig, orig_targets = tr.ingest_signal, tr.CONFIG["SELF_TARGETS"]
    try:
        tr.ingest_signal = lambda *a, **kw: got.append(a[:5])
        tr.CONFIG["SELF_TARGETS"] = [("BTCUSDT", "5m")]   # 픽스처는 5m만
        tr.self_detect(sent.append, fetch=fetch, now_ms=now_ms)
    finally:
        tr.ingest_signal, tr.CONFIG["SELF_TARGETS"] = orig, orig_targets
    return got


def test_replay_self_detect_1014_0815_short():
    # ②확인봉 08:10~08:15 UTC 음봉, ①RSI봉 RSI 79.8 → 08:15 UTC 숏
    got = _replay(utc_ms(2026, 10, 14, 8, 15, 3))
    assert len(got) == 1
    sym, tf, side, entry, bar_open = got[0]
    assert (sym, tf, side, bar_open) == ("BTCUSDT", "5m", "short", utc_ms(2026, 10, 14, 8, 10))
    assert entry == pytest.approx(next(b["c"] for b in _fixture_bars() if b["t"] == bar_open))
    assert _replay(utc_ms(2026, 10, 14, 8, 20, 3)) == []   # 다음 봉엔 신호 없음


@pytest.mark.xfail(strict=True, reason=(
    "사용자가 1m 신호(② 08:02 UTC)를 5m로 오인한 케이스, 봇 판정이 맞음 — "
    "5m ①RSI봉(07:55) RSI 67.72 < 70이라 5m 신호 없음"))
def test_replay_self_detect_1015_0800_tv_alarm():
    got = _replay(utc_ms(2026, 10, 15, 8, 0, 3))
    assert got and got[0][2] == "short"


# ── Webhook 파싱 ──

def test_parse_webhook_tradingview_placeholders():
    w = tr.parse_webhook_payload(json.dumps(
        {"symbol": "BTCUSDT.P", "tf": "5", "side": "short", "close": "81353.3",
         "time": "2026-10-15T07:55:00Z"}).encode())
    assert w == {"symbol": "BTCUSDT", "tf": "5m", "side": "short", "close": 81353.3,
                 "bar_open_ms": utc_ms(2026, 10, 15, 7, 55)}


@pytest.mark.parametrize("body", [b"not json", b"[1,2]",
                                  b'{"symbol":"BTCUSDT","tf":"7","side":"short","close":1,"time":0}',
                                  b'{"symbol":"BTCUSDT","tf":"5m","side":"up","close":1,"time":0}',
                                  b'{"symbol":"BTCUSDT","tf":"5m","side":"long","close":-1,"time":0}',
                                  b'{"symbol":"BTCUSDT","tf":"5m","side":"long","close":1}'])
def test_parse_webhook_rejects(body):
    with pytest.raises(ValueError):
        tr.parse_webhook_payload(body)


# ── v2.40 알림 억제·교차 중복 (v2.41: 억제는 목표1 거리만) ──

def no_fetch(*a, **kw):
    """봉 없음. 포지션 따라잡기가 실제 바이낸스를 부르지 않게(날짜 따라 결과가 바뀜)."""
    return []


def _ctx(range_pct=None, tp=None):
    return {"tp": tp, "tp_pct": 0.5, "ref_swing": None, "confirm_range_pct": range_pct,
            "cloud": None, "htf_rsi": None, "errors": {}}


def test_suppress_still_confirm_bar(tmp_path):
    assert tr.suppress_reason(0.019) == "미동 확인봉"
    assert tr.suppress_reason(0.02) is None                     # 0.02%는 발송
    assert tr.suppress_reason(None) is None                     # 봉 못 받으면 판정 안 함
    # ② 확인봉(08:00~08:05) 고가−저가 = 0.015 / 시가 100 → 0.015% → 억제, 저장·기록은 함
    t0 = utc_ms(2026, 10, 16, 8, 0)
    bar = {"t": t0, "T": t0 + 299_999, "o": 100.0, "h": 100.05, "l": 99.95, "c": 99.985}   # v2.51 몸통 0.015%(고저 0.1%)

    def fetch(symbol, tf, limit=500, end_ms=None, start_ms=None):
        return [bar] if tf == "5m" else []

    p, sent = str(tmp_path / "s.json"), []
    sig = tr.ingest_signal("BTCUSDT", "5m", "short", 99.998, t0, "self", sent.append, fetch, p)
    assert sent == []
    assert (sig["suppressed"], sig["suppress_reason"], sig["sent"], sig["position_id"]) == \
        (True, "미동 확인봉", False, None)
    assert sig["confirm_range_pct"] == pytest.approx(0.015)
    s = tr.load_signals(p)[0]
    tr.track_open_signals(sent.append, fetch=lambda *a, **kw: [m1(s, 1, 100.1, 99.4)], path=p, now_ms=FAR)
    assert sent == [] and tr.load_signals(p)[0]["result"] == "hit_tp"      # 통계용 기록


def test_cross_tf_duplicate(tmp_path, monkeypatch):
    p = str(tmp_path / "s.json")
    monkeypatch.setattr(tr.lv, "build_signal_context", lambda *a, **kw: _ctx())
    sent = []
    a = tr.ingest_signal("BTCUSDT", "5m", "short", 100.0, utc_ms(2026, 10, 16, 8, 0), "self", sent.append, no_fetch, path=p)
    assert a["sent"]
    # 1m ② 마감 08:15 = 5m 마감 08:05 + 10분 → 중복
    b = tr.ingest_signal("BTCUSDT", "1m", "short", 100.0, utc_ms(2026, 10, 16, 8, 14), "self", sent.append, no_fetch, path=p)
    assert (b["duplicate"], b["dup_of"], b["sent"], b["position_id"]) == (True, a["id"], False, None)
    # v2.42 반대 순서: 1m 발송 뒤 10분 안 5m → 중복 아님, 확인 신호로 발송
    d = tr.ingest_signal("ETHUSDT", "1m", "long", 10.0, utc_ms(2026, 10, 16, 9, 0), "self", sent.append, no_fetch, path=p)
    e = tr.ingest_signal("ETHUSDT", "5m", "long", 10.0, utc_ms(2026, 10, 16, 9, 5), "self", sent.append, no_fetch, path=p)
    assert d["sent"] and e["sent"] and not e["duplicate"]
    assert (e["confirm_of"], e["position_id"]) == (d["id"], d["position_id"])
    # 10분 초과면 중복 아님 (1m 마감 09:01, 5m 마감 09:15 → 14분)
    g = tr.ingest_signal("SOLUSDT", "1m", "short", 1.0, utc_ms(2026, 10, 16, 9, 0), "self", sent.append, no_fetch, path=p)
    h = tr.ingest_signal("SOLUSDT", "5m", "short", 1.0, utc_ms(2026, 10, 16, 9, 10), "self", sent.append, no_fetch, path=p)
    assert g["sent"] and h["sent"] and not h["duplicate"]
    assert len(sent) == 5


def test_1m_then_5m_confirm_binds_position(tmp_path, monkeypatch):
    p, pp = str(tmp_path / "s.json"), str(tmp_path / "positions.json")
    ctxs = iter([_ctx(tp=100.5), _ctx(tp=100.6)])
    monkeypatch.setattr(tr.lv, "build_signal_context", lambda *a, **kw: next(ctxs))
    sent = []
    for _ in range(19):     # 신호ID #0020이 되도록 다른 심볼로 채움(저장만)
        tr.save_signals(tr.load_signals(p) + [base_sig(id=len(tr.load_signals(p)) + 1, symbol="XRPUSDT",
                                                       sent=False)], p)
    cp = tr.cp
    for i in range(5):      # #P6이 되도록 종료된 포지션 5개
        cp.save_positions(cp.load_positions(pp) + [{"id": i + 1, "symbol": "BTCUSDT", "status": "closed"}], pp)
    a = tr.ingest_signal("BTCUSDT", "1m", "long", 100.0, utc_ms(2026, 10, 16, 9, 0), "self", sent.append,
                         fetch=lambda *a, **kw: [], path=p, pos_path=pp)
    assert (a["id"], a["position_id"], a["position_note"]) == (20, 6, "포지션: 신규 1차 #P6")
    # 5m ② 마감 09:05 (1m 마감 09:01 + 4분)
    b = tr.ingest_signal("BTCUSDT", "5m", "long", 100.1, utc_ms(2026, 10, 16, 9, 0), "self", sent.append,
                         fetch=lambda *a, **kw: [], path=p, pos_path=pp)
    assert b["sent"] and not b["duplicate"]
    assert (b["confirm_of"], b["position_id"], b["position_action"]) == (20, 6, "confirm")
    assert "1m #0020 → 5m 확인 (포지션 #P6 유지)" in sent[-1].split("\n")
    assert "포지션:" not in sent[-1] and "추가" not in sent[-1]       # 추가·보유 줄 대신 확인 줄
    pos = [x for x in cp.load_positions(pp) if x["id"] == 6][0]
    # v2.44 고정 TP: 1m 때 TP 유지(5m 값으로 바꾸지 않음)
    assert (pos["tp"], pos["qty"], pos["signal_ids"]) == (100.5, 1.0, [20, 21])
    assert pos["events"][-1]["kind"] == "confirm"
    # 반대 순서(5m 먼저, 1m 뒤)는 기존대로 1m 중복
    c = tr.ingest_signal("BTCUSDT", "1m", "long", 100.0, utc_ms(2026, 10, 16, 9, 7), "self", sent.append,
                         fetch=lambda *a, **kw: [], path=p, pos_path=pp)
    assert c is None or (c["duplicate"] and not c["sent"])
    assert len(sent) == 2


def test_suppressed_signal_is_not_cross_dup_reference(tmp_path, monkeypatch):
    p = str(tmp_path / "s.json")
    ctxs = iter([_ctx(range_pct=0.01), _ctx()])
    monkeypatch.setattr(tr.lv, "build_signal_context", lambda *a, **kw: next(ctxs))
    sent = []
    a = tr.ingest_signal("BTCUSDT", "5m", "short", 100.0, utc_ms(2026, 10, 16, 8, 0), "self", sent.append, no_fetch, path=p)
    b = tr.ingest_signal("BTCUSDT", "1m", "short", 100.0, utc_ms(2026, 10, 16, 8, 6), "self", sent.append, no_fetch, path=p)
    assert a["suppressed"] and b["sent"] and not b["duplicate"] and len(sent) == 1


# ── v2.43 신저점/신고점 경고 ──

def test_pullback_check_message_and_fields(tmp_path):
    t0 = utc_ms(2026, 10, 16, 8, 0)
    # 5m 24봉(2h): 최저 81,400 / 최고 81,900. 그보다 오래된 봉의 극값(80,000·83,000)은 창 밖
    bars5 = [{"t": t0 - (25 - i) * 300_000, "T": t0 - (24 - i) * 300_000 - 1, "o": 81600.0,
              "h": 83000.0 if i == 0 else 81900.0, "l": 80000.0 if i == 0 else (81400.0 if i == 20 else 81500.0),
              "c": 81600.0} for i in range(25)]
    # 신호 봉 뒤(아직 미마감)의 봉은 제외돼야 함
    bars5.append({"t": t0, "T": t0 + 299_999, "o": 81450.0, "h": 81460.0, "l": 81000.0, "c": 81450.0})

    def fetch(symbol, tf, limit=500, end_ms=None, start_ms=None):
        return bars5 if tf == "5m" else []

    close = t0 - 1
    assert lv.pullback_check([b for b in bars5 if b["T"] <= close], "long", 81_448.84)["pullback_ok"] is False
    sent = []
    p = str(tmp_path / "s.json")
    warn = tr.ingest_signal("BTCUSDT", "1m", "long", 81_448.84, close - 59_999, "self", sent.append, fetch, p)
    assert (warn["pullback_ok"], warn["ref_extreme"]) == (False, 81400.0)
    assert warn["dist_pct"] == pytest.approx(0.06, abs=1e-6)
    assert "⚠️ 신저점 구간 (2h 최저 81,400 대비 +0.06%)" in sent[-1].split("\n")
    ok = tr.ingest_signal("ETHUSDT", "1m", "long", 81_490.0, close - 59_999, "self", sent.append, fetch, p)
    assert ok["pullback_ok"] and "되돌림 ✓" in sent[-1].split("\n")       # +0.11%
    sh = tr.ingest_signal("SOLUSDT", "1m", "short", 81_850.0, close - 59_999, "self", sent.append, fetch, p)
    assert (sh["pullback_ok"], sh["ref_extreme"]) == (False, 81900.0)
    assert "⚠️ 신고점 구간 (2h 최고 81,900 대비 −0.06%)" in sent[-1].split("\n")
    assert len(sent) == 3                                                   # 경고여도 발송(억제 안 함)


def test_summary_still_suppressed_and_legacy_split():
    now = utc_ms(2026, 10, 16, 20, 0)
    t = now - 86_400_000
    still = dict(bar_open_ms=t, suppressed=True, sent=False, suppress_reason="미동 확인봉")
    sigs = [base_sig(status="done", result="hit_tp", **still),
            base_sig(status="done", result="expired", **still),
            base_sig(**still),                                             # 기록 중
            base_sig(bar_open_ms=t)]                                       # 일반 발송
    legacy = {k: v for k, v in base_sig(bar_open_ms=t).items() if k not in ("tp", "tp_pct")}
    pos_legacy = {"opened_ms": t, "status": "closed", "result": "hit_target2", "result_pct": 1.0,
                  "adds": [], "mae_pct": 0.1, "trimmed": False}
    lines = tr.format_summary(sigs + [legacy], [pos_legacy], now).split("\n")
    assert lines[0].startswith("📊 코인 요약 (")
    assert "[최근 7일] 포지션 0 (TP 0 · 포기선 0 · 만료 0 / 보유 0)" in lines
    assert "신호: 발송 2 · 억제 3 · 중복 0 · 반대방향 0" in lines           # 이전 신호 1 포함
    assert "미동 억제 3건 → TP 1/2 (50%) (기록 중 1)" in lines
    assert "되돌림 ✓ 신호: TP 0/0 · MAE 평균 — · −2% 선행 0건" in lines      # v2.45 (추적 파일 없음)
    assert "v2.44 이전: 포지션 1 (목표2 1 · 포기선 0 · 만료 0 / 보유 0) · 발송 신호 1" in lines


def test_webhook_pullback_uses_spot_converted_entry(tmp_path):
    t0 = utc_ms(2026, 10, 16, 8, 0)
    # 5m 현물 24봉: 최저 81,400. 신호 봉(07:55~08:00) 현물 종가 81,450 (+0.06%)
    bars5 = [{"t": t0 - (24 - i) * 300_000, "T": t0 - (23 - i) * 300_000 - 1, "o": 81600.0, "h": 81900.0,
              "l": 81400.0 if i == 10 else 81500.0, "c": 81450.0 if i == 23 else 81600.0} for i in range(24)]

    def fetch(symbol, tf, limit=500, end_ms=None, start_ms=None):
        return bars5 if tf == "5m" else []

    sent = []
    p = str(tmp_path / "s.json")
    # 선물 진입 81,540(현물보다 +0.11%) — 선물가로 비교하면 되돌림 ✓로 잘못 나옴
    w = tr.ingest_signal("BTCUSDT", "5m", "long", 81_540.0, t0 - 300_000, "webhook", sent.append, fetch, p)
    assert (w["entry"], w["entry_spot"]) == (81_540.0, 81_450.0)
    assert w["pullback_ok"] is False and w["dist_pct"] == pytest.approx(50 / 81_400 * 100)
    assert "⚠️ 신저점 구간 (2h 최저 81,400 대비 +0.06%)" in sent[-1].split("\n")
    assert sent[-1].split("\n")[0].endswith("81,540 · 08:00 UTC (21:00 NZ)")                     # 표시·포지션은 선물가 그대로
    # 자체 판정(현물 진입가)은 환산 안 함
    s_ = tr.ingest_signal("ETHUSDT", "5m", "long", 81_540.0, t0 - 300_000, "self", sent.append, fetch, p)
    assert s_["entry_spot"] is None and s_["pullback_ok"] is True


def test_suppressed_signal_does_not_block_same_tf_next_signal(tmp_path, monkeypatch):
    """v2.44.2 Railway 12:27 #0042 미동 억제 → 12:29~12:38 같은 1m 롱 전부 "중복 무시" 재현."""
    p = str(tmp_path / "s.json")
    ctxs = iter([_ctx(range_pct=0.01), _ctx(), _ctx()])
    monkeypatch.setattr(tr.lv, "build_signal_context", lambda *a, **kw: next(ctxs))
    sent = []
    t = utc_ms(2026, 10, 17, 12, 26)
    a = tr.ingest_signal("BTCUSDT", "1m", "long", 100.0, t, "webhook", sent.append, no_fetch, path=p)
    assert a["suppressed"] and not a["sent"]
    # 같은 봉을 자체 판정이 또 내면 여전히 중복(두 번 저장 안 함)
    assert tr.ingest_signal("BTCUSDT", "1m", "long", 100.0, t, "self", sent.append, no_fetch, path=p) is None
    b = tr.ingest_signal("BTCUSDT", "1m", "long", 100.0, t + 2 * MIN, "self", sent.append, no_fetch, path=p)
    assert b is not None and b["sent"] and len(sent) == 1
    # 발송된 신호는 계속 기준 — 30봉 안 다음 신호는 중복
    assert tr.ingest_signal("BTCUSDT", "1m", "long", 100.0, t + 5 * MIN, "webhook", sent.append, no_fetch,
                            path=p) is None


def test_opposite_signal_sent_with_note_and_counted(tmp_path):
    """v2.47 보유 중 반대방향 신호 → 그대로 발송(참고 한 줄), 요약 반대방향 +1, dedup·추적 기준에 포함."""
    p, tp = str(tmp_path / "s.json"), str(tmp_path / "track.json")
    sent = []
    t0 = utc_ms(2026, 10, 15, 8, 0)
    ing = lambda side, t: tr.ingest_signal("BTCUSDT", "5m", side, 100.0, t, "self", sent.append,
                                           _fake_fetch_levels, p, track_path=tp)
    ing("short", t0)
    n0 = len(sent)
    opp = ing("long", t0 + 300_000)
    assert len(sent) == n0 + 1                                              # 발송 1
    assert "봇 가상 #P1 반대 보유" in sent[-1]
    assert (opp["sent"], opp["reason"], opp["opposite_of"]) == (True, "opposite", 1)
    assert tr.signal_counts(tr.load_signals(p), t0 + 600_000, 7)["opposite"] == 1
    assert [t["signal_id"] for t in tr.st.load_tracks(tp)] == [1, 2]        # 발송됐으니 추적도
    assert tr.is_duplicate(tr.load_signals(p), "BTCUSDT", "5m", "long", t0 + 2 * 300_000)
