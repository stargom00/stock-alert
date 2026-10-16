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
         "bar_open_ms": utc_ms(2026, 10, 15, 8, 0), "target1": 99.0, "target2": 98.0, "stop": 101.0,
         "rr": 1.0, "cloud": None, "htf_rsi": None, "source": "self",
         "status": "open", "phase": "pre_t1", "result": None, "bars": 0,
         "max_high": None, "min_low": None, "last_close": None, "mfe_pct": 0.0, "mae_pct": 0.0,
         "t1_hit_ms": None, "t1_bars": None, "result_ms": None, "result_bars": None,
         "result_pct": None, "after_t1": None}
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

def test_target1_then_target2_upgrades():
    s = base_sig()
    m_a = tr.update_signal(s, [m1(s, 1, 100.1, 98.9)], FAR)
    assert s["result"] == "hit_target1" and s["status"] == "open" and s["t1_bars"] == 1
    assert m_a == ["#0042 숏 → 목표1 도달 (+1.00%, 1분)"]
    m_b = tr.update_signal(s, [m1(s, 1, 100.1, 98.9), m1(s, 2, 99.5, 98.5), m1(s, 3, 99.0, 97.9)], FAR)
    assert s["result"] == "hit_target2" and s["status"] == "done" and s["result_bars"] == 3
    assert m_b == ["#0042 숏 → 목표2 도달 (+2.00%, 3분)"]
    assert s["mfe_pct"] == pytest.approx(2.1)
    assert s["mae_pct"] == pytest.approx(0.1)


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
    s = base_sig(entry=81206.0, target1=81040.0, target2=80900.0,
                 bar_open_ms=utc_ms(2026, 10, 15, 8, 0),
                 cloud={"pos": "above", "thick_pct": 1.5, "bias": "역"},
                 htf_rsi={"value": 38.2, "dir": "↓"},
                 position_note="포지션 #P12 추가 2회차 (평단 81,100 → 81,153)")
    assert tr.format_signal_message(s).split("\n") == [
        "🔻 BTCUSDT 5m 숏 신호 08:05 UTC (17:05 KST / 21:05 NZ)",
        "진입 참고 81,206",
        "목표1 81,040 (−0.20%)",
        "목표2 80,900 (−0.38%)",
        "청산가(10배 격리): 88,515 (+9.00%)",
        "30m 구름: 위, 두께 1.5% → 숏 역",
        "2h RSI 38 ↓",
        "포지션 #P12 추가 2회차 (평단 81,100 → 81,153)",
        "신호ID #0042",
        "가격 기준: 현물",
    ]


def test_ingest_klines_451_reason_in_message(tmp_path):
    """v2.39 #0001 사고: 봉 조회 전부 451 → "없음/계산 실패" 대신 사유 한 줄."""
    def fetch_451(*a, **kw):
        raise lv.KlinesError("HTTP 451 (data-api.binance.vision) → HTTP 451 (api.binance.com)")

    sent = []
    sig = tr.ingest_signal("BTCUSDT", "1m", "long", 80855.7, utc_ms(2026, 10, 15, 10, 47),
                           "webhook", sent.append, fetch_451, str(tmp_path / "s.json"))
    assert set(sig["errors"]) == {"levels", "cloud", "htf_rsi", "pullback"}
    assert (sig["pullback_ok"], sig["ref_extreme"], sig["dist_pct"]) == (None, None, None)
    lines = sent[0].split("\n")
    assert lines[2:] == [
        "목표 —",
        "청산가(10배 격리): 73,579 (−9.00%)",
        "30m 구름 —",
        "2h RSI —",
        "되돌림 —",
        "⚠️ 목표·30m 구름·2h RSI·되돌림 조회 실패: "
        "HTTP 451 (data-api.binance.vision) → HTTP 451 (api.binance.com)",
        "포지션: 신규 1차 #P1",
        "신호ID #0001",
        "가격 기준: 현물",
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
    assert opp["id"] == 2 and opp["opposite_of"] == 1 and not opp["sent"]   # v2.41 반대 방향 = 신호는 저장만
    assert sent[-1] == "#P1 반대 신호 #0002 발생 (보유 중)"                   # v2.42 포지션 메시지 한 줄
    assert tr.ingest_signal("BTCUSDT", "5m", "short", 99.0, t0 + 12 * 300_000, "self", sent.append, _fake_fetch_levels, p)["id"] == 3
    assert len(sent) == 3
    saved = tr.load_signals(p)
    assert [s["source"] for s in saved] == ["webhook", "self", "self"]
    assert saved[0]["status"] == "open" and saved[0]["target1"] is None


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


def _ctx(target1=None, target2=None):
    return {"target1": target1, "target2": target2, "src": {},
            "cloud": None, "htf_rsi": None, "errors": {}}


def test_suppress_reason_thresholds():
    assert tr.suppress_reason(100.0, 99.9) == "목표1 거리 < 0.15%"
    assert tr.suppress_reason(100.0, 99.85) is None             # 0.15%는 발송
    assert tr.suppress_reason(100.0, None) is None              # 값 없으면 판정 안 함


def test_suppressed_saved_and_tracked_silently(tmp_path, monkeypatch):
    p = str(tmp_path / "s.json")
    monkeypatch.setattr(tr.lv, "build_signal_context", lambda *a, **kw: _ctx(target1=99.9))
    sent = []
    sig = tr.ingest_signal("BTCUSDT", "5m", "short", 100.0, utc_ms(2026, 10, 16, 8, 0), "self", sent.append, no_fetch, path=p)
    assert sent == []
    assert (sig["suppressed"], sig["sent"], sig["position_id"]) == (True, False, None)   # 포지션도 안 만듦
    s = tr.load_signals(p)[0]
    tr.track_open_signals(sent.append, fetch=lambda *a, **kw: [m1(s, 1, 100.1, 99.8)], path=p, now_ms=FAR)
    assert sent == []                                            # 신호 단위 기록은 발송 안 함
    assert tr.load_signals(p)[0]["result"] == "hit_target1"


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


def test_1m_then_5m_confirm_retargets_position(tmp_path, monkeypatch):
    p, pp = str(tmp_path / "s.json"), str(tmp_path / "positions.json")
    ctxs = iter([_ctx(target1=100.2, target2=100.3), _ctx(target1=100.5, target2=101.0)])
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
    assert "1m #0020 → 5m 확인 (목표 상향)" in sent[-1].split("\n")
    assert "포지션" not in sent[-1]                       # 추가·보유 줄 대신 확인 줄
    pos = [x for x in cp.load_positions(pp) if x["id"] == 6][0]
    assert (pos["target1"], pos["target2"], pos["qty"], pos["signal_ids"]) == (100.5, 101.0, 1.0, [20, 21])
    assert pos["events"][-1]["kind"] == "retarget"
    # 반대 순서(5m 먼저, 1m 뒤)는 기존대로 1m 중복
    c = tr.ingest_signal("BTCUSDT", "1m", "long", 100.0, utc_ms(2026, 10, 16, 9, 7), "self", sent.append,
                         fetch=lambda *a, **kw: [], path=p, pos_path=pp)
    assert c is None or (c["duplicate"] and not c["sent"])
    assert len(sent) == 2


def test_suppressed_signal_is_not_cross_dup_reference(tmp_path, monkeypatch):
    p = str(tmp_path / "s.json")
    ctxs = iter([_ctx(target1=99.9), _ctx()])
    monkeypatch.setattr(tr.lv, "build_signal_context", lambda *a, **kw: next(ctxs))
    sent = []
    a = tr.ingest_signal("BTCUSDT", "5m", "short", 100.0, utc_ms(2026, 10, 16, 8, 0), "self", sent.append, no_fetch, path=p)
    b = tr.ingest_signal("BTCUSDT", "1m", "short", 100.0, utc_ms(2026, 10, 16, 8, 6), "self", sent.append, no_fetch, path=p)
    assert a["suppressed"] and b["sent"] and not b["duplicate"] and len(sent) == 1


# ── 요약 (v2.41: 포지션 기준 + 신호 한 줄) ──

def test_signal_counts_line():
    now = utc_ms(2026, 10, 16, 20, 0)
    t = now - 86_400_000
    sigs = [base_sig(bar_open_ms=t), base_sig(bar_open_ms=t, suppressed=True, sent=False),
            base_sig(bar_open_ms=t, duplicate=True, sent=False),
            base_sig(bar_open_ms=t, opposite_of=3, sent=False),
            base_sig(bar_open_ms=now - 20 * 86_400_000)]
    text = tr.format_summary(sigs, [], now)
    lines = text.split("\n")
    assert lines[0].startswith("📊 코인 요약 (")
    assert "신호: 발송 1 · 억제 1 · 중복 1 · 반대방향 보류 1" in lines
    assert "신호: 발송 2 · 억제 1 · 중복 1 · 반대방향 보류 1" in lines   # 30일


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


def test_summary_pullback_breakdown_line():
    now = utc_ms(2026, 10, 16, 20, 0)
    t = now - 86_400_000
    done = dict(bar_open_ms=t, status="done")
    sigs = [base_sig(pullback_ok=True, result="hit_target2", result_pct=0.5, t1_hit_ms=t, mae_pct=0.2, **done),
            base_sig(pullback_ok=True, result="expired", result_pct=-0.3, mae_pct=0.4, **done),
            base_sig(pullback_ok=False, result="expired", result_pct=-0.8, mae_pct=1.2, **done),
            base_sig(pullback_ok=False, bar_open_ms=t),                          # 기록 중 → 제외
            base_sig(pullback_ok=False, sent=False, suppressed=True, **done),    # 미발송 → 제외
            base_sig(**done)]                                                    # v2.43 이전 → 제외
    lines = tr.format_summary(sigs, [], now).split("\n")
    assert "되돌림 ✓ 2건: 목표1 1/2 · 평균 +0.10% · MAE −0.30% | ⚠️ 1건: 목표1 0/1 · 평균 −0.80% · MAE −1.20%" in lines


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
    assert "진입 참고 81,540" in sent[-1].split("\n")                     # 표시·포지션은 선물가 그대로
    # 자체 판정(현물 진입가)은 환산 안 함
    s_ = tr.ingest_signal("ETHUSDT", "5m", "long", 81_540.0, t0 - 300_000, "self", sent.append, fetch, p)
    assert s_["entry_spot"] is None and s_["pullback_ok"] is True
