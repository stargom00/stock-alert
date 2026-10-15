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


# ── 추적 ──

def test_same_bar_target_and_stop_is_stop():
    s = base_sig()
    msgs = tr.update_signal(s, [m1(s, 1, 100.2, 99.9), m1(s, 2, 101.2, 98.9)], FAR)
    assert s["result"] == "hit_stop" and s["status"] == "done"
    assert s["result_bars"] == 2
    assert s["result_pct"] == pytest.approx(-1.0)
    assert msgs == ["#0042 숏 → 손절 (−1.00%, 2분)"]


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


def test_target1_then_stop_keeps_target1():
    s = base_sig()
    tr.update_signal(s, [m1(s, 1, 100.1, 98.9), m1(s, 2, 101.5, 99.5)], FAR)
    assert s["result"] == "hit_target1" and s["after_t1"] == "stop" and s["status"] == "done"


def test_long_symmetric_stop():
    s = base_sig(side="long", target1=101.0, target2=102.0, stop=99.0)
    msgs = tr.update_signal(s, [m1(s, 1, 101.1, 98.9)], FAR)
    assert s["result"] == "hit_stop" and msgs == ["#0042 롱 → 손절 (−1.00%, 1분)"]


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
    s = base_sig(entry=81206.0, target1=81190.0, target2=81040.0, stop=81555.0, rr=0.46,
                 bar_open_ms=utc_ms(2026, 10, 15, 8, 0),
                 cloud={"pos": "above", "thick_pct": 1.5, "bias": "역"},
                 htf_rsi={"value": 38.2, "dir": "↓"})
    assert tr.format_signal_message(s).split("\n") == [
        "🔻 BTCUSDT 5m 숏 신호 08:05 UTC (17:05 KST / 21:05 NZ)",
        "진입 참고 81,206",
        "목표1 81,190 (−0.02%) ⚠️",
        "목표2 81,040 (−0.20%)",
        "손절 81,555 (+0.43%)",
        "손익비 0.5 ⚠️",
        "30m 구름: 위, 두께 1.5% → 숏 역",
        "2h RSI 38 ↓",
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
    assert set(sig["errors"]) == {"levels", "cloud", "htf_rsi"}
    lines = sent[0].split("\n")
    assert lines[2:] == [
        "목표/손절 —",
        "30m 구름 —",
        "2h RSI —",
        "⚠️ 목표/손절·30m 구름·2h RSI 조회 실패: "
        "HTTP 451 (data-api.binance.vision) → HTTP 451 (api.binance.com)",
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
    assert tr.ingest_signal("BTCUSDT", "5m", "long", 99.0, t0 + 300_000, "self", sent.append, _fake_fetch_levels, p)["id"] == 2
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


# ── 요약 ──

def test_summary_breakdown():
    now = utc_ms(2026, 10, 15, 20, 0)
    t = now - 86_400_000

    def S(result, rr, bias, mfe, mae, status="done", days_ago=1):
        return base_sig(bar_open_ms=now - days_ago * 86_400_000, result=result, rr=rr,
                        cloud={"pos": "above", "thick_pct": 1, "bias": bias},
                        mfe_pct=mfe, mae_pct=mae, status=status)

    sigs = [S("hit_target1", 1.5, "편", 0.4, 0.1), S("hit_stop", 0.5, "역", 0.1, 0.5),
            S("hit_target2", 2.0, "편", 0.6, 0.2), S("expired", 0.8, "편", 0.2, 0.3),
            S(None, 1.2, "편", 0.0, 0.0, status="open"), S("hit_stop", 1.1, "역", 0.0, 0.4, days_ago=20)]
    s7 = tr.summarize(sigs, now, 7)
    assert (s7["n"], s7["decided"], s7["pending"]) == (5, 4, 1)
    assert s7["t1_rate"] == pytest.approx(50) and s7["stop_rate"] == pytest.approx(25)
    assert s7["expired_rate"] == pytest.approx(25)
    assert s7["avg_mfe"] == pytest.approx(0.325) and s7["avg_mae"] == pytest.approx(0.275)
    assert s7["rr_ge1"] == (pytest.approx(100), 2)
    assert s7["cloud_with"] == (pytest.approx(200 / 3), 3)
    assert s7["cloud_against"] == (pytest.approx(0), 1)
    s30 = tr.summarize(sigs, now, 30)
    assert s30["n"] == 6 and s30["rr_ge1"] == (pytest.approx(200 / 3), 3)
    text = tr.format_summary([s7, s30], now)
    assert "[최근 7일] 신호 5 (판정 4, 진행 1)" in text
    assert "손익비≥1: 목표1 100% (2건)" in text
    assert "30m 구름 편: 목표1 67% (3건) · 역: 0% (1건)" in text


# ── v2.40 알림 억제·교차 중복 ──

def _ctx(target1=None, stop=None, rr=None):
    return {"target1": target1, "target2": None, "stop": stop, "rr": rr, "src": {},
            "cloud": None, "htf_rsi": None, "errors": {}}


def test_suppress_reason_thresholds():
    assert tr.suppress_reason(100.0, 99.0, 0.4) == "손익비 < 0.5"
    assert tr.suppress_reason(100.0, 99.0, 0.5) is None              # 0.5는 발송
    assert tr.suppress_reason(100.0, 99.9, 2.0) == "목표1 거리 < 0.15%"
    assert tr.suppress_reason(100.0, None, None) is None             # 값 없으면 판정 안 함


def test_rr_04_suppressed_but_saved_and_tracked(tmp_path, monkeypatch):
    p = str(tmp_path / "s.json")
    monkeypatch.setattr(tr.lv, "build_signal_context",
                        lambda *a, **kw: _ctx(target1=99.6, stop=101.0, rr=0.4))
    sent = []
    t0 = utc_ms(2026, 10, 16, 8, 0)
    sig = tr.ingest_signal("BTCUSDT", "5m", "short", 100.0, t0, "self", sent.append, path=p)
    assert sent == []
    assert (sig["suppressed"], sig["suppress_reason"], sig["sent"]) == (True, "손익비 < 0.5", False)
    assert tr.load_signals(p)[0]["status"] == "open"
    # 추적은 그대로 — 손절 도달이 기록되지만 결과 한 줄도 발송 안 함
    s = tr.load_signals(p)[0]
    bars = [m1(s, 1, 101.2, 99.9)]
    tr.track_open_signals(sent.append, fetch=lambda *a, **kw: bars, path=p, now_ms=FAR)
    assert sent == []
    assert tr.load_signals(p)[0]["result"] == "hit_stop"


def test_cross_tf_duplicate(tmp_path, monkeypatch):
    p = str(tmp_path / "s.json")
    monkeypatch.setattr(tr.lv, "build_signal_context", lambda *a, **kw: _ctx())
    sent = []
    t5 = utc_ms(2026, 10, 16, 8, 0)                  # 5m ② 08:00~08:05, 마감 08:05
    a = tr.ingest_signal("BTCUSDT", "5m", "short", 100.0, t5, "self", sent.append, path=p)
    assert a["sent"]
    # 1m ② 마감 08:15 = 5m 마감 + 10분 → 중복
    b = tr.ingest_signal("BTCUSDT", "1m", "short", 100.0, utc_ms(2026, 10, 16, 8, 14), "self", sent.append, path=p)
    assert (b["duplicate"], b["dup_of"], b["sent"]) == (True, a["id"], False)
    # 다른 방향은 중복 아님
    c = tr.ingest_signal("BTCUSDT", "1m", "long", 100.0, utc_ms(2026, 10, 16, 8, 6), "self", sent.append, path=p)
    assert c["sent"] and not c["duplicate"]
    assert len(sent) == 2
    # 반대 방향: 1m 발송 뒤 10분 안 5m → 중복
    d = tr.ingest_signal("ETHUSDT", "1m", "long", 10.0, utc_ms(2026, 10, 16, 9, 0), "self", sent.append, path=p)
    e = tr.ingest_signal("ETHUSDT", "5m", "long", 10.0, utc_ms(2026, 10, 16, 9, 5), "self", sent.append, path=p)
    assert d["sent"] and (e["duplicate"], e["dup_of"]) == (True, d["id"])
    # 5m 같은 주기 12봉 안 재신호는 기존 규칙대로 저장도 안 함
    f = tr.ingest_signal("ETHUSDT", "5m", "long", 10.0, utc_ms(2026, 10, 16, 9, 10), "self", sent.append, path=p)
    assert f is None   # 이건 5m 같은 주기 12봉 중복 제거 — 저장 자체 안 함
    # 10분 초과면 중복 아님 (1m 마감 09:01, 5m 마감 09:15 → 14분)
    g = tr.ingest_signal("SOLUSDT", "1m", "short", 1.0, utc_ms(2026, 10, 16, 9, 0), "self", sent.append, path=p)
    h = tr.ingest_signal("SOLUSDT", "5m", "short", 1.0, utc_ms(2026, 10, 16, 9, 10), "self", sent.append, path=p)
    assert g["sent"] and h["sent"] and not h["duplicate"]


def test_suppressed_signal_is_not_cross_dup_reference(tmp_path, monkeypatch):
    p = str(tmp_path / "s.json")
    ctxs = iter([_ctx(target1=99.6, stop=101.0, rr=0.4), _ctx()])
    monkeypatch.setattr(tr.lv, "build_signal_context", lambda *a, **kw: next(ctxs))
    sent = []
    a = tr.ingest_signal("BTCUSDT", "5m", "short", 100.0, utc_ms(2026, 10, 16, 8, 0), "self", sent.append, path=p)
    b = tr.ingest_signal("BTCUSDT", "1m", "short", 100.0, utc_ms(2026, 10, 16, 8, 6), "self", sent.append, path=p)
    assert a["suppressed"] and b["sent"] and not b["duplicate"] and len(sent) == 1


def test_summary_suppressed_and_duplicate_lines():
    now = utc_ms(2026, 10, 16, 20, 0)

    def S(result, **kw):
        return base_sig(bar_open_ms=now - 86_400_000, result=result, status="done", **kw)

    sigs = [S("hit_target1"), S("hit_stop"),
            S("hit_target1", suppressed=True, sent=False), S("hit_stop", suppressed=True, sent=False),
            S("hit_stop", suppressed=True, sent=False), S("hit_target2", duplicate=True, sent=False)]
    s7 = tr.summarize(sigs, now, 7)
    assert (s7["n"], s7["n_suppressed"], s7["n_duplicate"]) == (2, 3, 1)
    assert s7["t1_rate"] == pytest.approx(50)                  # 주 통계는 발송분만
    text = tr.format_summary([s7], now)
    assert "억제 3건 / 중복 1건" in text
    assert "억제 신호: 목표1 33% · 손절 67% (판정 3건)" in text
