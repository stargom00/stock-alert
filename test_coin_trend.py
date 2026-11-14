"""v2.71 50일선 알림."""
import json

import coin_trend as ct
import signal_tracker as tr

D = 86_400_000


def days(closes):
    return [{"t": i * D, "T": (i + 1) * D - 1, "o": c, "h": c, "l": c, "c": c} for i, c in enumerate(closes)]


def after(n):            # n번째 일봉(0부터)까지 마감되고 정착한 시각
    return (n + 1) * D + ct.SETTLE_MS


def test_state_above_below_and_cross():
    flat = [100.0] * 50
    s = ct.state(days(flat + [100.0, 110.0]), after(51))
    assert s["above"] and s["crossed"] and s["close"] == 110.0 and s["ma"] == 100.2      # 어제 선 위 아님 → 오늘 위
    s = ct.state(days(flat + [110.0, 111.0]), after(51))
    assert s["above"] and not s["crossed"]
    s = ct.state(days(flat + [110.0, 90.0]), after(51))
    assert not s["above"] and s["crossed"]
    s = ct.state(days(flat + [90.0, 89.0]), after(51))
    assert not s["above"] and not s["crossed"]
    assert ct.state(days(flat), after(49)) is None                                       # 봉 부족
    # 어제 종가가 어제 선(100.002)은 넘고 오늘 선(100.402)은 못 넘는 경우 — 어제 상태는 어제 선으로 재야 한다
    s = ct.state(days(flat + [100.1, 120.0]), after(51))
    assert s["above"] and not s["crossed"]


def test_unsettled_and_running_day_ignored():
    b = days([100.0] * 50 + [100.0, 110.0, 50.0])         # 마지막(진행 중) 봉은 50으로 급락
    assert ct.state(b, after(51))["close"] == 110.0       # 오늘 진행 중 봉은 안 본다
    assert ct.state(b, after(51) - ct.SETTLE_MS - 1)["close"] == 100.0   # 마감 직후 1분은 아직 어제


def test_messages():
    s = {"day_open_ms": D * 20000, "close": 69072.0, "ma": 78410.3, "above": False, "crossed": False,
         "gap_pct": (69072 / 78410.3 - 1) * 100}
    day = "10/04"
    assert ct.format_message(s) == f"⚪ BTC 50일선 아래 · 쉬기\n{day} 종가 69,072 · 50일선 78,410 (−11.9%)"
    assert ct.format_message({**s, "above": True}).startswith("🟢 BTC 50일선 위 · 들고 있기\n")
    assert ct.format_message({**s, "above": True, "crossed": True}).startswith("🔔 <b>BTC 50일선 위로 넘음 → 사기</b>\n")
    assert ct.format_message({**s, "crossed": True}).startswith("🔔 <b>BTC 50일선 아래로 내려감 → 팔기</b>\n")
    assert ct.format_message(s).count("\n") == 1                                        # 두 줄


def test_check_sends_once_per_day(tmp_path):
    p, sent = str(tmp_path / "t.json"), []
    bars = days([100.0] * 50 + [100.0, 110.0, 111.0])
    fetch = lambda *a, **kw: bars
    assert ct.check(sent.append, fetch, p, now_ms=after(51))
    assert ct.check(sent.append, fetch, p, now_ms=after(51) + 3_600_000) is None      # 같은 날 다시 안 보냄
    assert len(sent) == 1 and sent[0].startswith("🔔")
    assert ct.check(sent.append, fetch, p, now_ms=after(52))                            # 다음 날
    assert len(sent) == 2 and sent[1].startswith("🟢")
    assert json.load(open(p))["day_open_ms"] == 52 * D


def test_check_too_few_bars_sends_nothing(tmp_path):
    sent = []
    assert ct.check(sent.append, lambda *a, **kw: days([1.0] * 10), str(tmp_path / "t.json"), now_ms=after(9)) is None
    assert sent == []


def test_signal_alerts_on_by_default_can_mute():
    """v2.72 기본 발송(v2.71의 기본 무음은 사용자 의도와 달라 되돌림). COIN_SIGNAL_ALERTS=0이면 무음."""
    out = []
    tg = lambda msg, chat_id=None: out.append((chat_id, msg))
    send, sig_send = tr.senders(tg, {"COIN_CHAT_ID": "-100"})
    sig_send("🔻 1m 숏")
    send("⚪ BTC 50일선 아래 · 쉬기")
    assert out == [("-100", "🔻 1m 숏"), ("-100", "⚪ BTC 50일선 아래 · 쉬기")]          # 둘 다 코인 방으로
    out.clear()
    _, off = tr.senders(tg, {"COIN_SIGNAL_ALERTS": "0"})
    off("x")
    assert out == []


def test_register_routes_signal_jobs_to_sig_sender(monkeypatch):
    """register가 신호 쪽 잡엔 sig_send, 50일선엔 send를 넘기는지 — 잡을 직접 실행해 본다(무음으로 확인)."""
    class Job:
        def __init__(self, jobs): self.jobs = jobs
        def __getattr__(self, name): return self
        def __call__(self, *a, **kw): return self
        def do(self, fn): self.jobs.append(fn); return self
    jobs, out = [], []
    sched = type("S", (), {"every": lambda self, *a: Job(jobs)})()
    monkeypatch.setenv("COIN_HTTP_ENABLED", "0")
    monkeypatch.setenv("COIN_SIGNAL_ALERTS", "0")
    seen = {}
    monkeypatch.setattr(tr, "daily_summary", lambda send: send("📊 요약"))
    monkeypatch.setattr(tr, "self_detect", lambda send: send("🔻 신호"))
    monkeypatch.setattr(tr, "drain_webhook_queue", lambda send: None)
    monkeypatch.setattr(tr, "track_open_signals", lambda send: None)
    monkeypatch.setattr(tr.cp, "track_positions", lambda send, f: send("#P1 덜어내기"))
    monkeypatch.setattr(tr.st, "track_open", lambda f: None)
    monkeypatch.setattr(tr.pv, "track_open", lambda f: None)
    import coin_trend
    monkeypatch.setattr(coin_trend, "check", lambda send: send("⚪ 50일선"))
    tr.register(sched, lambda msg, chat_id=None: out.append(msg))
    for j in jobs:
        j()
    assert out == ["⚪ 50일선"]
