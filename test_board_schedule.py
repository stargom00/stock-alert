"""v2.85 상황판 하루 세 번(뉴질랜드 09·15·21시)."""
import pytest

import signal_tracker as tr


def test_default_times_and_override():
    assert tr.board_times({}) == ["09:00", "15:00", "21:00"]
    assert tr.board_times({"COIN_BOARD_TIMES_NZ": "8:05, 20:30"}) == ["08:05", "20:30"]
    assert tr.board_times({"COIN_BOARD_TIMES_NZ": ""}) == []
    with pytest.raises(ValueError):
        tr.board_times({"COIN_BOARD_TIMES_NZ": "25:00"})


def test_registered_in_nz_time(monkeypatch):
    jobs = []

    class Job:
        def __init__(self): self.at_args = None
        def __getattr__(self, name): return self      # seconds·minutes·day 등 아무 단위

        def at(self, *a): self.at_args = a; return self
        def do(self, fn): jobs.append((self.at_args, fn)); return self

    class Sched:
        def every(self, *a): return Job()

    sent = []
    monkeypatch.setattr(tr, "_board", lambda: "📋 판")
    monkeypatch.setattr(tr, "senders", lambda st: (sent.append, sent.append))
    import coin_http
    monkeypatch.setattr(coin_http, "start_server", lambda: None)
    tr.register(Sched(), lambda m, chat_id=None: None)
    board_jobs = [(a, fn) for a, fn in jobs if a and a[0] in ("09:00", "15:00", "21:00") and a[1] == "Pacific/Auckland"]
    times = sorted(a[0] for a, _ in board_jobs)
    assert "15:00" in times and "21:00" in times and times.count("09:00") >= 1
    for a, fn in board_jobs:
        if a[0] == "15:00":
            fn()
    assert sent == ["📋 판"]
