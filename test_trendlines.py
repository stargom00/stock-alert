"""v2.56 빗각 추적 — 선 값(로그 보간)·접근·종가 이탈/돌파·요약.

사보타주 확인(2026-11-11, FAIL 확인 후 원복):
  ① line_value를 일반(산술) 보간으로 바꿈 → test_log_interpolation FAIL
  ② check_near의 하루 1회 막기 삭제 → test_near_once_per_day FAIL
  ③ close_event가 처음(prev None)에도 알림 → test_first_close_no_event FAIL
"""
from datetime import datetime, timedelta, timezone

import pytest

import trendlines as t

KST = timezone(timedelta(hours=9))
NV = {"id": "nv", "ticker": "NVDA", "name": "엔비디아", "kind": "flat", "p1": ["2025-09-01", 164.07], "p2": ["2026-03-30", 164.07]}
UP = {"id": "up", "ticker": "AAA", "name": "A", "kind": "rise", "p1": ["2024-01-01", 10.0], "p2": ["2025-01-01", 20.0]}


def test_log_interpolation():
    assert t.line_value(UP, "2024-07-02") == pytest.approx(10 * 2 ** (183 / 366))      # 중간 = 기하평균 근처
    assert t.line_value(UP, "2026-01-01") == pytest.approx(10 * 2 ** (731 / 366))     # 연장(같은 속도로 계속)
    assert t.line_value(NV, "2027-01-01") == pytest.approx(164.07)


def test_repo_lines_load_and_values():
    lines = {x["id"]: x for x in t.load_lines()}
    assert t.line_value(lines["nvda-flat-164"], "2026-11-10") == pytest.approx(164.48, abs=0.01)
    assert t.line_value(lines["jpm-rise-2024"], "2026-11-10") == pytest.approx(332.9, abs=0.2)


def test_same_date_rejected():
    with pytest.raises(ValueError):
        t.line_value({"id": "x", "p1": ["2026-01-01", 1], "p2": ["2026-01-01", 2]}, "2026-02-01")


def test_side_and_events():
    assert t.side_of(170, 164.07) == "above" and t.side_of(166, 164.07) == "on" and t.side_of(158, 164.07) == "below"
    assert t.close_event("on", "below") == "이탈" and t.close_event("below", "above") == "돌파"
    assert t.close_event("above", "above") is None and t.close_event("above", "on") is None


def test_first_close_no_event():
    assert t.close_event(None, "below") is None


def test_market_and_hours():
    assert t.market_of("005930.KS") == "kr" and t.market_of("NVDA") == "us"
    assert t.market_open("us", datetime(2026, 11, 11, 0, 30, tzinfo=KST))        # 뉴욕 11-10 10:30
    assert not t.market_open("us", datetime(2026, 11, 11, 12, 0, tzinfo=KST))
    assert t.market_open("kr", datetime(2026, 11, 11, 10, 0, tzinfo=KST))


def test_near_once_per_day():
    sent, st = [], {}
    now = datetime(2026, 11, 11, 0, 30, tzinfo=KST)
    price = lambda tk: {"price": 168.0}
    assert t.check_near(price, sent.append, now, [NV], st, persist=False) == 1
    assert t.check_near(price, sent.append, now + timedelta(minutes=5), [NV], st, persist=False) == 0
    assert "빗각 접근" in sent[0] and "+2.4%" in sent[0]


def test_near_skips_far_price_and_closed_market():
    sent, st = [], {}
    assert t.check_near(lambda tk: {"price": 190.0}, sent.append, datetime(2026, 11, 11, 0, 30, tzinfo=KST), [NV], st, persist=False) == 0
    assert t.check_near(lambda tk: {"price": 165.0}, sent.append, datetime(2026, 11, 11, 12, 0, tzinfo=KST), [NV], st, persist=False) == 0


def test_close_check_digest_and_event():
    st = {"nv": {"side": "on"}}
    sent = []
    now = datetime(2026, 11, 13, 6, 30, tzinfo=KST)       # 뉴욕 11-12(목) 16:30
    msg = t.close_check("us", lambda tk: {"price": 155.0}, sent.append, now, [NV], st, persist=False)
    assert "빗각 종가 점검 — 미국" in msg and "종가 이탈" in msg and st["nv"]["side"] == "below"
    msg2 = t.close_check("us", lambda tk: {"price": 156.0}, sent.append, now, [NV], st, persist=False)
    assert "종가 이탈" not in msg2                        # 같은 상태면 다시 안 띄움


def test_close_check_reports_missing_price():
    msg = t.close_check("us", lambda tk: None, lambda m: None, datetime(2026, 11, 13, 6, 30, tzinfo=KST), [NV], {}, persist=False)
    assert "가격 조회 실패: nv" in msg


def test_main_wiring():
    src = open("main.py", encoding="utf-8").read()
    assert "trendlines.register(schedule, get_stock_data, send_telegram, SCANNER_URL, _SCANNER_HEADERS)" in src
    assert 'if text in ("빗각", "빗각현황", "추세선"):' in src


def test_close_digest_shows_note_on_event():
    line = {**NV, "note": "박스 바닥 설명"}
    msg = t.close_check("us", lambda tk: {"price": 155.0}, lambda m: None,
                        datetime(2026, 11, 13, 6, 30, tzinfo=KST), [line], {"nv": {"side": "on"}}, persist=False)
    assert "↳ 박스 바닥 설명" in msg
    msg2 = t.close_check("us", lambda tk: {"price": 155.0}, lambda m: None,
                         datetime(2026, 11, 13, 6, 30, tzinfo=KST), [line], {"nv": {"side": "below"}}, persist=False)
    assert "↳" not in msg2


# ── v2.90 추적 장부(스캐너 v5.386) ──────────────────────────────────────────
@pytest.fixture
def src_reset():
    keep = dict(t._SRC)
    t._SRC.update(url=None, headers=None, get=None, lines=None, ts=0.0, **{"from": "local"})
    yield
    t._SRC.update(keep)


class _Rsp:
    def __init__(self, j): self.j = j
    def json(self): return self.j


def test_shared_vectors():
    """스캐너 track_lines.line_value와 같은 값(pullback test_track_lines.py::test_shared_vectors와 같은 숫자)."""
    log = {"id": "a", "p1": ["2026-01-01", 100.0], "p2": ["2026-04-11", 200.0]}
    assert t.line_value(log, "2026-02-20") == pytest.approx(100 * 2 ** 0.5, abs=1e-9)
    assert t.line_value(log, "2026-07-20") == pytest.approx(400.0, abs=1e-9)
    lin = dict(log, scale="linear")
    assert t.line_value(lin, "2026-02-20") == pytest.approx(150.0, abs=1e-9)
    assert t.line_value(lin, "2026-07-20") == pytest.approx(300.0, abs=1e-9)


def test_remote_ledger_is_source_and_skips_closed_and_bad(src_reset, capsys):
    calls = []
    body = {"ok": True, "lines": [
        {"id": "tl_h", "ticker": "352820.KS", "name": "하이브", "label": "채널 상단", "kind": "fall", "scale": "log",
         "p1": ["2026-04-10", 330000], "p2": ["2026-10-20", 185000], "status": "추적", "theme": "한한령"},
        {"id": "tl_end", "ticker": "NVDA", "name": "끝", "p1": ["2026-01-01", 1], "p2": ["2026-02-01", 2], "status": "종료"},
        {"id": "tl_bad", "ticker": "NVDA", "name": "틀림", "p1": ["2026-01-01", 1], "p2": ["2026-01-01", 2], "status": "추적"}]}
    def g(url, params=None, timeout=None, headers=None):
        calls.append((url, params, headers)); return _Rsp(body)
    t.configure("https://s", {"X": "1"}, get=g)
    lines = t.load_lines()
    assert [x["id"] for x in lines] == ["tl_h"]
    assert calls[0] == ("https://s/api/track-lines", {"prices": 0}, {"X": "1"})
    assert "tl_bad" in capsys.readouterr().out                       # 건너뛴 선은 로그에 남는다
    t.load_lines()
    assert len(calls) == 1                                           # TTL 안에서는 다시 안 받는다
    row = t.fmt_line_row(lines[0], 189400.0, "2026-11-19", "kr")
    assert "하이브" in row and "채널 상단" in row                      # 장부의 선 종류 라벨을 쓴다
    assert "20개" not in t.status_text(lambda tk: {"price": 189400.0}, lines=None) and "⚠" not in t.status_text(lambda tk: {"price": 189400.0})


def test_remote_failure_falls_back(src_reset, capsys):
    def boom(*a, **k): raise RuntimeError("503")
    t.configure("https://s", {}, get=boom)
    lines = t.load_lines()                                           # 받은 적 없음 → 레포 파일
    assert len(lines) == 19 and "레포 trendlines.json" in capsys.readouterr().out
    assert "⚠ 장부를 못 읽어 옛 목록" in t.status_text(lambda tk: {"price": 100.0})
    # 한 번 받은 뒤 실패하면 마지막 장부로 계속
    ok = {"ok": True, "lines": [{"id": "x", "ticker": "NVDA", "name": "n", "p1": ["2026-01-01", 1.0], "p2": ["2026-02-01", 2.0], "status": "추적"}]}
    t.configure("https://s", {}, get=lambda *a, **k: _Rsp(ok))
    assert [x["id"] for x in t.load_lines()] == ["x"]
    t._SRC["ts"] = 0.0
    t.configure("https://s", {}, get=boom)
    assert [x["id"] for x in t.load_lines()] == ["x"] and "마지막 장부" in capsys.readouterr().out


def test_no_scanner_url_uses_local(src_reset):
    assert len(t.load_lines()) == 19
