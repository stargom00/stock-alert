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
    assert "trendlines.register(schedule, get_stock_data, send_telegram)" in src
    assert 'if text in ("빗각", "빗각현황", "추세선"):' in src


def test_close_digest_shows_note_on_event():
    line = {**NV, "note": "박스 바닥 설명"}
    msg = t.close_check("us", lambda tk: {"price": 155.0}, lambda m: None,
                        datetime(2026, 11, 13, 6, 30, tzinfo=KST), [line], {"nv": {"side": "on"}}, persist=False)
    assert "↳ 박스 바닥 설명" in msg
    msg2 = t.close_check("us", lambda tk: {"price": 155.0}, lambda m: None,
                         datetime(2026, 11, 13, 6, 30, tzinfo=KST), [line], {"nv": {"side": "below"}}, persist=False)
    assert "↳" not in msg2
