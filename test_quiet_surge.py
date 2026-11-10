"""v2.55 — 조용하던 종목 거래량 폭발 알림. main.py는 import 시 루프에 빠지므로 함수만 AST로 뽑는다.

사보타주 확인(2026-11-10, FAIL 확인 후 원복):
  ① quiet_surge_due가 US 슬롯도 KST로 판정 → test_us_slots_use_new_york_time FAIL
  ② 장중 0건도 본문을 만듦 → test_intraday_zero_is_silent FAIL
  ③ 마감 본문에서 후속 줄 삭제 → test_close_message_has_followups FAIL
"""
import ast
import html
import os
from datetime import datetime, timedelta, timezone

import pytz

MAIN_PY = os.path.join(os.path.dirname(__file__), "main.py")
SRC = open(MAIN_PY, encoding="utf-8").read()
KST = timezone(timedelta(hours=9))


def _load():
    tree = ast.parse(SRC)
    fn = {"quiet_surge_due", "_qs_link", "format_quiet_surge", "format_price"}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in fn]
    nodes += [n for n in tree.body if isinstance(n, ast.Assign) and any(getattr(t, "id", "") in ("QUIET_SURGE_SLOTS", "QUIET_SURGE_WINDOW_MIN") for t in n.targets)]
    ns = {"html": html, "ET": pytz.timezone("America/New_York")}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), MAIN_PY, "exec"), ns)
    return ns


NS = _load()


def _due(dt, fired=None):
    return [slot for _, slot, _, _ in NS["quiet_surge_due"](dt, fired or {})]


def test_kr_slots():
    assert _due(datetime(2026, 11, 10, 10, 2, tzinfo=KST)) == ["kr-10"]
    assert _due(datetime(2026, 11, 10, 10, 31, tzinfo=KST)) == []
    assert _due(datetime(2026, 11, 10, 20, 16, tzinfo=KST)) == ["kr-close"]
    assert _due(datetime(2026, 11, 14, 10, 2, tzinfo=KST)) == []          # 토요일


def test_us_slots_use_new_york_time():
    assert _due(datetime(2026, 11, 11, 0, 31, tzinfo=KST)) == ["us-1030"]   # 뉴욕 11-10 10:31 EST
    assert _due(datetime(2026, 11, 11, 6, 35, tzinfo=KST)) == ["us-close"]  # 뉴욕 16:35
    assert _due(datetime(2026, 11, 14, 0, 31, tzinfo=KST)) == ["us-1030"]   # KST 토요일이지만 뉴욕은 금요일


def test_fired_once():
    dt = datetime(2026, 11, 10, 10, 2, tzinfo=KST)
    assert _due(dt, {"kr-10:2026-11-10": True}) == []


HIT = {"ticker": "407400.KQ", "name": "꿈비", "close": 2100.0, "change_pct": 8.2, "surge_ratio": 7.5, "quiet_ratio": 0.42}


def test_intraday_zero_is_silent():
    assert NS["format_quiet_surge"]({"market": "kr", "mode": "intraday", "hits": [], "elapsed_pct": 15}) is None


def test_intraday_message():
    m = NS["format_quiet_surge"]({"market": "kr", "mode": "intraday", "hits": [HIT], "elapsed_pct": 15.4})
    assert "국내 장중" in m and "15.4% 경과" in m and "7.5배(장중 환산)" in m and "symbol=KRX:407400" in m and "₩2,100 (+8.2%)" in m


def test_close_zero_is_explicit():
    m = NS["format_quiet_surge"]({"market": "us", "mode": "close", "session_date": "2026-11-10", "hits": [], "n_checked": 2000})
    assert "미국 마감" in m and "오늘 폭발 0건 (대상 2000종목)" in m


def test_close_message_has_followups():
    f = {"ticker": "ABCD", "name": "Abcd", "date": "2026-11-06", "surge_ratio": 6.0,
         "after": [{"vol_x": 3.2, "close_vs_surge_pct": 2.1}, {"vol_x": 1.1, "close_vs_surge_pct": -0.5}]}
    m = NS["format_quiet_surge"]({"market": "us", "mode": "close", "session_date": "2026-11-10", "hits": [], "n_checked": 5,
                                  "followups": [f]})
    assert "Abcd" in m and "11-06 6.0배 → D+1 ×3.2 (+2.1%) · D+2 ×1.1 (-0.5%)" in m


def test_scheduled_and_persisted():
    assert "schedule.every(2).minutes.do(check_quiet_surge)" in SRC
    assert '"quiet_surge_fired": _quiet_surge_fired' in SRC
    assert '_quiet_surge_fired.update(_sent_log_loaded.get("quiet_surge_fired")' in SRC
