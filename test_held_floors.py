"""v2.54 — 보유 종목 바닥선(저점관리 기준선) 알림. main.py는 import 시 루프에 빠지므로 함수만 AST로 뽑는다.

사보타주 확인(2026-11-10, FAIL 확인 후 원복):
  ① held_floor_decide의 broken 제외 삭제 → test_broken_is_digest_only FAIL
  ② near를 보낸 날 touch도 막음("near" in fired_kinds면 None) → test_touch_after_near_same_day FAIL
  ③ 요약에서 바닥선 없는 종목 줄 삭제 → test_digest_lists_missing_floors FAIL
"""
import ast
import html
import os

MAIN_PY = os.path.join(os.path.dirname(__file__), "main.py")
SRC = open(MAIN_PY, encoding="utf-8").read()


def _load():
    tree = ast.parse(SRC)
    names = {"held_floor_decide", "_held_cur", "format_held_floor_alert", "format_held_floor_digest", "format_price"}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names)
    ns = {"html": html, "HELD_FLOOR_NEAR_PCT": 3.0}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), MAIN_PY, "exec"), ns)
    return ns


NS = _load()
ROW = {"code": "407400.KQ", "name": "꿈비", "mkt": "KR", "has_floor": True, "state": "holding",
       "active_line": 1850.0, "floor_line": 1850.0, "avg_price": 2000, "hit": {"tf": "week", "bar_date": "2026-10-02"}}


def test_near_and_touch():
    d = NS["held_floor_decide"]
    assert d(ROW, 1950, []) is None                 # +5.4%
    assert d(ROW, 1900, []) == "near"               # +2.7%
    assert d(ROW, 1905.5, []) == "near"             # 경계 +3.0%
    assert d(ROW, 1850, []) == "touch"
    assert d(ROW, 1800, []) == "touch"


def test_once_per_kind_per_day():
    d = NS["held_floor_decide"]
    assert d(ROW, 1900, ["near"]) is None
    assert d(ROW, 1800, ["touch"]) is None


def test_touch_after_near_same_day():
    assert NS["held_floor_decide"](ROW, 1840, ["near"]) == "touch"


def test_broken_is_digest_only():
    assert NS["held_floor_decide"]({**ROW, "state": "broken"}, 1700, []) is None


def test_no_floor_or_price():
    d = NS["held_floor_decide"]
    assert d({**ROW, "has_floor": False}, 1800, []) is None
    assert d(ROW, None, []) is None


def test_alert_text():
    msg = NS["format_held_floor_alert"](ROW, 1840, "touch")
    assert "기준선 도달(장중)" in msg and "₩1,840" in msg and "₩1,850" in msg and "(-0.5%)" in msg
    assert "종가로 한다" in msg and "손절 없음" in msg and "주봉 2026-10-02" in msg and "평단 ₩2,000 (-8.0%)" in msg


def test_digest_lists_missing_floors():
    payload = {"ok": True, "checked_at": "2026-11-10T07:00:12+09:00", "state_label": {"holding": "지킴"},
               "rows": [{**ROW, "last_close": 1990.0, "dist_pct": 7.6},
                        {"code": "005930", "name": "삼성전자", "mkt": "KR", "has_floor": False, "reason": "no_hit"},
                        {"code": "AAPL", "name": "Apple", "mkt": "US", "has_floor": False, "reason": "no_row"}]}
    msg = NS["format_held_floor_digest"](payload)
    assert "1/3종목" in msg and "꿈비 기준선 ₩1,850 · 종가 ₩1,990 (+7.6%) · 지킴" in msg
    assert "바닥선 없음: 삼성전자(저점 신호 없음), Apple(추적 전)" in msg


def test_digest_empty_is_explicit():
    assert "보유 종목 없음" in NS["format_held_floor_digest"]({"ok": True, "rows": []})


def test_scheduled_and_persisted():
    assert "schedule.every(2).minutes.do(check_held_floors)" in SRC
    assert 'schedule.every().day.at("08:52", "Asia/Seoul").do(held_floor_digest)' in SRC
    assert '"held_floor_fired": _held_floor_fired' in SRC
    assert '_held_floor_fired.update(_sent_log_loaded.get("held_floor_fired")' in SRC
