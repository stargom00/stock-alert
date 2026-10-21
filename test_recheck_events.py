"""v2.50 — 스캐너 달력 "재확인 종목" 아침 알림.

main.py는 import하면 while True로 빠지므로(test_holiday_alerts.py와 같은 이유) 함수만 AST로 뽑아 돌린다.
사보타주 확인(2026-10-21, FAIL 확인 후 원복): format_recheck_message가 memo 줄을 빠뜨림 → test_format_links_and_memo FAIL;
스케줄 줄 삭제 → test_scheduled_daily FAIL.
"""
import ast
import html
import os
import re

MAIN_PY = os.path.join(os.path.dirname(__file__), "main.py")
SRC = open(MAIN_PY, encoding="utf-8").read()


def _load(names):
    tree = ast.parse(SRC)
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names)
    ns = {"html": html}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), MAIN_PY, "exec"), ns)
    return ns


PAYLOAD = {"ok": True, "date": "2026-12-29", "events": [
    {"id": "ue_1", "date": "2026-12-29", "title": "크래프톤 구름 모양 확인", "time": None,
     "ticker": "259960.KS", "memo": "일목 선행구름 두꺼움\n이날 구름 꼬임 여부 <b>"},
    {"id": "ue_2", "date": "2026-12-29", "title": "AAPL 실적", "time": "06:00", "ticker": "AAPL", "memo": None},
    {"id": "ue_3", "date": "2026-12-29", "title": "FOMC", "time": "03:00", "ticker": None, "memo": None},
]}


def test_format_links_and_memo():
    f = _load({"format_recheck_message"})["format_recheck_message"]
    msg = f(PAYLOAD)
    assert msg.startswith("📅 <b>오늘 열어볼 종목</b> · 2026-12-29")
    assert 'symbol=KRX:259960">259960.KS</a> 크래프톤 구름 모양 확인' in msg
    assert 'symbol=AAPL">AAPL</a> AAPL 실적' in msg and "<b>06:00</b>" in msg
    assert "· <b>03:00</b> FOMC" in msg
    assert "↳ 일목 선행구름 두꺼움" in msg and "↳ 이날 구름 꼬임 여부 &lt;b&gt;" in msg   # 메모 줄 + 이스케이프
    assert msg.count("↳") == 2


def test_empty_day_sends_nothing():
    f = _load({"format_recheck_message"})["format_recheck_message"]
    assert f({"ok": True, "date": "2026-12-30", "events": []}) is None
    assert f(None) is None


def test_check_uses_due_endpoint_and_skips_when_empty():
    ns = _load({"format_recheck_message", "check_recheck_events"})
    sent = []
    got = {}

    class _R:
        def __init__(self, p): self._p = p
        def json(self): return self._p

    class _Req:
        @staticmethod
        def get(url, timeout=None, headers=None):
            got["url"] = url
            return _R({"ok": True, "date": "2026-12-30", "events": []})
    ns.update({"requests": _Req, "SCANNER_URL": "http://s", "_SCANNER_HEADERS": {}, "send_telegram": sent.append, "print": lambda *a, **k: None})
    ns["check_recheck_events"]()
    assert got["url"] == "http://s/api/user-events/due" and sent == []

    class _Req2(_Req):
        @staticmethod
        def get(url, timeout=None, headers=None): return _R(PAYLOAD)
    ns["requests"] = _Req2
    ns["check_recheck_events"]()
    assert len(sent) == 1 and "259960.KS" in sent[0]


def test_scheduled_daily():
    assert re.search(r'schedule\.every\(\)\.day\.at\("08:45", "Asia/Seoul"\)\.do\(check_recheck_events\)', SRC)
