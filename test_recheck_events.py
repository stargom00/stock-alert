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
    from datetime import datetime
    ns = {"html": html, "datetime": datetime}
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


def test_added_message_format_and_empty():
    f = _load({"format_added_message"})["format_added_message"]
    assert f({"events": []}) is None
    # 옛 일정(conv 없음) — 제목·메모 그대로, 이스케이프
    msg = f({"events": [{"date": "2026-11-20", "ticker": "036890.KQ", "title": "📐 빗각×기준선 수렴 · 진성티이씨",
                         "memo": "빗각 9,000 <x>"}]})
    assert msg.startswith("📐 <b>빗각×기준선 수렴</b> · 새로 1종목 달력에 추가")
    assert 'symbol=KRX:036890">📐 빗각×기준선 수렴 · 진성티이씨</a>' in msg and "빗각 9,000 &lt;x&gt;" in msg
    assert "🗓 <b>11/20(금) 만남</b> · 1종목" in msg


def test_added_message_v282_table_grouped_sorted():
    """v2.83 — 날짜별 묶음 · 종목마다 종가 + 빗각/기준선까지 % · 빗각에 가까운 순 · US는 티커·소수 2자리."""
    f = _load({"format_added_message"})["format_added_message"]
    ev = [{"date": "2026-11-27", "ticker": "079960.KQ", "conv": {"mkt": "KR", "name": "동양이엔피", "line": 15280, "base": 13920, "close": 14730}},
          {"date": "2026-11-20", "ticker": "AKTX", "conv": {"mkt": "US", "name": "Akari Therapeutics Plc - ADS", "line": 7.24, "base": 6.51, "close": 6.80}},
          {"date": "2026-11-20", "ticker": "065150.KQ", "conv": {"mkt": "KR", "name": "대산F&B", "line": 1152, "base": 1060, "close": 1144}}]
    msg = f({"events": ev})
    assert msg.index("11/20(금)") < msg.index("11/27(금)")                       # 날짜순
    assert msg.index("대산F&amp;B") < msg.index("AKTX")                          # 같은 날 안에서 빗각 가까운 순(+0.7% < +6.5%)
    assert "<b>대산F&amp;B</b></a> 1,144\n   ↗ 빗각 1,152 (+0.7%)  ↘ 기준선 1,060 (-7.3%)" in msg
    assert "🇺🇸" in msg and "<b>AKTX</b></a> 6.80" in msg and "빗각 7.24 (+6.5%)" in msg and "Akari" not in msg
    assert "주당" not in msg                                                      # 기울기·고점 날짜는 알림에서 뺐다(달력 메모엔 그대로)


def test_added_scheduled_daily():
    assert re.search(r'schedule\.every\(\)\.day\.at\("08:46", "Asia/Seoul"\)\.do\(check_added_events\)', SRC)
    assert "/api/user-events/added" in SRC
