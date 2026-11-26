"""v2.97 거래대금 상위 10. 응답 모양은 스캐너 v5.403 turnover_rank.analyze 그대로.
사보타주 확인(2026-11-26, 전부 FAIL 확인 후 원복): 같은 날 두 번 보내기 / 20:25 전 보내기 / 새 진입 표시 제거 / main 라우팅 제거."""
from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo

import turnover_alert as ta

HERE = os.path.dirname(os.path.abspath(__file__))
KST = ZoneInfo("Asia/Seoul")


def _payload(day="2026-11-26"):
    top = [{"rank": 1, "code": "000660", "ticker": "000660.KS", "name": "SK하이닉스", "value_mil": 2930200, "chg_pct": 3.79,
            "prev_rank": 1, "move": 0, "new": False, "streak": 5, "in_top_20d": 5},
           {"rank": 2, "code": "009150", "ticker": "009150.KS", "name": "삼성전기", "value_mil": 530000, "chg_pct": 5.56,
            "prev_rank": 14, "move": 12, "new": True, "streak": 1, "in_top_20d": 1},
           {"rank": 3, "code": "053800", "ticker": "053800.KQ", "name": "안랩", "value_mil": 246200, "chg_pct": -1.0,
            "prev_rank": 2, "move": -1, "new": False, "streak": 2, "in_top_20d": 2}]
    return {"ok": True, "analysis": {"day": day, "prev_day": "2026-11-25", "top": top,
                                     "dropped": [{"code": "x", "ticker": "x.KS", "name": "KB금융", "prev_rank": 7, "rank": None}],
                                     "n_days": 3, "first_day": "2026-11-24"}}


class Get:
    def __init__(self, p):
        self.p = p

    def __call__(self, url, **kw):
        assert url.endswith("/api/turnover-rank")
        p = self.p

        class R:
            def json(self):
                return p
        return R()


def test_text():
    t = ta.rank_text(_payload())
    assert "거래대금 상위 10</b> · 11-26" in t and "29,302억" in t and "5일째" in t
    assert "🆕(14위→)" in t and "▼1" in t and "새로 10위 안: 삼성전기" in t and "KB금융(7위→20위 밖)" in t
    assert "KRX:000660" in t
    assert "아직 없어" in ta.rank_text({"ok": True, "analysis": None})


def test_check_once_per_day_after_2025():
    ta._cfg["url"] = "http://x"
    sent, st = [], {}
    before = datetime(2026, 11, 26, 20, 20, tzinfo=KST)
    after = datetime(2026, 11, 26, 20, 30, tzinfo=KST)
    assert ta.check(sent.append, Get(_payload()), st, before) is None and sent == []
    assert ta.check(sent.append, Get(_payload()), st, after) == "2026-11-26" and len(sent) == 1
    assert ta.check(sent.append, Get(_payload()), st, after) is None and len(sent) == 1           # 같은 날 두 번 안 보냄
    assert ta.check(sent.append, Get(_payload("2026-11-25")), {}, after) is None                  # 오늘 기록 아직 없음
    assert ta.check(sent.append, Get(_payload()), {}, datetime(2026, 11, 28, 21, 0, tzinfo=KST)) is None   # 토요일


def test_routed_in_main():
    src = open(os.path.join(HERE, "main.py"), encoding="utf-8").read()
    assert src.count("turnover_alert.on_demand()") == 1 and src.count("turnover_alert.register(") == 1
