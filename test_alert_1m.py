"""tools/alert_1m.py — 목록 형식과 월별 묶음(네트워크 없음)."""
import sys
from datetime import date

sys.path.insert(0, "tools")
import alert_1m  # noqa: E402


def test_list_is_valid_and_nonempty():
    jobs = alert_1m.read_list()
    assert len(jobs) >= 5000                      # 채널 알림 6,003줄 → 5,167 종목·날짜
    assert len(set(jobs)) == len(jobs)
    for s, d in jobs:
        assert s.endswith("USDT") and s.isascii()
        date.fromisoformat(d)


def test_fetch_groups_by_month(monkeypatch):
    monkeypatch.setattr(alert_1m, "get", lambda url: b"x" if "-1m-2026-04-12" in url else None)
    monkeypatch.setattr(alert_1m, "rows_of", lambda b: iter([(1, "1", "2", "0.5", "1.5", "9")]))
    assert alert_1m.fetch(("AUSDT", "2026-04-12")) == ("AUSDT", "2026-04", [(1, "1", "2", "0.5", "1.5", "9")])
    assert alert_1m.fetch(("AUSDT", "2026-04-13"))[2] == []
