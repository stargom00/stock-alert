"""v2.88 덜빠진 — 스캐너 /api/rs-drawdown 응답 → 텔레그램 본문(실응답 형태: sector '기타', 플래그 False, 얕은 하락)."""
import rs_strong as rs

W = {"KOSPI": {"peak_date": "2026-10-28", "trough_date": "2026-11-14", "dd": -0.082, "reb": 0.031, "meaningful": True},
     "KOSDAQ": {"peak_date": "2026-11-03", "trough_date": "2026-11-17", "dd": -0.031, "reb": 0.004, "meaningful": False}}
R = [{"ticker": "009150.KS", "name": "삼성전기", "sector": "전자부품", "dd": -0.021, "rel_dd": 0.061, "reb": 0.12, "low_ok": True, "ma50_ok": True},
     {"ticker": "0013V0.KQ", "name": "신규코드", "sector": "기타", "dd": -0.02, "rel_dd": 0.011, "reb": 0.0, "low_ok": False, "ma50_ok": False}]
P = {"ok": True, "session_date": "2026-11-18", "index": W, "n_checked": 1040, "top": R,
     "sectors": [{"sector": "반도체", "n": 8, "rel_dd_med": 0.052}]}


def test_format():
    t = rs.format_text(P)
    assert "KOSPI 10-28 고점 → 11-14 저점 <b>-8.2%</b> · 이후 +3.1%" in t
    assert "KOSDAQ" in t and t.count("⚠ 하락이 얕음") == 1
    assert "하락 -2.1% (지수보다 +6.1%p) · 이후 +12% · ✓저점 유지 · ✓50일선 위" in t
    assert "· 기타" not in t and "✓" not in t.split("신규코드")[1].split("\n")[1]
    assert 'symbol=KRX:0013V0"' in t                                       # 알파벳 섞인 코드도 6자리 그대로
    assert "반도체 +5.2%p(8)" in t and "상위 2" in t


def test_on_demand_reasons():
    class Rsp:
        def __init__(self, j): self.j = j
        def json(self): return self.j
    assert "naver" in rs.on_demand("u", {}, get=lambda *a, **k: Rsp({"ok": True, "reason": "no_index"}))
    assert "계산할 종목이 없어" in rs.on_demand("u", {}, get=lambda *a, **k: Rsp({"ok": True, "reason": "no_rows"}))
    assert "응답 오류" in rs.on_demand("u", {}, get=lambda *a, **k: Rsp({"ok": False, "error": "x"}))
    seen = {}
    def g(url, params=None, timeout=None, headers=None):
        seen.update(url=url, params=params); return Rsp(P)
    assert "삼성전기" in rs.on_demand("https://s", {"X": "1"}, get=g) and seen["url"] == "https://s/api/rs-drawdown"


def test_aliases_match_main():
    src = open("main.py", encoding="utf-8").read()
    assert 'if text in ("덜빠진", "덜빠진종목", "덜 빠진", "상대강도", "버틴종목"):' in src
    assert tuple(rs.ALIASES) == ("덜빠진", "덜빠진종목", "덜 빠진", "상대강도", "버틴종목")
