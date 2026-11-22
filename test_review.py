"""v2.95 토요일 주간 리뷰 — 알림 기록(review_log.json)과 리뷰 글.
사보타주 확인(2026-11-22, 전부 FAIL 확인 후 원복): 7일 범위를 안 거름 / 같은 기록 중복 저장 / 종료한 선을 근처로 셈 /
한 곳 실패 시 전체 실패 / 빗각·테마·매수 알림이 기록을 안 남김."""
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

import review

KST = ZoneInfo("Asia/Seoul")
SAT = date(2026, 11, 21)


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    return tmp_path


def test_log_dedupes_and_prunes(data_dir):
    assert review.log("line", "하이브", "채널 상단 종가 돌파", "2026-11-18") is True
    assert review.log("line", "하이브", "채널 상단 종가 돌파", "2026-11-18") is False      # 같은 기록은 한 번
    assert review.log("theme", "엔터", "점화 17/36 · 평균 +6.2%", "2026-11-18") is True
    assert review.log("buy", "옛날", "매수가 대비 +5% 단계 도달(+5.5%)", "2026-08-01") is True
    assert review.log("buy", "아남전자", "매수가 대비 +10% 단계 도달(+10.5%)", "2026-11-20") is True
    rows = review.load_log()
    assert [r["name"] for r in rows] == ["하이브", "엔터", "아남전자"]                       # 60일 넘은 기록은 정리
    with pytest.raises(AssertionError):
        review.log("etc", "x", "y", "2026-11-20")


def test_week_rows_covers_seven_days():
    rows = [{"date": d, "kind": "line", "name": n, "what": "w"} for d, n in
            [("2026-11-14", "지난주"), ("2026-11-15", "첫날"), ("2026-11-21", "오늘"), ("2026-11-22", "내일")]]
    assert [r["name"] for r in review.week_rows(rows, SAT)] == ["첫날", "오늘"]
    assert review.week_range(SAT) == (date(2026, 11, 15), SAT)


def _rows():
    return [{"date": "2026-11-18", "kind": "line", "name": "하이브", "what": "채널 상단 종가 돌파"},
            {"date": "2026-11-18", "kind": "theme", "name": "엔터", "what": "점화 17/36 · 평균 +6.2%"},
            {"date": "2026-11-20", "kind": "buy", "name": "아남전자", "what": "매수가 대비 +10% 단계 도달(+10.5%)"}]


def _basket(n, avg, strong=3, status="추적", n_px=10):
    return {"name": n, "status": status, "summary": {"avg_pct": avg, "strong": strong, "n_px": n_px, "date": "2026-11-20"}}


def test_build_text():
    lines = [{"name": "엔비디아", "label": "빗각", "status": "추적", "view": {"zone": "near", "to_line_pct": -0.3}},
             {"name": "팔란티어", "label": "수평선", "status": "추적", "view": {"zone": "above", "to_line_pct": 12.0}},
             {"name": "종료된선", "label": "빗각", "status": "종료", "view": {"zone": "near", "to_line_pct": 0.1}}]
    baskets = [_basket(n, a) for n, a in [("엔터", 6.2), ("콘텐츠", 8.1), ("로봇", 1.0), ("음식료", -0.9), ("조선", -2.0), ("보안", 3.3), ("전력", 0.5)]]
    baskets += [_basket("끝난 테마", 30.0, status="종료"), _basket("가격 없음", None, n_px=0)]
    buys = [({"name": "아남전자"}, 8.7), ({"name": "엔비디아"}, -3.8), ({"name": "조회 실패"}, None)]
    t = review.build_text(_rows(), SAT, lines, baskets, buys, ["테스트 경고"])
    assert "11-15 ~ 11-21" in t and "내 선</b> · 이번 주 알림 1건" in t and "· 11-18 <b>하이브</b> 채널 상단 종가 돌파" in t
    assert "지금 선 근처: 엔비디아(빗각 -0.3%)" in t and "종료된선" not in t and "추적 중인 선 2개" in t
    assert "이번 주 점화 1건" in t and "강한 순: 콘텐츠 +8.1%(3/10) · 엔터 +6.2%(3/10) · 보안 +3.3%(3/10)" in t
    assert "약한 순: 조선 -2.0%(3/10) · 음식료 -0.9%(3/10) · 전력 +0.5%(3/10)" in t and "끝난 테마" not in t and "가격 없음" not in t
    assert "매수 추적</b> · 이번 주 알림 1건" in t and "지금: 아남전자 +8.7% · 엔비디아 -3.8% · 조회 실패 —" in t
    assert "⚠ 테스트 경고" in t and "매매 복기에 한 줄" in t
    # 아무 일도 없던 주 · 못 읽은 곳(None)은 그 줄만 빠진다
    t = review.build_text([], SAT, None, None, [])
    assert "닿거나 넘은 종목이 없었어" in t and "점화한 테마가 없었어" in t and "추적 중인 매수가 없어" in t
    assert "지금 선 근처" not in t and "강한 순" not in t


class _Resp:
    def __init__(self, p):
        self.p = p

    def json(self):
        return self.p


def test_weekly_text_survives_one_source_failing(data_dir):
    import buy_track
    buy_track.save_state({"items": [{"id": "008700|2026-11-18", "ticker": "008700", "name": "아남전자", "buy": 1000.0, "date": "2026-11-18",
                                     "until": "2027-05-20", "hit": [], "max": None, "min": None}]})
    calls = []

    def get(url, **kw):
        calls.append(url.rsplit("/", 1)[1])
        if url.endswith("/api/track-lines"):
            raise OSError("연결 실패")
        return _Resp({"ok": True, "baskets": [_basket("엔터", 6.2)]})
    t = review.weekly_text(lambda tk: {"price": 1087.0}, "http://scanner", {}, get, datetime(2026, 11, 21, 9, 5, tzinfo=KST), _rows())
    assert calls == ["track-lines", "theme-baskets"]
    assert "⚠ 추적 장부를 못 읽었어" in t and "강한 순: 엔터 +6.2%" in t and "지금: 아남전자 +8.7%" in t and "하이브" in t


def test_alerts_write_review_log(data_dir):
    """세 모듈이 실제로 알림을 보낼 때 기록이 남는다(프로덕션과 같은 경로: 상태를 넘기지 않는다)."""
    import buy_track
    import theme_ignite as ti
    import trendlines as tl
    now = datetime(2026, 11, 18, 19, 30, tzinfo=KST)
    ti._cfg.update(url="http://scanner", headers={})
    b = {"id": "tb2_enter", "name": "엔터", "status": "추적", "rows": [],
         "summary": {"strong": 17, "n_px": 36, "n_failed": 0, "avg_pct": 6.15, "date": "2026-11-18", "ignited": True, "is_today": True}}
    assert ti.check(lambda m: None, lambda url, **kw: _Resp({"ok": True, "baskets": [b], "lined": []}), None, now) == ["tb2_enter"]
    buy_track.save_state({"items": [{"id": "008700|2026-11-10", "ticker": "008700", "name": "아남전자", "buy": 1000.0, "date": "2026-11-10",
                                     "until": "2027-05-12", "hit": [], "max": None, "min": None}]})
    assert buy_track.close_check("kr", lambda t: {"price": 1105.0}, lambda m: None, datetime(2026, 11, 18, 20, 22, tzinfo=KST)) == 1
    line = {"id": "L1", "ticker": "352820.KS", "name": "하이브", "label": "채널 상단", "p1": ["2026-06-25", 250000], "p2": ["2026-10-15", 180000]}
    v = tl.line_value(line, date(2026, 11, 18))
    assert tl.check_near(lambda t: {"price": v * 1.01}, lambda m: None, datetime(2026, 11, 18, 10, 0, tzinfo=KST), [line]) == 1
    got = {(r["kind"], r["name"], r["date"]) for r in review.load_log()}
    assert got == {("theme", "엔터", "2026-11-18"), ("buy", "아남전자", "2026-11-18"), ("line", "하이브", "2026-11-18")}
    whats = {r["kind"]: r["what"] for r in review.load_log()}
    assert whats["theme"] == "점화 17/36 · 평균 +6.2%" and "+10% 단계 도달" in whats["buy"] and whats["line"] == "채널 상단 선 근처 접근"
