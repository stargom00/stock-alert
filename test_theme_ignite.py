"""v2.92 테마 점화 — 스캐너 /api/theme-baskets 응답으로 알림·현황 글을 만든다.
사보타주 확인(2026-11-19, 전부 FAIL 확인 후 원복): is_today 조건 제거 / 중복 방지 키 제거 / 종료 바구니 포함 / 폴링 창 무시."""
from datetime import datetime
from zoneinfo import ZoneInfo

import theme_ignite as ti

KST = ZoneInfo("Asia/Seoul")
OPEN = datetime(2026, 11, 18, 19, 30, tzinfo=KST)        # 수요일 애프터마켓
CLOSED = datetime(2026, 11, 21, 12, 0, tzinfo=KST)       # 토요일


def _basket(id_="tb_h", name="한한령", ignited=True, today=True, status="추적", last=None):
    rows = [{"ticker": "041510.KQ", "name": "에스엠", "chg_pct": 30.59}, {"ticker": "352820.KS", "name": "하이브", "chg_pct": 16.12},
            {"ticker": "NVDA", "name": "엔비디아", "chg_pct": 5.5}, {"ticker": "035900.KQ", "name": "JYP Ent.", "chg_pct": None}]
    return {"id": id_, "name": name, "status": status, "rows": rows, "last_ignited": last,
            "summary": {"strong": 19 if ignited else 2, "n_px": 23, "n_failed": 1, "avg_pct": 12.93, "date": "2026-11-18",
                        "ignited": ignited, "is_today": today, "leader": {"name": "에스엠", "chg_pct": 30.59}}}


class _Resp:
    def __init__(self, p):
        self.p = p

    def json(self):
        return self.p


def _get(baskets, ok=True, calls=None):
    def get(url, **kw):
        if calls is not None:
            calls.append((url, kw.get("params")))
        return _Resp({"ok": ok, "error": "장부 읽기 실패", "baskets": baskets, "lined": ["352820", "NVDA"],
                      "rule": {"up_pct": 5.0, "min_count": 3, "min_frac": 0.3}})
    return get


def setup_function(_):
    ti._cfg.update(url="http://scanner", headers={"X": "1"})


def test_window():
    assert ti.window_open(OPEN) and not ti.window_open(CLOSED)
    assert ti.window_open(datetime(2026, 11, 18, 9, 0, tzinfo=KST)) and ti.window_open(datetime(2026, 11, 18, 20, 10, tzinfo=KST))
    assert not ti.window_open(datetime(2026, 11, 18, 20, 11, tzinfo=KST))
    assert not ti.window_open(datetime(2026, 11, 18, 8, 59, tzinfo=KST))
    ny = ZoneInfo("America/New_York")
    assert ti.window_open(datetime(2026, 11, 18, 10, 0, tzinfo=ny)) and not ti.window_open(datetime(2026, 11, 18, 17, 0, tzinfo=ny))


def test_alert_once_per_basket_and_day():
    sent, st, calls = [], {}, []
    bs = [_basket(), _basket("tb_q", "조용한 테마", ignited=False), _basket("tb_old", "어제 점화", today=False),
          _basket("tb_end", "끝난 테마", status="종료")]
    assert ti.check(sent.append, _get(bs, calls=calls), st, OPEN) == ["tb_h"]
    assert calls == [("http://scanner/api/theme-baskets", {"prices": 1})]
    assert len(sent) == 1 and st == {"tb_h|2026-11-18": "2026-11-18"}
    m = sent[0]
    assert "테마 점화" in m and "<b>한한령</b>" in m and "<b>19/23</b>" in m and "+12.9%" in m
    assert "KRX:041510" in m and "symbol=NVDA" in m and "+30.6%" in m
    assert m.count("📐") == 3 and "하이브</a> <b>+16.1%</b> 📐" in m       # 장부에 선 있는 종목 표시(범례 1 + 하이브·엔비디아)
    assert "JYP" not in m and "가격 실패 1종목" in m and "측정 전" in m
    assert ti.check(sent.append, _get(bs), st, OPEN) == [] and len(sent) == 1   # 같은 날 다시 안 보낸다
    nxt = _basket()
    nxt["summary"]["date"] = "2026-11-19"
    assert ti.check(sent.append, _get([nxt]), st, OPEN) == ["tb_h"] and len(sent) == 2   # 다음 날 또 점화하면 보낸다


def test_closed_window_makes_no_request():
    calls = []
    assert ti.check(lambda m: 1 / 0, _get([_basket()], calls=calls), {}, CLOSED) == [] and calls == []


def test_state_file_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    sent = []
    assert ti.check(sent.append, _get([_basket()]), None, OPEN) == ["tb_h"]
    assert ti.load_state() == {"tb_h|2026-11-18": "2026-11-18"}
    assert ti.check(sent.append, _get([_basket()]), None, OPEN) == [] and len(sent) == 1   # 재배포 뒤에도 파일로 막는다


def test_on_demand():
    t = ti.on_demand(_get([_basket(), _basket("tb_q", "화장품", ignited=False, last={"date": "2026-11-17", "strong": 8, "n_px": 10}),
                           _basket("tb_end", "끝난 테마", status="종료")]))
    assert "🔥 <b>한한령</b> 강한 19/23" in t and "⚪ <b>화장품</b> 강한 2/23" in t and "최근 점화 11-17 (8/10)" in t
    assert "끝난 테마" not in t and "점화 중" in t and "30% 이상" in t
    assert "조회 실패" in ti.on_demand(_get([], ok=False))
    assert "+ 테마" in ti.on_demand(_get([]))
