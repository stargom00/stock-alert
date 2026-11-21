"""v2.94 매수 종목 6개월 추적 — 매수가 대비 단계 알림.
사보타주 확인(2026-11-21, 전부 FAIL 확인 후 원복): 같은 단계 다시 알림(hit 기록 제거) / 등록 때 이미 넘은 단계를 안 적음 /
만료 종목을 안 지움 / 다른 시장 종목까지 판정 / 삭제가 이름·티커를 안 가림."""
from datetime import datetime
from zoneinfo import ZoneInfo

import buy_track as bt

KST = ZoneInfo("Asia/Seoul")


def _now(s):
    return datetime.fromisoformat(s).replace(tzinfo=KST)


def _prices(d):
    return lambda t: ({"price": d[t]} if d.get(t) else None)


def test_parse_line():
    today = _now("2026-11-21T10:00").date()
    it, err = bt.parse_line("아남 전자 008700.ks 1,352 2026-11-18", today)
    assert err is None and (it["id"], it["ticker"], it["name"], it["buy"], it["date"], it["until"]) == (
        "008700|2026-11-18", "008700", "아남 전자", 1352.0, "2026-11-18", "2027-05-20")
    it, _ = bt.parse_line("nvda 172.54", today)
    assert (it["ticker"], it["name"], it["date"]) == ("NVDA", "NVDA", "2026-11-21")
    assert bt.parse_line("빅웨이브 0035S0 17000", today)[0]["ticker"] == "0035S0"
    for bad in ("아남전자 1352", "008700 abc", "008700 0", "아남전자 008700 1352 2027-01-01", "008700 1352 2026-13-40", "한글티커 100"):
        it, err = bt.parse_line(bad, today)
        assert it is None and err, bad
    assert bt.market_of("0035S0") == "kr" and bt.market_of("008700.KS") == "kr" and bt.market_of("NVDA") == "us"


def test_levels():
    it = {"hit": []}
    assert bt.new_level(it, 4.9) is None and it["hit"] == []
    assert bt.new_level(it, 5.0) == 5 and bt.new_level(it, 7.0) is None           # 같은 단계는 한 번
    assert bt.new_level(it, 31.0) == 30 and it["hit"] == [5, 10, 20, 30]          # 여러 단계를 넘으면 가장 먼 것 하나
    assert bt.new_level(it, 12.0) is None                                         # 되돌아와도 지난 단계는 조용
    assert bt.new_level(it, -5.5) == -5 and bt.new_level(it, -22.0) == -20 and bt.new_level(it, -6.0) is None
    assert bt.crossed(0) == [] and bt.crossed(150) == list(bt.UP_LEVELS) and bt.crossed(-60) == list(bt.DOWN_LEVELS)


def test_register_marks_levels_already_passed(tmp_path):
    st = {"items": []}
    msg = bt.register_text("매수추적 아남전자 008700 1000 2026-11-18\nNVDA 100\n\n한글 abc", _prices({"008700": 1120.0, "NVDA": 93.0}),
                           _now("2026-11-21T21:00"), st, persist=False)
    a, n = st["items"]
    assert a["hit"] == [5, 10] and n["hit"] == [-5] and a["max"]["pct"] == 12.0      # 이미 넘은 단계는 지난 것으로
    assert "+12.0%" in msg and "-7.0%" in msg and "⚠" in msg and "2027-05-20까지" in msg and "KRX:008700" in msg
    # 같은 종목·같은 날을 다시 보내면 바꿔 넣는다(두 개가 되지 않는다)
    bt.register_text("매수추적 아남전자 008700 1100 2026-11-18", _prices({"008700": 1120.0}), _now("2026-11-21T21:00"), st, persist=False)
    assert len(st["items"]) == 2 and [x["buy"] for x in st["items"] if x["ticker"] == "008700"] == [1100.0]
    # 가격을 못 받으면 등록하지 않는다
    msg = bt.register_text("매수추적 없는종목 999999 100", _prices({}), _now("2026-11-21T21:00"), st, persist=False)
    assert len(st["items"]) == 2 and "가격을 못 받았어" in msg
    assert "이미 지났어" in bt.register_text("매수추적 옛날 005930 100 2026-01-02", _prices({"005930": 100}), _now("2026-11-21T21:00"), st, persist=False)
    # 내용 없이 보내면 현황
    assert "매수 추적 중" in bt.register_text("매수추적", _prices({"008700": 1120.0, "NVDA": 93.0}), _now("2026-11-21T21:00"), st, persist=False)


def _item(tk="008700", buy=1000.0, d="2026-11-18", **kw):
    it, _ = bt.parse_line(f"테스트 {tk} {buy} {d}", _now("2026-11-21T10:00").date())
    it.update(kw)
    return it


def test_close_check_alerts_once_per_level():
    st = {"items": [_item(), _item("NVDA", 100.0)]}
    sent = []
    assert bt.close_check("kr", _prices({"008700": 1030.0, "NVDA": 150.0}), sent.append, _now("2026-11-23T20:22"), st, persist=False) == 0
    assert st["items"][1]["hit"] == [] and st["items"][1]["max"] is None                 # 다른 시장 종목은 건드리지 않는다
    assert bt.close_check("kr", _prices({"008700": 1105.0}), sent.append, _now("2026-11-24T20:22"), st, persist=False) == 1
    m = sent[0]
    assert "+10%" in m and "도달" in m and "+10.5%" in m and "1,105원" in m and "매수가 1,000원" in m and "6일째" in m
    assert bt.close_check("kr", _prices({"008700": 1108.0}), sent.append, _now("2026-11-25T20:22"), st, persist=False) == 0
    assert bt.close_check("kr", _prices({"008700": 940.0}), sent.append, _now("2026-11-26T20:22"), st, persist=False) == 1
    assert "-5%" in sent[1] and "이탈" in sent[1] and "최고 +10.8%(11-25)" in sent[1] and "최저 -6.0%(11-26)" in sent[1]
    # 주말은 건너뛴다 · 가격 실패는 알림 없이 넘어간다
    assert bt.close_check("kr", _prices({"008700": 2000.0}), sent.append, _now("2026-11-28T20:22"), st, persist=False) == 0
    assert bt.close_check("kr", _prices({}), sent.append, _now("2026-11-30T20:22"), st, persist=False) == 0 and len(sent) == 2
    # 미국은 뉴욕 날짜로
    assert bt.close_check("us", _prices({"NVDA": 121.0}), sent.append, _now("2026-12-02T06:32"), st, persist=False) == 1
    assert "+20%" in sent[2] and "$121.00" in sent[2] and "symbol=NVDA" in sent[2]


def test_expiry_sends_summary_and_stops():
    it = _item(d="2026-05-20")            # until 2026-11-19
    it.update(hit=[5], max={"pct": 8.0, "date": "2026-06-01"}, min={"pct": -3.0, "date": "2026-07-01"})
    st = {"items": [it]}
    sent = []
    assert bt.close_check("kr", _prices({"008700": 1500.0}), sent.append, _now("2026-11-19T20:22"), st, persist=False) == 1
    assert st["items"] == [] and "매수 추적 끝" in sent[0] and "+50.0%" in sent[0] and "최고 +50.0%(11-19)" in sent[0]
    assert "단계 도달" not in sent[0]


def test_remove_and_state_file(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    bt.register_text("매수추적 아남전자 008700 1000 2026-11-18\n매수추적 엔비디아 NVDA 100", _prices({"008700": 1000.0, "NVDA": 100.0}), _now("2026-11-21T21:00"))
    assert [x["ticker"] for x in bt.load_state()["items"]] == ["008700", "NVDA"]
    assert "아남전자" in bt.remove(" 008700.ks ") and [x["ticker"] for x in bt.load_state()["items"]] == ["NVDA"]
    assert "없어" in bt.remove("005930") and "엔비디아" in bt.remove("엔비디아") and bt.load_state()["items"] == []
    assert "없어" in bt.remove("") and "추적 중인 매수가 없어" in bt.status_text(_prices({}), _now("2026-11-21T21:00"))
