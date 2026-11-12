"""v2.58 120·240일선 상향 돌파 — 실데이터(NVDA 2025-06-02~2026-11-10 일봉) + 경계 조합."""
import os
from datetime import date, datetime, timedelta

import pytest

import ma_cross as m

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "nvda_daily_250602_261110.txt")


def nvda():
    rows = []
    for x in open(FIX).read().strip().split(";"):
        f = x.split(",")
        rows.append((date(2000 + int(f[0][:2]), int(f[0][2:4]), int(f[0][4:6])), int(f[4]) / 100))
    return rows


def test_real_close_crosses_exact_set():
    rows = nvda()
    c = [v for _, v in rows]
    hits = []
    for i in range(242, len(c) + 1):
        for w in m.WINDOWS:
            if m.close_cross_up(c[:i], w)[0]:
                hits.append((str(rows[i - 1][0]), w))
    # 사보타주 확인: close_cross_up의 < 를 <= 로, 혹은 >= 를 > 로 바꾸면 이 집합이 바뀐다(개수가 아니라 집합으로 고정)
    assert hits == [("2026-06-29", 120), ("2026-07-06", 120), ("2026-07-30", 240),
                    ("2026-07-31", 120), ("2026-10-19", 120), ("2026-10-26", 120)]


def test_cross_needs_prev_below_and_today_above():
    c = [10.0] * 120 + [9.0, 11.0]
    up, ma_t, ma_y = m.close_cross_up(c, 120)
    assert up and c[-2] < ma_y and c[-1] >= ma_t
    assert not m.close_cross_up(c[:-1] + [9.5], 120)[0]          # 오늘도 아래
    assert not m.close_cross_up([10.0] * 120 + [11.0, 12.0], 120)[0]   # 어제부터 위 = 이미 돌파한 상태
    assert m.close_cross_up([10.0] * 120, 120) == (False, None, None)  # 봉 w+1개 미만


def test_intraday_estimate_uses_today_price_in_average():
    conf = [10.0] * 119 + [9.0]
    hit, est = m.intraday_cross_up(conf, 10.5, 120)
    assert hit and est == pytest.approx((sum(conf[-119:]) + 10.5) / 120)
    assert not m.intraday_cross_up(conf, 9.9, 120)[0]
    assert not m.intraday_cross_up([10.0] * 119 + [11.0], 12.0, 120)[0]   # 어제 이미 위


def _bars_until(rows, upto):
    return [r for r in rows if r[0] <= upto]


def test_close_check_real_day_sends_and_dedupes():
    rows = nvda()
    on = date(2026, 7, 30)                       # 240일선 종가 돌파일
    now = datetime(2026, 7, 31, 6, 0, tzinfo=m.KST)   # 뉴욕 7/30 17:00
    sent, st = [], {}
    out = m.close_check("us", sent.append, now_kst=now, tickers=["NVDA"], state=st,
                        fetch=lambda t: _bars_until(rows, on), persist=False)
    assert [(t, w, k) for t, w, k, _, _ in out] == [("NVDA", 240, "up")]
    assert len(sent) == 1 and "240일선" in sent[0] and "상향 돌파 확정" in sent[0]
    m.close_check("us", sent.append, now_kst=now, tickers=["NVDA"], state=st,
                  fetch=lambda t: _bars_until(rows, on), persist=False)
    assert len(sent) == 1                        # 같은 날 두 번 안 보냄


def test_close_check_silent_without_event_and_skips_missing_today_bar():
    rows = nvda()
    now = datetime(2026, 11, 11, 6, 30, tzinfo=m.KST)   # 뉴욕 11/10 마감 뒤
    sent = []
    assert m.close_check("us", sent.append, now_kst=now, tickers=["NVDA"], state={},
                         fetch=lambda t: rows, persist=False) == []
    assert sent == []
    out = m.close_check("us", sent.append, now_kst=now, tickers=["NVDA"], state={},
                        fetch=lambda t: rows[:-1], persist=False)   # 오늘 봉 없음(지연·휴장)
    assert out == [] and sent == []


def test_intraday_then_close_fail_reported():
    conf_dates = [date(2026, 1, 1) + timedelta(days=i) for i in range(240)]
    closes = [10.0] * 239 + [9.0]
    on = date(2026, 11, 10)                      # 화요일
    now = datetime(2026, 11, 11, 3, 0, tzinfo=m.KST)   # 뉴욕 11/10 13:00 장중
    m._confirmed_cache.clear()
    bars = list(zip(conf_dates, closes))
    st, sent = {}, []
    n = m.check_intraday(lambda t: {"price": 10.6}, sent.append, now_kst=now, tickers=["ZZZ"],
                         state=st, fetch=lambda t: bars + [(on, 10.6)], persist=False)
    assert n == 2 and st["ZZZ:120:intra"] == str(on) and st["ZZZ:240:intra"] == str(on)
    assert m.check_intraday(lambda t: {"price": 10.6}, sent.append, now_kst=now, tickers=["ZZZ"],
                            state=st, fetch=lambda t: bars, persist=False) == 0   # 하루 1회
    close_now = datetime(2026, 11, 11, 7, 0, tzinfo=m.KST)
    out = m.close_check("us", sent.append, now_kst=close_now, tickers=["ZZZ"], state=st,
                        fetch=lambda t: bars + [(on, 9.5)], persist=False)
    assert sorted((w, k) for _, w, k, _, _ in out) == [(120, "fail"), (240, "fail")]
    assert "종가 복귀" in sent[-1]


def test_intraday_skips_closed_market():
    sent = []
    sat = datetime(2026, 11, 15, 3, 0, tzinfo=m.KST)   # 일 03:00 KST = 뉴욕 토 13:00
    assert m.check_intraday(lambda t: {"price": 1}, sent.append, now_kst=sat, tickers=["NVDA", "034020.KS"],
                            state={}, fetch=lambda t: 1 / 0, persist=False) == 0


def test_parsers():
    txt = "[['날짜','시가','고가','저가','종가','거래량','외국인소진율'],\n[\"20261110\", 1,2,1,72000,5,1.0],\n[\"20261109\",0,0,0,71000,0,1.0],\n[\"20261108\",0,0,0,0,0,1.0],\n]"
    assert m.parse_sise(txt) == [(date(2026, 11, 9), 71000.0), (date(2026, 11, 10), 72000.0)]
    j = {"chart": {"result": [{"timestamp": [1762785000, 1762871400],
                               "indicators": {"quote": [{"close": [170.0, None]}]}}]}}
    assert m.parse_yahoo(j) == [(date(2025, 11, 10), 170.0)]


def test_list_edit(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    base = m.load_list()
    assert "NVDA" in base and "034020.KS" in base
    assert "TSLA" in m.edit_list("tsla", add=True)
    assert "NVDA" not in m.edit_list("NVDA", add=False)
    assert m.load_list() == [t for t in base if t != "NVDA"] + ["TSLA"]


def test_registered_in_main():
    src = open(os.path.join(os.path.dirname(__file__), "main.py"), encoding="utf-8").read()
    assert src.count("ma_cross.register(schedule, get_stock_data, send_telegram)") == 1
    assert src.count("ma_cross.status_text(get_stock_data)") == 1


def test_equality_boundaries():
    # 오늘 종가 = 오늘 이평 → 돌파로 친다(>=). 어제 종가 = 어제 이평 → "아래"가 아니므로 돌파 아님(<).
    c = [10.0] * 119 + [7.0, 10.0]           # 오늘 이평 = (118×10 + 7 + 10)/120 → 오늘 종가 10 ≥ 이평
    c_eq_today = [8.0] * 120 + [8.0 - 1.2, 0.0]
    ma_y = m.sma(c_eq_today, 120, end=-1)
    # 오늘 종가를 오늘 이평과 정확히 같게: x = (sum(마지막 119개) + x)/120 → x = sum/119
    x = sum(c_eq_today[-120:-1]) / 119
    c_eq_today[-1] = x
    up, ma_t, _ = m.close_cross_up(c_eq_today, 120)
    assert c_eq_today[-2] < ma_y and ma_t == pytest.approx(x) and up
    c_eq_prev = [10.0] * 121 + [11.0]        # 어제 종가 10 = 어제 이평 10
    assert not m.close_cross_up(c_eq_prev, 120)[0]
    assert m.close_cross_up(c, 120)[0]
