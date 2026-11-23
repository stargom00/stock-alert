"""v2.96 ATR 조회. 실데이터 픽스처 test_fixtures/sise_053030_20261120.txt(바이넥스 naver siseJson 2026-05-01~11-20).
사보타주 확인(2026-11-23, 전부 FAIL 확인 후 원복): Wilder를 단순평균으로 / TR에서 전일 종가 갭 빼기 / 확정 전 오늘 봉 포함 /
1 ATR 경고 문턱 뒤집기 / main 라우팅 제거."""
from __future__ import annotations

import os
from datetime import date, datetime, timezone

import atr_check as a

HERE = os.path.dirname(os.path.abspath(__file__))
ROWS = a.parse_sise_ohlc(open(os.path.join(HERE, "test_fixtures", "sise_053030_20261120.txt"), encoding="utf-8").read())


def test_parse_real():
    assert ROWS[-1][0] == date(2026, 11, 20) and ROWS[-1][3] == 7050.0
    assert all(h >= l > 0 for _, h, l, _ in ROWS)


def test_wilder_by_hand():
    rows = [(date(2026, 1, i + 1), 10 + (i % 3), 8 - (i % 2), 9 + (i % 2)) for i in range(20)]
    tr = [max(h - l, abs(h - rows[i - 1][3]), abs(l - rows[i - 1][3])) for i, (_, h, l, _) in enumerate(rows) if i]
    x = sum(tr[:14]) / 14
    for t in tr[14:]:
        x = (x * 13 + t) / 14
    assert abs(a.atr_wilder(rows) - x) < 1e-9
    assert a.atr_wilder(rows[:14]) is None


def test_gap_counts_in_true_range():
    rows = [(date(2026, 1, i + 1), 101, 99, 100) for i in range(15)] + [(date(2026, 1, 16), 121, 119, 120)]
    assert a.atr_wilder(rows) > a.atr_wilder(rows[:-1]) + 1      # 갭 +20 → TR 21


def test_real_value():
    assert round(a.atr_wilder(ROWS)) == 365


def test_confirmed_drops_open_kr_bar():
    rows = ROWS + [(date(2026, 11, 23), 6980.0, 6680.0, 6920.0)]
    before = datetime(2026, 11, 23, 6, 0, tzinfo=timezone.utc)        # 15:00 KST
    after = datetime(2026, 11, 23, 11, 15, tzinfo=timezone.utc)       # 20:15 KST
    assert a.confirmed(rows, "kr", before)[-1][0] == date(2026, 11, 20)
    assert a.confirmed(rows, "kr", after)[-1][0] == date(2026, 11, 23)


def test_report_flags():
    r = a.report("바이넥스 053030", "053030.KQ", ROWS, 6861, 6600)
    assert "0.7 ATR" in r and "⚠ 1 ATR 안" in r and "6,314" in r and "붙여 넣기: 365" in r
    r2 = a.report("x", "053030.KQ", ROWS, 6861, 6300)                   # 1.5 ATR 근처
    assert "⚠" not in r2 and "1.5 ATR" in r2


def test_parse_and_on_demand():
    assert a.parse("ATR 바이넥스 6,861 6600") == ("바이넥스", 6861.0, 6600.0)
    assert a.parse("ATR 바이넥스") == ("바이넥스", None, None)
    assert a.parse("ATR 바이넥스 abc") is None and a.parse("AT 바이넥스") is None
    out = a.on_demand("atr 바이넥스 6861 6600", lambda s: "053030.KQ", fetch=lambda t: ROWS,
                      now=datetime(2026, 11, 23, 6, 0, tzinfo=timezone.utc))
    assert "바이넥스 053030" in out and "0.7 ATR" in out
    assert "일봉을 못 받았어요" in a.on_demand("ATR 없음", lambda s: s, fetch=lambda t: None)


def test_routed_in_main():
    src = open(os.path.join(HERE, "main.py"), encoding="utf-8").read()
    assert src.count('if (text.split() or [""])[0].upper() == "ATR":') == 1 and "atr_check.on_demand(text" in src   # ATRC 같은 티커 조회는 안 가로챔
