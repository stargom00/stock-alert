"""v2.37 — signal_levels: RSI·스윙·목표/손절·이치모쿠 순수 계산 테스트."""
import pytest

import signal_levels as lv


def B(h, l, o=None, c=None, t=0):
    o = (h + l) / 2 if o is None else o
    c = (h + l) / 2 if c is None else c
    return {"t": t, "T": t + 299_999, "o": o, "h": h, "l": l, "c": c}


# ── RSI (Wilder = 트레이딩뷰 ta.rsi) ──

def test_rsi_wilder_hand_computed():
    # length 2, 변화 +1,-1,+1
    # [2]: avg_g=(1+0)/2=0.5, avg_l=(0+1)/2=0.5 → 50
    # [3]: avg_g=(0.5*1+1)/2=0.75, avg_l=(0.5*1+0)/2=0.25 → RS 3 → 75
    r = lv.rsi_wilder([1, 2, 1, 2], length=2)
    assert r[:2] == [None, None]
    assert r[2] == pytest.approx(50.0)
    assert r[3] == pytest.approx(75.0)


def test_rsi_all_up_is_100_and_short_input_none():
    assert lv.rsi_wilder(list(range(20)), 14)[-1] == 100.0
    assert lv.rsi_wilder([1, 2, 3], 14) == [None, None, None]


def test_signal_at_rules():
    bars = [B(11, 9, o=10, c=10), B(11, 9, o=9.5, c=10.5)]   # 두 번째 = 양봉
    assert lv.signal_at(bars, 1, [29.9, None]) == "long"
    assert lv.signal_at(bars, 1, [30.0, None]) is None          # 30 "밑"이어야
    assert lv.signal_at(bars, 1, [70.1, None]) is None          # 양봉엔 숏 없음
    red = [bars[0], B(11, 9, o=10.5, c=9.5)]
    assert lv.signal_at(red, 1, [70.1, None]) == "short"
    doji = [bars[0], B(11, 9, o=10, c=10)]
    assert lv.signal_at(doji, 1, [10.0, None]) is None
    assert lv.signal_at(doji, 1, [90.0, None]) is None


# ── 스윙 ──

def test_find_swings_excludes_edges():
    # 0번(가장 낮은 저가 1)과 마지막 번(가장 높은 고가 50)은 이웃이 모자라 제외
    lows = [1, 7, 6, 5, 4, 5, 6, 8, 6, 5, 6, 7, 6]
    highs = [l + 2 for l in lows]
    highs[-1] = 50
    bars = [B(h, l) for h, l in zip(highs, lows)]
    sl, sh = lv.find_swings(bars, k=3)
    assert sl == [(4, 4), (9, 5)]
    assert sh == [(7, 10)]


def test_find_swings_requires_strict():
    lows = [9, 8, 7, 5, 5, 7, 8, 9, 9]   # 동률 저점 → 둘 다 스윙 아님
    bars = [B(l + 1, l) for l in lows]
    assert lv.find_swings(bars, k=2)[0] == []


# ── 목표/손절 ──

def _short_bars():
    # 스윙 저점 98(idx3), 95(idx11); 스윙 고점 103(idx7). 마지막 봉 종가 100.
    lows = [99.5, 99.4, 99.2, 98.0, 99.2, 99.4, 99.6, 101.5, 99.6, 99.4, 99.0, 95.0, 99.0, 99.4, 99.6, 99.8]
    highs = [100.5, 100.6, 100.8, 100.5, 100.8, 101.0, 101.2, 103.0, 101.2, 101.0, 100.8, 100.0, 100.8, 101.0, 100.8, 100.5]
    return [B(h, l) for h, l in zip(highs, lows)]


def test_levels_short():
    lv_ = lv.compute_levels(_short_bars(), "short", 100.0, k=3, pad_pct=0.05)
    assert lv_["target1"] == pytest.approx(98.0 * 1.0005)    # 저점 +여유
    assert lv_["target2"] == pytest.approx(95.0 * 1.0005)
    assert lv_["stop"] == pytest.approx(103.0 * 1.0005)      # 고점 +여유(바깥)
    assert lv_["rr"] == pytest.approx((100 - 98.049) / (103.0515 - 100))


def test_levels_long_mirror():
    # 숏 배열을 200 기준으로 뒤집은 것 → 같은 구조의 롱
    bars = [B(200 - b["l"], 200 - b["h"]) for b in _short_bars()]
    lv_ = lv.compute_levels(bars, "long", 100.0, k=3, pad_pct=0.05)
    assert lv_["target1"] == pytest.approx(102.0 * 0.9995)   # 고점 −여유
    assert lv_["target2"] == pytest.approx(105.0 * 0.9995)
    assert lv_["stop"] == pytest.approx(97.0 * 0.9995)       # 저점 −여유(바깥)
    assert lv_["rr"] == pytest.approx((101.949 - 100) / (100 - 96.9515))


def test_levels_missing_side_is_none():
    lv_ = lv.compute_levels(_short_bars(), "short", 104.0, k=3, pad_pct=0.05)
    assert lv_["stop"] is None and lv_["rr"] is None        # 진입가 위 스윙 고점 없음
    assert lv_["target1"] == pytest.approx(98.0 * 1.0005)


# ── 이치모쿠 (작은 파라미터로 수작업 대조) ──

ICHI = [B(10, 8), B(12, 9), B(11, 7), B(14, 10), B(13, 11), B(15, 12), B(16, 13)]


def test_ichimoku_hand_computed():
    # displacement 3 → 마지막(6) 봉의 구름은 6-(3-1)=4번 봉에서 계산
    # 전환(2): 3~4번 (14+10)/2=12, 기준(3): 2~4번 (14+7)/2=10.5 → A=11.25
    # 스팬B(4): 1~4번 (14+7)/2=10.5
    a, b = lv.ichimoku_cloud(ICHI, tenkan=2, kijun=3, senkou_b=4, displacement=3)
    assert a == pytest.approx(11.25)
    assert b == pytest.approx(10.5)


def test_ichimoku_insufficient_data():
    assert lv.ichimoku_cloud(ICHI[:5], tenkan=2, kijun=3, senkou_b=4, displacement=3) is None


def test_cloud_context():
    c = (11.25, 10.5)
    up = lv.cloud_context(12.0, c, "long")
    assert (up["pos"], up["bias"]) == ("above", "편")
    assert up["thick_pct"] == pytest.approx(0.75 / 12 * 100)
    assert lv.cloud_context(12.0, c, "short")["bias"] == "역"
    assert lv.cloud_context(11.0, c, "short")["bias"] == "중립"
    dn = lv.cloud_context(10.0, c, "short")
    assert (dn["pos"], dn["bias"]) == ("below", "편")


def test_htf_rsi_direction():
    closes = [10 + (i % 3) for i in range(40)] + [5]
    h = lv.htf_rsi([B(c + 1, c - 1, c=c) for c in closes])
    assert h["dir"] == "↓" and 0 <= h["value"] <= 100


def test_parse_targets():
    assert lv._parse_targets("btcusdt:5m, ETHUSDT:1m,bad,") == [("BTCUSDT", "5m"), ("ETHUSDT", "1m")]
