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


def test_levels_long_mirror():
    # 숏 배열을 200 기준으로 뒤집은 것 → 같은 구조의 롱
    bars = [B(200 - b["l"], 200 - b["h"]) for b in _short_bars()]
    lv_ = lv.compute_levels(bars, "long", 100.0, k=3, pad_pct=0.05)
    assert lv_["target1"] == pytest.approx(102.0 * 0.9995)   # 고점 −여유
    assert lv_["target2"] == pytest.approx(105.0 * 0.9995)


def test_liquidation_price():
    assert lv.liquidation_price("long", 100.0) == pytest.approx(91.0)
    assert lv.liquidation_price("short", 100.0) == pytest.approx(109.0)


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


# ── v2.39 봉 조회 호스트 폴백 (Railway 미국 리전 451) ──

import requests  # noqa: E402

ROW = [1, "1", "2", "0.5", "1.5", "0", 59_999, "0", 0, "0", "0", "0"]


def _resp(code, body=None):
    r = requests.Response()
    r.status_code = code
    r._content = (b"[]" if body is None else body)
    r.url = "http://x"
    return r


def test_fetch_451_falls_back_without_retry(monkeypatch):
    calls = []

    def get(url, params=None, timeout=None):
        calls.append(url)
        return _resp(451) if "binance.vision" in url else _resp(200, ("[" + str(ROW).replace("'", '"') + "]").encode())

    monkeypatch.setattr(lv.requests, "get", get)
    bars = lv.fetch_klines("BTCUSDT", "1m", limit=1500)
    assert calls == ["https://data-api.binance.vision/api/v3/klines", "https://api.binance.com/api/v3/klines"]
    assert bars[0]["c"] == 1.5


def test_fetch_all_hosts_fail_reason(monkeypatch):
    calls = []
    monkeypatch.setattr(lv.requests, "get", lambda url, **kw: calls.append(url) or _resp(451))
    with pytest.raises(lv.KlinesError) as ei:
        lv.fetch_klines("BTCUSDT", "5m")
    assert len(calls) == 2   # 호스트당 1회, 재시도 없음
    assert str(ei.value) == "HTTP 451 (data-api.binance.vision) → HTTP 451 (api.binance.com)"


def test_fetch_network_error_falls_back(monkeypatch):
    def get(url, params=None, timeout=None):
        if "binance.vision" in url:
            raise requests.ConnectionError("boom")
        assert params["limit"] == 1000   # 현물 최대 1000으로 제한
        return _resp(200)

    monkeypatch.setattr(lv.requests, "get", get)
    assert lv.fetch_klines("BTCUSDT", "1m", limit=1500) == []


def test_build_context_records_reasons():
    def fetch(*a, **kw):
        raise lv.KlinesError("HTTP 451 (data-api.binance.vision) → HTTP 451 (api.binance.com)")

    ctx = lv.build_signal_context("BTCUSDT", "1m", "long", 100.0, 59_999, fetch=fetch)
    assert ctx["errors"] == {k: "HTTP 451 (data-api.binance.vision) → HTTP 451 (api.binance.com)"
                             for k in ("levels", "cloud", "htf_rsi", "pullback")}
    assert lv.price_basis() == "현물"


# ── v2.40 최소 거리·구간 극값 (v2.41: 목표만) ──

# 스윙 저점 99.9(idx3)·99.0(idx7)·98.0(idx11), 스윙 고점 100.6(idx9).
# 양 끝(0~2, 13~15)은 스윙 후보가 아님 — 최고가 100.7(idx14), 최저가 97.0(idx15)
_L = [100.2, 100.1, 100.0, 99.9, 100.0, 100.1, 100.2, 99.0, 100.2, 100.3, 100.2, 98.0, 100.2, 100.3, 100.4, 97.0]
MIN_BARS = [B(l + 0.3, l) for l in _L]


def test_swing_closer_than_min_dist_skipped():
    lv_ = lv.compute_levels(MIN_BARS, "short", 100.0, k=3, pad_pct=0.05, min_dist_pct=0.15)
    # 99.9 스윙 → 99.95(0.05%) 건너뜀 → 다음 스윙들
    assert lv_["target1"] == pytest.approx(99.0 * 1.0005)
    assert lv_["target2"] == pytest.approx(98.0 * 1.0005)
    assert lv_["src"] == {"target1": "swing", "target2": "swing"}
    # 최소 거리 0이면(옛 동작) 99.9 스윙이 목표1
    assert lv.compute_levels(MIN_BARS, "short", 100.0, k=3, pad_pct=0.05,
                             min_dist_pct=0)["target1"] == pytest.approx(99.9 * 1.0005)


def test_extreme_also_too_close_is_none():
    # 진입 97.2 숏: 진입가 아래 스윙 없음 → 최저가 97.0 +0.1% = 97.097(0.106%) < 0.15% → 없음
    lv_ = lv.compute_levels(MIN_BARS, "short", 97.2, k=3, pad_pct=0.05,
                            min_dist_pct=0.15, extreme_pad_pct=0.1)
    assert lv_["target1"] is None and lv_["target2"] is None


def test_target2_falls_back_to_window_extreme():
    # 진입 98.8: 목표 스윙은 98.0 하나 → 목표2 = 최저가 97.0 +0.1%
    lv_ = lv.compute_levels(MIN_BARS, "short", 98.8, k=3, pad_pct=0.05,
                            min_dist_pct=0.15, extreme_pad_pct=0.1)
    assert lv_["target1"] == pytest.approx(98.0 * 1.0005)
    assert lv_["target2"] == pytest.approx(97.0 * 1.001)
    assert lv_["src"]["target2"] == "extreme"


def test_target2_min_gap_next_swing_or_gap_fix():
    # 스윙 저점 99.0·98.95(목표1 대비 0.05% — 너무 가까움)·98.0
    l = [100.2, 100.1, 100.0, 99.0, 100.0, 100.1, 100.2, 98.95, 100.2, 100.3, 100.2, 98.0, 100.2, 100.3, 100.4, 100.5]
    bars = [B(x + 0.3, x) for x in l]
    lv_ = lv.compute_levels(bars, "short", 100.0, k=3, pad_pct=0, min_dist_pct=0.15, gap_pct=0.1)
    assert (lv_["target1"], lv_["target2"]) == (99.0, 98.0)          # 98.95 건너뛰고 다음 스윙
    assert lv_["src"]["target2"] == "swing"
    # 98.0 스윙이 없고 남은 후보(98.95, 극값 98.95)가 전부 가까우면 → 목표1 −0.1% (간격 보정)
    l2 = l[:11] + [100.1] + l[12:]
    lv_ = lv.compute_levels([B(x + 0.3, x) for x in l2], "short", 100.0, k=3, pad_pct=0,
                            min_dist_pct=0.15, extreme_pad_pct=0, gap_pct=0.1)
    assert lv_["target1"] == 99.0 and lv_["target2"] == pytest.approx(99.0 * 0.999)
    assert lv_["src"]["target2"] == "gap_fix"
    # 롱 대칭
    lv_ = lv.compute_levels([B(200 - x, 199.7 - x) for x in l2], "long", 100.0, k=3, pad_pct=0,
                            min_dist_pct=0.15, extreme_pad_pct=0, gap_pct=0.1)
    assert lv_["target1"] == pytest.approx(101.0) and lv_["target2"] == pytest.approx(101.0 * 1.001)
    assert lv_["src"]["target2"] == "gap_fix"


def test_gap_fix_shown_in_message():
    sig = {"id": 1, "symbol": "BTCUSDT", "tf": "5m", "side": "short", "entry": 100.0,
           "bar_close_ms": 1792051499999, "target1": 99.0, "target2": 98.901,
           "level_src": {"target1": "swing", "target2": "gap_fix"}, "cloud": None, "htf_rsi": None}
    import signal_tracker as tr
    assert "목표2 98.901 (−1.10%) (간격 보정)" in tr.format_signal_message(sig).split("\n")


def test_long_min_dist_mirror():
    bars = [B(200 - b["l"], 200 - b["h"]) for b in MIN_BARS]
    lv_ = lv.compute_levels(bars, "long", 100.0, k=3, pad_pct=0.05, min_dist_pct=0.15)
    assert lv_["target1"] == pytest.approx(101.0 * 0.9995)   # 100.1 스윙(0.05%) 건너뜀
